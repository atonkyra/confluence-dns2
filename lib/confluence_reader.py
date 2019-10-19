import requests
from bs4 import BeautifulSoup
import logging
import ipaddress
import hashlib


class Confluence:
    def __init__(self, base_url, username, password, verify_ssl=True):
        self.username = username
        self.password = password
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.r = requests.session()
        self.r.auth = (self.username, self.password)
        self.r.headers.update({'User-Agent': 'Confluence-DNS updater'})

    def get_page(self, page_id):
        url = "%s/rest/api/content/%s?expand=body.storage" % (self.base_url, page_id)
        response = self.r.get(url, verify=self.verify_ssl)
        if response.status_code != 200:
            return None
        else:
            return response.json()['body']['storage']['value']


class ConfluenceReader:
    def __init__(self, config):
        self._config = config
        self._confluence_client = Confluence(
            self._config['url'],
            self._config['username'],
            self._config['password']
        )
        self._logger = logging.getLogger('confluence-reader')

    def _find_first_confluence_dns_tag(self, parsed_html):
        tables = parsed_html.find_all('table')
        for table in tables:
            try:
                first_td = table.find('td')
                if first_td is None:
                    continue
                hint = None
                try:
                    hint = first_td.next_element.getText()
                except AttributeError:
                    continue
                if hint == '#ConfluenceDNSTable':
                    return table
            except BaseException as be:
                self._logger.exception(be)

        return None

    def _create_named_entry(self, entry_name, entry_ipv4, entry_ipv6, current_zone, dns_zone, dns_subzone):
        if entry_name in current_zone:
            if 'CNAME' in current_zone[entry_name]:
                self._logger.error(
                    'CNAME exists for entry (%s.%s.%s), clearing CNAME away!',
                    entry_name,
                    dns_subzone,
                    dns_zone
                )
                current_zone[entry_name] = {
                    'A': entry_ipv4,
                    'AAAA': entry_ipv6,
                }
            else:
                current_zone[entry_name]['A'] += entry_ipv4
                current_zone[entry_name]['AAAA'] += entry_ipv6
        else:
            current_zone[entry_name] = {
                'A': entry_ipv4,
                'AAAA': entry_ipv6,
            }

    def _create_alias_entries(self, entry_name, entry_aliases, current_zone):
        for alias in entry_aliases:
            if alias in current_zone:
                self._logger.error('%s already in current zone, not adding a CNAME', alias)
                continue
            current_zone[alias] = {
                'CNAME': ['%s' % entry_name]
            }

    def _create_srv_entries(self, entry_name, entry_srv_records, current_zone, dns_zone):
        for entry_srv_record in entry_srv_records:
            srv_name, port = entry_srv_record.split(':', 1)
            srv_fqdn = '%s.%s' % (srv_name, dns_zone)
            if srv_fqdn not in current_zone:
                current_zone[srv_fqdn] = {
                    'SRV': []
                }
            current_zone[srv_fqdn]['SRV'].append('0 0 %s %s' % (port, entry_name))

    def _create_ptr(self, entry_name, entry_ipv4, entry_ipv6, current_reverse4, current_reverse6):
        if len(entry_ipv4) > 0 and current_reverse4 is not None:
            ipv4_ptr = '%s.' % ipaddress.ip_address(entry_ipv4[0]).reverse_pointer
            current_reverse4[ipv4_ptr] = {'PTR': [entry_name]}
        if len(entry_ipv6) > 0 and current_reverse6 is not None:
            ipv6_ptr = '%s.' % ipaddress.ip_address(entry_ipv6[0]).reverse_pointer
            current_reverse6[ipv6_ptr] = {'PTR': [entry_name]}

    def _build_dict(self, address_table):
        zone_dict = {}

        columns = []

        dns_subzone = None
        dns_zone = None
        dns_reverse4 = None
        dns_reverse6 = None

        current_zone = None
        current_reverse4 = None
        current_reverse6 = None

        for row in address_table.find_all('tr'):
            rowcells = []
            if row.find('th'):
                for cell in row.find_all('th'):
                    columns.append(cell.getText())
                continue
            for cell in row.find_all('td'):
                rowcell_plaintext = cell.getText().replace('\xa0', ' ').strip()
                if rowcell_plaintext.startswith('#'):
                    if rowcell_plaintext == '#ConfluenceDNSTable':
                        break
                    command_tag, arguments = rowcell_plaintext.strip('#').split(' ', 1)
                    if command_tag == 'ConfluenceDNSSubzone':
                        try:
                            dns_subzone, dns_zone, dns_reverse4, dns_reverse6 = arguments.split(' ')
                            if dns_zone not in zone_dict:
                                zone_dict[dns_zone] = {}
                            current_zone = zone_dict[dns_zone]
                            if dns_reverse4 == '-':
                                current_reverse4 = None
                            else:
                                if dns_reverse4 not in zone_dict:
                                    zone_dict[dns_reverse4] = {}
                                current_reverse4 = zone_dict[dns_reverse4]
                            if dns_reverse6 == '-':
                                current_reverse6 = None
                            else:
                                if dns_reverse6 not in zone_dict:
                                    zone_dict[dns_reverse6] = {}
                                current_reverse6 = zone_dict[dns_reverse6]
                        except ValueError:
                            self._logger.error('failed to parse %s as 4 arguments', arguments)
                        finally:
                            break
                else:
                    rowcells.append(rowcell_plaintext)
            if len(rowcells) == 0:
                continue
            rowdata = dict(zip(columns, rowcells))
            for key in rowdata:
                if len(rowdata[key]) > 0:
                    keystmp = rowdata[key].split(' ')
                    rowdata[key] = []
                    for keytmp in keystmp:
                        keytmp = keytmp.strip()
                        if len(keytmp) > 0:
                            rowdata[key].append(keytmp)
                else:
                    rowdata[key] = []
            entry_name = None
            if len(rowdata['Name']) == 0:
                continue
            if dns_subzone == '-':
                entry_name = "%s.%s" % (rowdata['Name'][0], dns_zone)
            else:
                entry_name = "%s.%s.%s" % (rowdata['Name'][0], dns_subzone, dns_zone)
            entry_ipv4 = rowdata['A']
            entry_ipv6 = rowdata['AAAA']
            entry_aliases = rowdata['ALIAS']
            entry_srv_records = rowdata['SRV']
            self._create_named_entry(entry_name, entry_ipv4, entry_ipv6, current_zone, dns_zone, dns_subzone)
            self._create_alias_entries(entry_name, entry_aliases, current_zone)
            self._create_srv_entries(entry_name, entry_srv_records, current_zone, dns_zone)
            self._create_ptr(entry_name, entry_ipv4, entry_ipv6, current_reverse4, current_reverse6)
        return zone_dict

    def fetch_dict(self, old_checksum):
        pagedata = self._confluence_client.get_page(self._config['page_id'])
        if not pagedata:
            return None, old_checksum
        new_checksum = hashlib.sha256(pagedata.encode()).hexdigest()
        if old_checksum == new_checksum:
            return None, new_checksum
        parsed_html = BeautifulSoup(pagedata, "lxml")
        address_table = self._find_first_confluence_dns_tag(parsed_html)
        if address_table is None:
            self._logger.error('failed to fetch DNS table from page')
            return None, new_checksum
        return self._build_dict(address_table), new_checksum
