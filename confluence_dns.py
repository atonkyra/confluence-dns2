import sys
from lib.confluence_reader import ConfluenceReader
import configparser
import logging
import time
import updater


logger = logging.getLogger('confluence-dns')
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(name)-16s %(message)s')


def main():
    config = configparser.ConfigParser(strict=False, empty_lines_in_values=False)
    if not config.read('config.ini'):
        logger.fatal('config file could not be read')
        return 1
    confluence_reader = ConfluenceReader(config['confluence'])
    checksum = None
    while True:
        confluence_dns_dict, checksum = confluence_reader.fetch_dict(checksum)
        if confluence_dns_dict is not None:
            plugin = updater.PLUGINS[config['dnsupdate']['plugin']]
            plugin.update_zone_from_dict(config['knotcli'], confluence_dns_dict)
        time.sleep(10)


if __name__ == '__main__':
    sys.exit(main())
