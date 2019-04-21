import sys
from lib.confluence_reader import ConfluenceReader
import configparser
import logging
import time
import updater
import signal


logger = logging.getLogger('confluence-dns')
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(name)-16s %(message)s')
running = True


def sighandler(_signum, _frame):
    global running
    running = False


def main():
    global running
    signal.signal(signal.SIGTERM, sighandler)
    signal.signal(signal.SIGINT, sighandler)
    config = configparser.ConfigParser(strict=False, empty_lines_in_values=False)
    if not config.read('config.ini'):
        logger.fatal('config file could not be read')
        return 1
    confluence_reader = ConfluenceReader(config['confluence'])
    checksum = None
    while running:
        confluence_dns_dict, checksum = confluence_reader.fetch_dict(checksum)
        if confluence_dns_dict is not None:
            plugin = updater.PLUGINS[config['dnsupdate']['plugin']]
            logger.info('begin DNS update...')
            all_ok = plugin.update_zone_from_dict(config['knotcli'], confluence_dns_dict)
            if all_ok:
                logger.info('completed DNS update')
            else:
                logger.warning('completed DNS update with errors')
        for i in range(0, 10):
            if not running:
                break
            time.sleep(1)


if __name__ == '__main__':
    sys.exit(main())
