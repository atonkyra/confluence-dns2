import time
import subprocess
import logging
import json
import copy


logger = logging.getLogger('knotcli')


def knot_exec(cmd, debug):
    argv = ['knotc'] + cmd.split(' ')
    if debug:
        print(' '.join(argv))
        return 0, ''
    else:
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ret = proc.wait()
        return ret, proc.stdout.read().decode().strip()+proc.stderr.read().decode().strip()


def begin_zone_transaction(zone_name, debug):
    waited = 0
    while True:
        ret, outerr = knot_exec('zone-begin %s' % zone_name, debug)
        if ret != 0:
            if 'too many transactions' in outerr:
                waited += 1
                logger.info('zone %s currently under too many transactions, waiting (%s times waited)', zone_name, waited)
                time.sleep(1)
            else:
                logger.error('failed to open zone transaction: %s', outerr)
                return False
        else:
            logger.info('opened zone %s transaction', zone_name)
            return True


def commit_zone_transaction(zone_name, debug):
    ret, outerr = knot_exec('zone-commit %s' % zone_name, debug)
    if ret != 0:
        logger.error('unable to commit zone %s, will revert: %s', zone_name, outerr)
        ret, outerr = knot_exec('zone-abort %s' % zone_name, debug)
        return False
    logger.info('committed zone %s transaction', zone_name)
    return True


def load_cache(config):
    if 'cache_file' not in config:
        return None
    try:
        with open(config['cache_file'], 'r') as fp:
            return json.loads(fp.read())
    except FileNotFoundError:
        return None


def store_cache(config, data):
    if 'cache_file' not in config:
        return
    with open(config['cache_file'], 'w') as fp:
        fp.write(json.dumps(data))


def invalidate_stale_data(new_zones, previous_zones, protected_entries):
    if previous_zones is None:
        return
    for old_zone_name, old_zone_data in previous_zones.items():
        if old_zone_name not in new_zones:
            continue
        new_zone_data = new_zones[old_zone_name]
        for old_zone_rrname in old_zone_data.keys():
            if old_zone_rrname in protected_entries:
                continue
            if old_zone_rrname not in new_zone_data:
                logger.info('marking stale entry %s for cleanup from zone %s', old_zone_rrname, old_zone_name)
                new_zone_data[old_zone_rrname] = {}


def update_zone_from_dict(config, new_zones):
    previous_zones = load_cache(config)
    new_zones_initial = copy.deepcopy(new_zones)

    protected_entries = []
    ttl = 60
    if 'ttl' in config:
        ttl = int(config['ttl'])
    debug = 0
    if 'debug' in config:
        debug = int(config['debug'])
    if 'protected_entries' in config:
        protected_entries_tmp = config['protected_entries'].split(',')
        for item in protected_entries_tmp:
            protected_entries.append(item.strip())
    invalidate_stale_data(new_zones, previous_zones, protected_entries)
    everything_ok = True
    for zone_name, zone_data in new_zones.items():
        if previous_zones is not None:
            if zone_name in previous_zones:
                if previous_zones[zone_name] == zone_data:
                    continue
        zone_ok = True
        transaction_open = begin_zone_transaction(zone_name, debug)
        if not transaction_open:
            everything_ok = False
            continue
        for rrname, rrdata in zone_data.items():
            if rrname in protected_entries:
                logger.warning('rrname %s in protected entries, skipping entry', rrname)
                continue
            knot_exec('zone-unset %s %s' % (zone_name, rrname), debug)
            for rrtype, rrset in rrdata.items():
                for rritem in rrset:
                    ret, outerr = knot_exec('zone-set %s %s %s %s %s' % (zone_name, rrname, ttl, rrtype, rritem), debug)
                    if ret != 0:
                        logger.error('failed to set zone %s attributes: %s, will not commit this zone', zone_name, outerr)
                        zone_ok = False
                        everything_ok = False
        if not zone_ok:
            _ret, _outerr = knot_exec('zone-abort %s' % zone_name, debug)
        else:
            transaction_ok = commit_zone_transaction(zone_name, debug)
            if not transaction_ok:
                everything_ok = False
    if everything_ok:
        store_cache(config, new_zones_initial)
    return everything_ok
