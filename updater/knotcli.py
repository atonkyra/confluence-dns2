import subprocess


def knot_exec(cmd, debug):
    argv = ['knotc'] + cmd.split(' ')
    if debug:
        print(' '.join(argv))
    else:
        subprocess.call(argv)


def update_zone_from_dict(config, data):
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
    for zone_name, zone_data in data.items():
        if zone_name != 'vectorama.fi.':
            continue
        knot_exec('zone-begin %s' % zone_name, debug)
        for rrname, rrdata in zone_data.items():
            if rrname in protected_entries:
                continue
            knot_exec('zone-unset %s %s' % (zone_name, rrname), debug)
            for rrtype, rrset in rrdata.items():
                for rritem in rrset:
                    knot_exec('zone-set %s %s %s %s %s' % (zone_name, rrname, ttl, rrtype, rritem), debug)
        knot_exec('zone-commit %s' % zone_name, debug)
