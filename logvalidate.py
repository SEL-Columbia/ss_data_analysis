# logvalidate.py

"""
Traverse SD-card logs and validate individual rows.
Maintain a list of files without headers, and files with corrupted lines for 
later review.
Also returns the valid lines for database inserts.

run using:
python logvalidate.py <logdir>
"""

import fnmatch, os
from datetime import datetime
from collections import defaultdict

class Globals:
    files_without_headers = []
    corrupted_files = defaultdict(int)

casts = {
    "timestamp": (lambda x: datetime.strptime(x, "%Y%m%d%H%M%S")),
    "watts": float,
    "volts": float,
    "amps": float,
    "watthourssc20": float,
    "watthourstoday": float,
    "maxwatts": float,
    "maxvolts": float,
    "maxamps": float,
    "minwatts": float,
    "minvolts": float,
    "minamps": float,
    "powerfactor": float,
    "powercycle": float,
    "frequency": float,
    "voltamps": float,
    "relaynotclosed": bool,
    "sendrate": float,
    "machineid": int,
    "credit": float,
    }

def field_map(dictseq, name, func):
    for d in dictseq:
        d[name] = func(d[name])
        yield d

def gen_cat(sources):
    for source in sources:
        for line in source:
            yield line.strip()

def gen_open(filenames):
    for name in filenames:
        with open(name, 'r') as f:
            yield f

def gen_find(filepat, top, daterange=None):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)

def gen_dict(sources):
    for source in sources:
        header = source.readline()

        # skip files without headers
        if header.lower().startswith("time"):

            # adjust for header column names ordered differently
            colnames = header.strip().lower().replace(' ', '').split(',')
            circuitid = int(
                source.name.rsplit('/',1)[1].split('.',1)[0].rsplit('_',1)[1][1:])
        else:
            Globals.files_without_headers.append(source.name)
            continue

        for line in source:
            d = dict(zip(colnames, line.strip().split(',')))
            if d.get('credit') is None: # for the MAINS
                d['credit'] = 0.0
            try:
                for name, cast in casts.items():
                    d[name] = cast(d[name])
                yield d
            except:
                Globals.corrupted_files[source.name] += 1

def gen_logdicts(pattern, root):
    names = gen_find(pattern, root)
    files = gen_open(names)
    dicts = gen_dict(files)
    return dicts

if __name__ == '__main__':
    import sys
    assert len(sys.argv) == 2, "python logvalidate.py <logdir>"

    root = sys.argv[1]
    assert os.path.exists(root), "directory does not exist: %s" % (root,)

    print "%s\n%s" % (root, '-' * len(root))
    logs = gen_logdicts("*.log", sys.argv[1])

    rows = 0
    for row in logs:
        rows += 1
    print '# valid rows: {:,}'.format(rows,)
    print

    print "files without headers: %d" % (len(Globals.files_without_headers),)
    for filename in Globals.files_without_headers:
        print "\t%s" % (filename,)
    print

    print "corrupted logs: %d" % (len(Globals.corrupted_files),)
    for filename, num_lines in Globals.corrupted_files.items():
        print "\t%s: %d lines" % (filename, num_lines)
    print '\n\tTotal: {:,} invalid lines'.format(sum(Globals.corrupted_files.values()),)

    print
    print '=' * 80
