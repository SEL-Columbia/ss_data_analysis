# kannel_stats_2.py

"""
Run:
$ python kannel_stats_2.py ~/work/testing_gateway/083112/kannel/logs/ pass|fail daily
$ python kannel_stats_2.py ~/work/testing_gateway/083112/kannel/logs/ pass|fail monthly
$ python kannel_stats_2.py ~/work/testing_gateway/083112/kannel/logs/ pass|fail yearly


Goes through kannel's access.log files, and prepares reports.
"""

import fnmatch, gzip, os, re, sys, time
from collections import defaultdict

""" Representative logs (may *not* be exhaustive)

2012-08-31 14:54:18 Receive SMS [SMSC:malitel] [SVC:] [ACT:] [BINF:1001] [FID:] [from:+22369861021] [to:22364614444] [flags:-1:0:-1:0:-1] [msg:47:(job=alert&mid=ml01&cid=192.168.1.208&alert=ce)] [udh:0:]

2012-08-31 14:53:23 Receive SMS [SMSC:malitel] [SVC:] [ACT:] [BINF:1001] [FID:] [from:+22361348502] [to:22364614444] [flags:-1:0:-1:0:-1] [msg:66:(l76ZE0#E,36I,894,3U3Y;F,96,7N2,1IUL;G,N9,9WD,2XHX;H,90,4RE,1824#)] [udh:0:]

2012-07-10 16:21:14 Receive SMS [SMSC:malitel] [SVC:] [ACT:] [BINF:1001] [FID:] [from:+22363839107] [to:22364614444] [flags:-1:0:-1:0:-1] [msg:21:Wmk7 +v o 2 sh +k 1 0] [udh:0:]

2012-07-10 17:22:10 Sent SMS [SMSC:malitel] [SVC:childcount] [ACT:] [BINF:] [FID:] [from:22364614444] [to:22363839120] [flags:-1:2:-1:-1:-1] [msg:14:006400E8006D00650073002E005D] [udh:0:]

2012-07-10 16:27:14 FAILED Send SMS [SMSC:malitel] [SVC:childcount] [ACT:] [BINF:] [FID:] [from:22364614444] [to:22363839073] [flags:-1:2:-1:-1:-1] [msg:56:0064006500200064006500200063006F006E00740072006100630065007000740069006F006E0020006D006F006400650072006E0065005D] [udh:0:]
"""

logpat_pass = r'(\S+) (\S+) (\S+) (\S+) \[SMSC:(.*?)\] \[SVC:(.*?)\] \[ACT:(.*?)\] '\
    r'\[BINF:(.*?)\] \[FID:(.*?)\] \[from:(.*?)\] \[to:(.*?)\] \[flags:(.*?)\] '\
    r'\[msg:(.*?)\] \[udh:(.*?)\]'
logpat_fail = r'(\S+) (\S+) (\S+) (\S+) (\S+) \[SMSC:(.*?)\] \[SVC:(.*?)\] \[ACT:(.*?)\] '\
    r'\[BINF:(.*?)\] \[FID:(.*?)\] \[from:(.*?)\] \[to:(.*?)\] \[flags:(.*?)\] '\
    r'\[msg:(.*?)\] \[udh:(.*?)\]'

colnames_pass = ('date', 'time', 'receive/sent', 'mode', 'smsc', 'svc', 
                 'act', 'binf', 'fid', 'from', 'to', 'flags', 'msg', 'udh')
colnames_fail = ('date', 'time', 'FAILED', 'receive/sent', 'mode', 'smsc', 
                 'svc', 'act', 'binf', 'fid', 'from', 'to', 'flags', 'msg', 'udh')

ranges = ('daily', 'monthly', 'yearly')
gateway_number = '22364614444'

logpat = None
colnames = None

def field_map(dictseq, name, func):
    for d in dictseq:
        d[name] = func(d[name])
        yield d

def gen_cat(sources):
    for s in sources:
        for item in s:
            yield item

def gen_open(filenames):
    for name in filenames:
        if name.endswith(".gz"):
            yield gzip.open(name)
        else:
            yield open(name)

def gen_find(filepat, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)

def kannel_logs(lines):
    groups = (logpat.match(line) for line in lines)
    tuples = (g.groups() for g in groups if g)
    log = (dict(zip(colnames, t)) for t in tuples)
    log = field_map(log, "msg", lambda s: s.rsplit(':', 1)[1] if s else '')
    log = field_map(log, "from", lambda s: s[1:] if s.startswith('+') else s)
    log = field_map(log, "to", lambda s: s[1:] if s.startswith('+') else s)
    return log

def lines_from_dir(filepat, dirname):
    names = gen_find(filepat, dirname)
    files = gen_open(names)
    lines = gen_cat(files)
    return lines

def main():
    logdir = sys.argv[1]

    passfail = sys.argv[2].lower()
    if passfail not in ('pass', 'fail'):
        print globals()['__doc__']
        sys.exit(1)
    else:
        if passfail == 'pass':
            globals()['logpat'] = re.compile(logpat_pass)
            globals()['colnames'] = colnames_pass
        else:
            globals()['logpat'] = re.compile(logpat_fail)
            globals()['colnames'] = colnames_fail

    r = sys.argv[3]
    if r not in ranges:
        print globals()['__doc__']
        sys.exit(1)

    if r == 'daily':
        k = (lambda s: s['date'])
    elif r == 'monthly':
        k = (lambda s: s['date'].rsplit('-', 1)[0])
    elif r == 'yearly':
        k = (lambda s: s['date'].split('-', 1)[0])

    # { daterange: [[childcount], [sharedsolar]], }
    ccss = {}
    logs_uncertain = []
    known_numbers = defaultdict(set)
    subscriber_volumes = {}

    lines = lines_from_dir("access.log*", logdir)
    logs = kannel_logs(lines)

    # pass 1: check if the logs are marked by their services.
    # if they are not, mark the phone numbers to be run through pass 2.
    for log in logs:
        try:
            dt = k(log)
            if not ccss.get(dt): ccss[dt] = [0,0]
            svc = log['svc']
            fro, to = log['from'], log['to']
            if log['receive/sent'] == 'Receive':
                if not subscriber_volumes.get(dt): 
                    subscriber_volumes[dt] = defaultdict(int)
                subscriber_volumes[dt][fro] += 1
            if svc == 'childcount':
                ccss[dt][0] += 1
                known_numbers['childcount'].add(fro)
                known_numbers['childcount'].add(to)
            elif svc == 'sharedsolar':
                ccss[dt][1] += 1
                known_numbers['sharedsolar'].add(fro)
                known_numbers['sharedsolar'].add(to)
            elif log['msg'].startswith('(') and log['msg'].endswith(')'):
                ccss[dt][1] += 1
                known_numbers['sharedsolar'].add(fro)
                known_numbers['sharedsolar'].add(to)
            elif log['receive/sent'] == 'Receive': # received messages
                if '+' in log['msg'] and ' ' in log['msg']:
                    # childcount
                    ccss[dt][0] += 1
                    known_numbers['childcount'].add(fro)
                    known_numbers['childcount'].add(to)
                elif len(log['msg'].split('.')) == 3:
                    # sharedsolar action code
                    ccss[dt][1] += 1
                    known_numbers['sharedsolar'].add(fro)
                    known_numbers['sharedsolar'].add(to)
                else:
                    logs_uncertain.append(log)
            elif log['receive/sent'] in ('Sent', 'Send'): # sent messages
                if log['msg'] == ("Votre message n'a pas ete compris. "
                                  "Verifiez le format."):
                    # childcount
                    ccss[dt][0] += 1
                    known_numbers['childcount'].add(fro)
                    known_numbers['childcount'].add(to)
                else:
                    logs_uncertain.append(log)
            else:
                logs_uncertain.append(log)
        except:
            print log
            logs_uncertain.append(log)
            raise

    try:
        known_numbers['childcount'].remove(gateway_number)
        known_numbers['sharedsolar'].remove(gateway_number)
    except: pass

    # pass 2: check if the unknown numbers have been seen before.
    # if they have been, add them to the associated service.
    logs_unknown = []
    for log in logs_uncertain:
        fro, to = log['from'], log['to']
        if fro in known_numbers['childcount'] or \
                to in known_numbers['childcount']:
            ccss[dt][0] += 1
        elif fro in known_numbers['sharedsolar'] or \
                to in known_numbers['sharedsolar']:
            ccss[dt][1] += 1
        else:
            logs_unknown.append(log)

    if passfail == 'pass':
        print "The following summary is for SUCCESSFUL transmissions."
    else:
        print "The following summary is for FAILED transmissions."

    print "DATERANGE,CHILDCOUNT,SHAREDSOLAR,TOTAL"
    for k in sorted(ccss.keys()):
        print "%s,%d,%d,%d" % (k, ccss[k][0], ccss[k][1], sum(ccss[k]))

    print
    print "# unknown logs:", len(logs_unknown)
    print ','.join(colnames)
    for ul in logs_unknown:
        print ','.join([ul[c] for c in colnames])

    print
    print "Message volumes by subscriber phone number"

if __name__ == '__main__':
    main()
