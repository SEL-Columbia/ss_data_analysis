# meter_stats_2.py

"""
python meter_stats.py <site/folder> <parameter> <time_start> <time_end>
eg: python meter_stats.py ml03 00 Watts 201111250400 201211250500
"""

import fnmatch, gzip, os, pylab, re, sys, time
from collections import defaultdict
from datetime import datetime
import psycopg2


defaultcolnames = ['Time Stamp', 'Watts', 'Volts', 'Amps', 'Watt Hours SC20', 
                   'Watt Hours Today', 'Max Watts', 'Max Volts', 'Max Amps', 
                   'Min Watts', 'Min Volts', 'Min Amps', 'Power Factor', 
                   'Power Cycle', 'Frequency', 'Volt Amps', 'Relay Not Closed', 
                   'Send Rate', 'Machine ID', 'Type', 'Credit']

class Global: pass

def field_map(dictseq, name, func):
    for d in dictseq:
        d[name] = func(d[name])
        yield d

def gen_cat(sources):
    for source in sources:
        header = source.readline()
        for line in source:
            yield line.strip()

def gen_open(filenames):
    for name in filenames:
            yield open(name)

def gen_find(filepat, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            timestamp = datetime.strptime(
                ''.join(path.split('/')[1:]), "%Y%m%d%H")
            if Global.t_start <= timestamp < Global.t_end:
                yield os.path.join(path, name)

def meter_logs(lines):
    lists = (line.lower().split(',') for line in lines)
    log = (dict(zip(defaultcolnames, el)) for el in lists)

    #log = field_map(log, 'Time Stamp', 
    #                lambda s: pylab.date2num(datetime.strptime(s, "%Y%m%d%H%M%S")))

    #log = field_map(log, 'Time Stamp', 
    #                lambda s: datetime.strptime(s, "%Y%m%d%H%M%S")  )


    log = field_map(log, 'Watts', lambda s: float(s))
    return log

def lines_from_dir(filepat, dirname):
    names = gen_find(filepat, dirname)
    files = gen_open(names)
    lines = gen_cat(files)
    return lines

def main():
    Global.logdir = sys.argv[1]
    Global.account = sys.argv[2]
    Global.parameter = sys.argv[3]
    Global.t_start = datetime.strptime(sys.argv[4], "%Y%m%d%H%M")
    Global.t_end = datetime.strptime(sys.argv[5], "%Y%m%d%H%M")
    lines = lines_from_dir("192_168_1_2%s.log" % (Global.account,), Global.logdir)
    logs = meter_logs(lines)

    print "Site:", Global.logdir
    print "Meter:", Global.account
    print "Parameter:", Global.parameter
    print "Time Range:", Global.t_start.isoformat(' '), "to", Global.t_end.isoformat(' ')
    tshash={}


    values = sorted([(el['Time Stamp'], el[Global.parameter]) 
              for el in logs], key=lambda t: t[0])
    timeseriesrows=values
    for v in values:
      (ts,value,)=v  
      tshash[ts]=value


    cur=conn=None
    try:
      conn=psycopg2.connect("dbname='timeseries' user='postgres' host='localhost' password='postgres'")
      cur=conn.cursor()

      for (ts,value) in tshash.items():
         print "inserting (timestamp, watts)"+str(ts)+str(value)+"...."
         try: 
            cur.execute("insert into watts_"+str(Global.logdir)+"_"+str(Global.account)+" values(%s,%s)",(ts,value,))
            conn.commit()
         except Exception,e:
            print e 

    except Exception,e:
       print "user_add DATABASE EXCEPTION!!!!"

       print e
       conn.rollback()
    finally:
       if cur is not None:
          cur.close()
       if conn is not None:
          conn.close()


    print "done."

    print "len(lines):"
    print len(values)


if __name__ == '__main__':
    main()
