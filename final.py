
# final.py
"""  
Insert SD-card logs into databases.
Here we try a single database and table per circuit.
"""
import errno
import os
import sqlite3
from datetime import datetime
from collections import defaultdict
from twisted.enterprise.adbapi import ConnectionPool
from twisted.internet import defer, reactor
from twisted.python import log

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
    "machineid": str,
    "credit": float,
}

   
class DBConnPool(object):

    def __init__(self, outdir, sitename, circuitid, year):
        self.outdir = outdir
        self.sitename = sitename
        self.circuitid = circuitid
        self.year = year

        self.tablename = "%s_%d_%d" % (self.sitename, self.circuitid, self.year)
        self.dbname = os.path.join(self.outdir, "%s.db" % (self.tablename,))

    def __str__(self):
        return self.dbname

    def shutdown(self):
        log.msg("shutting down database pool")
        self._pool.close()

    def save(self, logs):
        return self._pool.runInteraction(self._save, logs)


class SqliteConnPool(DBConnPool):

    def __init__(self, outdir, sitename, circuitid, year):
        super(SqliteConnPool, self).__init__(outdir, sitename, circuitid, year)

        self._pool = ConnectionPool(
            'sqlite3',
            self.dbname,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_COLNAMES,
            isolation_level=None
        )
        log.msg("getting database connection: %s" % (self.dbname,))

    def create_table(self):
        log.msg("creating  table: %s" % (self.tablename,))
        q1 = "DROP TABLE IF EXISTS %s" % (self.tablename,)
        q2 = "PRAGMA synchronous=OFF"
        q3 = "PRAGMA journal_mode=MEMORY"
        q4 = ("CREATE TABLE %s ("
              "id INTEGER PRIMARY KEY AUTOINCREMENT,"
              "circuitid INTEGER,"
              "timestamp TIMESTAMP,"
              "watts FLOAT,"
              "volts FLOAT,"
              "amps FLOAT,"
              "watthourssc20 FLOAT,"
              "watthourstoday FLOAT,"
              "maxwatts FLOAT,"
              "maxvolts FLOAT,"
              "maxamps FLOAT,""minwatts FLOAT,"
              "minvolts FLOAT,"
              "minamps FLOAT,"
              "powerfactor FLOAT,"
              "powercycle FLOAT,"
              "frequency FLOAT,"
              "voltamps FLOAT,"
              "relaynotclosed BOOLEAN,"
              "sendrate FLOAT,"
              "machineid INTEGER,"
              "credit FLOAT)" % (self.tablename,))


        queries = (q1, q2, q3, q4)


        dl = defer.gatherResults([
            self._pool.runOperation(query) for query in queries])
        #for python 2.7
        #dl = defer.gatherResults([
        #    self._pool.runOperation(query) for query in queries],
        #    consumeErrors=True)

        dl.addCallback(
            lambda ignored: log.msg("finished creating table: %s" % (self.tablename,)))
        return dl

    def _save(self, txn, logs):
        log.msg("saving logs for %s" % (self.tablename,))
        
        

        txn.executemany(
            "INSERT INTO %s ("
            "circuitid,timestamp,watts,volts,amps,"
            "watthourssc20,watthourstoday,"
            "maxwatts,maxvolts,maxamps,"
            "minwatts,minvolts,minamps,"
            "powerfactor,powercycle,frequency,"
            "voltamps,relaynotclosed,sendrate,"
            "machineid,credit"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % (
                self.tablename), logs    )

        log.msg("finished saving logs for %s" % (self.tablename,))

        return None

 
class MyPostgresConnPool(DBConnPool):

    def __init__(self, outdir, sitename, circuitid, year, user, password, host, port):
        super(MyPostgresConnPool, self).__init__(outdir, sitename, circuitid, year)

        self._pool = ConnectionPool(
            'psycopg2',
            database=self.dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        log.msg("getting database connection: %s" % (self.dbname,))

    def create_table(self):
        log.msg("creating table: %s" % (self.tablename,))


def gen_validate(sources):
    for source in sources:
        header = source.readline()

        # skip files without headers
        if header.lower().startswith("time"):

            # adjust for header column names ordered differently
            colnames = header.strip().lower().replace(' ', '').split(',')
            circuitid = int(source.name.rsplit('/', 1)[1].split('.', 1)[0].rsplit('_', 1)[1][1:])
        else:
            files_without_headers.append(source.name)
            continue

        prev_timestamp = None
        for line in source:
            d = dict(zip(colnames, line.strip().split(',')))
            if d.get('credit') is None:  # MAINS
                d['credit'] = 0.0
            d['circuitid'] = circuitid
            try:
                for name, cast in casts.items():
                    d[name] = cast(d[name])
                if prev_timestamp is None or d['timestamp'] != prev_timestamp:
                    yield (
                        unicode(d['circuitid']), unicode(d['timestamp']),
                        unicode(d['watts']), unicode(d['volts']), unicode(d['amps']),
                        unicode(d['watthourssc20']), unicode(d['watthourstoday']),
                        unicode(d['maxwatts']), unicode(d['maxvolts']), unicode(d['maxamps']),
                        unicode(d['minwatts']), unicode(d['minvolts']), unicode(d['minamps']),
                        unicode(d['powerfactor']), unicode(d['powercycle']), unicode(d['frequency']),
                        unicode(d['voltamps']), unicode(d['relaynotclosed']), unicode(d['sendrate']),
                        unicode(d['machineid']), unicode(d['credit']))
                prev_timestamp = d['timestamp']
            except:
                corrupted_files[source.name] += 1


def main(logdir, dbdir, max_concurrent):
    log.startLogging(sys.stdout)
    if not os.path.exists(dbdir):
        try:
            log.msg('mkdir -p: %s' % (dbdir,))
            os.makedirs(dbdir)
        except OSError as err:
            if err.errno == errno.EEXIST:
                pass
            else:
                raise

    log.msg("%s\n%s" % (logdir, '-' * len(logdir)))

    sitename = logdir.rsplit('/', 1)[1]
    sitename = sitename.lower()
    print "logdir="+str(logdir)
    years = map(int, os.listdir(logdir))

    # get a list of circuit IDs
    log.msg("building the circuit list...")
    import base
    num_logfiles = 0
    circuitids = set()
    for logfile in base.gen_find("*.log", logdir):
        #print "logfile="+str(logfile)
        num_logfiles += 1
        circuitids.add(int(
            logfile.rsplit('/', 1)[1].split('.', 1)[0].rsplit('_', 1)[1][1:]))
    circuitids = sorted(circuitids)
 
    # for ug01 ckt 0 we see errors
    #circuitids=[7]
    print "num_logfiles="+str(num_logfiles)

    # following works for python 2.6
    log.msg('# log files: {0:06}'.format(num_logfiles)   )

    # following works for python 2.7
    #log.msg('# log files: {:06}'.format(num_logfiles)   )


    # following works for python 2.6
    log.msg('# logged circuits: {0}'.format(len(circuitids),))

    #following works for python 2.7
    #log.msg('# logged circuits: {:,}'.format(len(circuitids),))


    # below works for python 2.7 only
    #log.msg('# log files: {:,}'.format(num_logfiles,)    )

    #below works for python 2.7 only
    #log.msg('# logged circuits: {:,}'.format(len(circuitids),))
    
    log.msg("done. following is circuitids,sitename,years")
    log.msg(circuitids)
    log.msg(sitename)
    log.msg(years)
 
    for yr in years:
        for cid in circuitids:
            conn = sqlite3.connect(
                os.path.join(dbdir, "%s_%d_%d.db" % (sitename, cid, yr)))
            #conn.text_factory = sqlite3.OptimizedUnicode
            #conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
            #conn.text_factory=str # fix for You must not use 8-bit bytestrings unless you use a text_factory that can interpret 8-bit bytestring
            conn.commit()
            conn.close()

    #following works for python 2.6
    gen_filenames = (
        ((cid, yr), base.gen_find("192_168_1_2{0:02}.log".format(cid),
                                  os.path.join(logdir, str(yr)))
         ) for cid in circuitids for yr in years)

    #following works for Python 2.7
    #gen_filenames = (
    #    ((cid, yr), base.gen_find("192_168_1_2{:02}.log".format(cid),
    #                              os.path.join(logdir, str(yr)))
    #     ) for cid in circuitids for yr in years)



    gen_files = ((key, base.gen_open(filenames)) for (key, filenames) in gen_filenames)
    gen_logs = dict(((key, gen_validate(files)) for (key, files) in gen_files))

    dbpools = [
        ((cid, yr), SqliteConnPool(dbdir, sitename, cid, yr)
         ) for cid in circuitids for yr in years]

    d = defer.DeferredList(
        [dbpool.create_table() for (ignored, dbpool) in dbpools])



    def cb_save(ignored):
        log.msg("saving")
        _sem = defer.DeferredSemaphore(max_concurrent)
        return defer.DeferredList(
            [_sem.run(dbpool.save, gen_logs[key]) for (key, dbpool) in dbpools])
    d.addCallback(cb_save)
    d.addErrback(log.err)

    def cb_shutdown(ignored):
        log.msg("shutting down")
        for (ignore, dbpool) in dbpools:
            log.msg(dbpool)
            dbpool.shutdown()
    d.addCallback(cb_shutdown)
    d.addErrback(log.err)

    d.addCallback(lambda ignored: reactor.stop())

    reactor.run()

if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 4, \
        "Usage: python final.py <logdir> <dbdir> <max_concurrent>"
    logdir = sys.argv[1]
    dbdir = sys.argv[2]
    max_concurrent = int(sys.argv[3])
    assert os.path.exists(logdir), "directory does not exist: %s" % (logdir,)

    main(logdir, dbdir, max_concurrent)
