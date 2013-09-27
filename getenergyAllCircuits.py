import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

import scipy.stats as norm
import os
import datetime
import time
import sqlite3
import pywt
from pylab import *
import fnmatch, gzip, os,  re, sys, time
import numpy as np
import datetime
import time
import sqlite3
import sys
import math
import matplotlib.mlab as mlab

import sqlite3
import csv, codecs, cStringIO

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f", 
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([unicode(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

if __name__=="__main__":
    li=[]
    li=sys.argv

    if len(li)!= 3:
      print "Invalid syntax"
      print "Usage: python getenergyAllCircuits.py <site> <total number circuits> "
      print "example:  python getenergyAllCircuits.py ug01 11"
      exit(0)

    site=sys.argv[1]
    print "site="+site

    numckts=sys.argv[2]
    print "number of circuits="+numckts
    totalckts=int(numckts)

    li=[]
    cur=[]
    try:
      for ckt in range(totalckts):
        li.append(sqlite3.connect("/home/apan3/sqllite/"+site+"/"+site+"_"+str(ckt)+"_2013.db"))
      
      for ckt in range(totalckts):
        cur.append(li[ckt].cursor())


      line=""
      startts=datetime.datetime(2012,12,31,23,0,0)

      currentts=startts
      for i in range(2900):

         line=line+currentts.__str__()+","
         for ckt in range(totalckts):
            cur[ckt].execute("select ts,wh from hour_res_watthours where ts=?",(currentts.__str__(),))
            data=cur[ckt].fetchall()
            if len(data)==0:
               line=line+"*,"
            else:
               if len(data)==1:
                 line=line+str(data[0][1])+","

         line=line+"\n"
         currentts=currentts+datetime.timedelta(hours=1)

      print line
      for ckt in range(totalckts):
         cur[ckt].close()
      for ckt in range(totalckts):
         li[ckt].close()

    except Exception,e:
      print "exception detected"
      print e
