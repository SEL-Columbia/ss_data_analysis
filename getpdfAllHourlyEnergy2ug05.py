import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

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

if __name__=="__main__":
    li=[]
    li=sys.argv
    if len(li)!= 3:
      print "Invalid syntax"
      print "Usage: python getpdfAllHourlyEnergy2ug05.py <site> <circuit> "
      print "example:  python getpdfAllHourlyEnergy2ug05.py ug05 0 "
      exit(0)

    site=sys.argv[1]
    ckt= sys.argv[2]


    _d=[]


    #print "site="+site
    #print "ckt="+ckt

    v={}

    v0={}
    v1={}
    v2={}
    v3={}
    v4={}
    v5={}
    v6={}
    ecnt=0

    for basedir in ['apan','apan2','apan3']:
      for year in ['2010',  '2011', '2012','2013']:
        for cktParm in ['0','1','2','3','4','5','6']:

          try:
 
             conn=sqlite3.connect("/home/"+basedir+"/sqllite/"+site+"/"+site+"_"+cktParm+"_"+year+".db")
             c=conn.cursor()
             for (ts,w) in c.execute("select ts,wh  from hour_res_watthours"):
               if cktParm=='0':
                  v0[ts]=w
               if cktParm=='1':
                  v1[ts]=w
               if cktParm=='2':
                  v2[ts]=w
               if cktParm=='3':
                  v3[ts]=w
               if cktParm=='4':
                  v4[ts]=w
               if cktParm=='5':
                  v5[ts]=w
               if cktParm=='6':
                  v6[ts]=w


          except Exception,e:
             #print "Exception caught!!!"
             #print e
             ecnt=ecnt+1
           
          finally:
             conn.close()

    #for cktParm in ['0','1','2','3','4','5','6']:

    for tsParm in v0.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v0[tsParm]
         else:
            v[tsParm]=v0[tsParm]

    for tsParm in v1.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v1[tsParm]
         else:
            v[tsParm]=v1[tsParm]


    for tsParm in v2.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v2[tsParm]
         else:
            v[tsParm]=v2[tsParm]

    for tsParm in v3.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v3[tsParm]
         else:
            v[tsParm]=v3[tsParm]

    for tsParm in v4.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v4[tsParm]
         else:
            v[tsParm]=v4[tsParm]

    for tsParm in v5.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v5[tsParm]
         else:
            v[tsParm]=v5[tsParm]

    for tsParm in v6.keys():
         if v.has_key(tsParm):
            v[tsParm]=v[tsParm]+v6[tsParm]
         else:
            v[tsParm]=v6[tsParm]




    #print "DONE!!!"
    #print "len(v.values())"
    #print len(v.values())  

    r = [(0,25),(25,50),(50,75),(75,100),(100,200),(200,400),(400,800)]
    _d1=[]
    _d2=[]
    _d3=[]
    _d4=[]
    _d5=[]
    _d6=[]


    for (_min,_max) in r:

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=0 and int(ts[11:13])<=4 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", AM 00-04,"+" count=," +"%e" % cnt
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=4 and int(ts[11:13])<=8 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", AM 04-08,"+" count=," +"%e" % cnt
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=8 and int(ts[11:13])<=12 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", AM 08-12,"+" count=," +"%e" % cnt
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=12 and int(ts[11:13])<=16 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", PM 12-16,"+" count=," +"%e" % cnt
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=16 and int(ts[11:13])<=20 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", PM 16-20,"+" count=," +"%e" % cnt
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=20 and int(ts[11:13])<=24 )] )
      print "min wh=,"+str(_min)+",to max wh=,"+str(_max)+", PM 20-24,"+" count=," +"%e" % cnt
