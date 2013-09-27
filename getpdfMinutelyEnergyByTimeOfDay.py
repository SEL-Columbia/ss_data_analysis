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
      print "Usage: python getpdfAllMinutelyEnergy.py <site> <circuit> "
      print "example:  python getpdfAllMinutelyEnergy.py ug03 0 "
      exit(0)

    site=sys.argv[1]
    ckt= sys.argv[2]


    _d=[]

    v={}
    print "site="+site
    print "ckt="+ckt

    v={}

    for basedir in ['apan','apan2']:
      for year in ['2010',  '2011','2012']:

        try:
 
           conn=sqlite3.connect("/home/"+basedir+"/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
           c=conn.cursor()
           for (ts,w) in c.execute("select ts,wh  from minute_res_watthours"):
             ##print "data read so far"+str(ts)+str(w)
             v[ts]=w

        except Exception,e:
           print "Exception caught!!!"
           print e
           
           
        finally:
           ####c.close()
           conn.close()

    print "DONE!!!"
    print "len(v.values())"
    print len(v.values())  

    r = [(0,0.2), (0.2,0.4), (0.4,0.6), (0.6,0.8), (0.8,1.0),(1.0,1.2), (1.2,1.4), (1.4,1.6),(1.6,1.8),(1.8,2.0),(2.0,2.2),(2.2,2.4),(2.4,2.6),(2.6,2.8),(2.8,3.0),(3.0,10.0)]

    for (_min,_max) in r:
      cnt=sum([1   for x in v.values() if x>=_min and x<=_max])
      if cnt==0:
         _d.append(0)
      else:
         _d.append(math.log10(cnt))

    alphab =  ['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1.0','1.0-1.2', '1.2-1.4', '1.4-1.6','1.6-1.8','1.8-2.0','2.0-2.2','2.2-2.4','2.4-2.6','2.6-2.8','2.8-3.0','3.0-10.0']


#['0-1', '1-5', '5-10', '10-20', '20-25','25-30', '30-35','35-40','40-50','50-60','60-80','80-100','100-200','200-400','400-800']
      
    frecuencies = _d

    pos = np.arange(len(alphab))

    width = 1.0     # gives histogram aspect to the bar diagram

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))

    xtickNames=ax.set_xticklabels(alphab)
    plt.setp(xtickNames,rotation=60,fontsize=10)
    ax.set_ylabel('log(count)')

    ax.set_xlabel('range of minutely watt hours')
    ax.set_title("Minutely Energy Histogram site "+site+"(comprehensive)")

    plt.bar(pos, frecuencies, width, color='r')
    plt.subplots_adjust(bottom=0.2)      
    plt.savefig("/root/ts/timeseries/timeseries/static/images/MinuteEnergyHist"+site+".png")
