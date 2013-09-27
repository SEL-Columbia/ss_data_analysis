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


if __name__=="__main__":
    li=[]
    li=sys.argv
    if len(li)!= 3:
      print "Invalid syntax"
      print "Usage: python getpdfDifferentialEnergy.py <site> <circuit> "
      print "example:  python getpdfDifferentialEnergy.py ug03 0 "
      exit(0)

    site=sys.argv[1]
    ckt= sys.argv[2]


    _d=[]

    v={}
    print "site="+site
    print "ckt="+ckt

    v={}

    for basedir in ['apan','apan2','apan3']:
      for year in ['2010',  '2011','2012','2013']:

        try:
 
           conn=sqlite3.connect("/home/"+basedir+"/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
           c=conn.cursor()
           for (ts,w) in c.execute("select ts,wh  from hour_res_watthours"):
             v[ts]=w

        except Exception,e:
           print "Exception caught!!!"
           print e
           
           
        finally:
           c.close()
           conn.close()

    print "DONE!!!"
    print "len(v.values())"
    print len(v.values())  
    diff=[]
    previous=None
    count=0
    for ts in sorted(v.keys()):
        _year= int(ts[0:4])
        _month= int(ts[5:7])
        _day= int(ts[8:10])
        _hour= int(ts[11:13])
        _minute= int(ts[14:16])
        current=datetime.datetime( _year, _month, _day,_hour, _minute)
        if count>0 and previous!=None:
           if current-previous==datetime.timedelta(hours=1):
             if prevwh!=0:
                diff.append(v[ts]/prevwh)

           else:
                print "time delta not one hour"
        count=count+1
        previous=current
        prevwh=v[ts]
    print "count="+str(len(diff)) 
    L=len(diff)
    _min=0
    _max=0.2
    pos=[]
    xpos=[]
    for i in range(50):

      cnt=sum([1   for x in diff if x>=_min and x<=_max])

      
      if cnt>0:
         print "min="+str(_min)+"max="+str(_max)+"count="+str(cnt)
      pos.append(cnt)
      xpos.append(str(_min)+"-"+str(_max))
      _min=_min+0.2
      _max=_max+0.2


    _min=-0.2
    _max=0
    neg=[]
    xneg=[]
    for i in range(50):

      cnt=sum([1   for x in diff if x>=_min and x<=_max])
      
      if cnt>0:
         print "min="+str(_min)+"max="+str(_max)+"count="+str(cnt)
      neg.append(cnt)
      xneg.append(str(_min)+"-"+str(_max))
      _min=_min-0.2
      _max=_max-0.2
     
    neg.reverse()
    xneg.reverse()
    
    _d=neg+pos
    alphab=[]
    alphab=xneg+xpos

    frecuencies = _d


    pos = np.arange(len(alphab))



    width = 1.0     # gives histogram aspect to the bar diagram

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))

    xtickNames=ax.set_xticklabels(alphab)
    plt.setp(xtickNames,rotation=90,fontsize=8)
    ax.set_ylabel('count')

    ax.set_xlabel('ratio watt hours from hour to hour')
    ax.set_title("Histogram of energy change between hours for site "+site)

    plt.bar(pos, frecuencies, width, color='r')


    plt.subplots_adjust(bottom=0.2)      
    plt.savefig("/root/ts/timeseries/timeseries/static/images/RatioEnergy"+site+".png")

##end
