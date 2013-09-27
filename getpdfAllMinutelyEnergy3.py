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

    for basedir in ['apan','apan2', 'apan3']:
      for year in ['2010',  '2011','2012','2013']:

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

    r = [(0,0.4), (0.4,1.0), (1.0,3.0), (3.0,6.0), (6.0,9.0), (9.0,12.0)]
    _d1=[]
    _d2=[]
    _d3=[]
    _d4=[]
    _d5=[]
    _d6=[]

    for (_min,_max) in r:

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=0 and int(ts[11:13])<=4 )] )
      if cnt==0:
         _d1.append(0)
      else:
         _d1.append(cnt)

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=4 and int(ts[11:13])<=8 )] )
      if cnt==0:
         _d2.append(0)
      else:
         _d2.append(cnt)

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=8 and int(ts[11:13])<=12 )] )
      if cnt==0:
         _d3.append(0)
      else:
         _d3.append(cnt)

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=12 and int(ts[11:13])<=16 )] )
      if cnt==0:
         _d4.append(0)
      else:
         _d4.append(cnt)

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=16 and int(ts[11:13])<=20 )] )
      if cnt==0:
         _d5.append(0)
      else:
         _d5.append(cnt)

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if (wh>=_min and wh<=_max and int(ts[11:13])>=20 and int(ts[11:13])<=24 )] )
      if cnt==0:
         _d6.append(0)
      else:
         _d6.append(cnt)

    alphab =  ['0-0.4', '0.4-1.0', '1.0-3.0', '3.0-6.0', '6.0-9.0','9.0-12.0']
    
    fig=plt.figure()
    ax=fig.add_subplot(111)
    N=len(alphab)
    ind=np.arange(N)
    width=0.1

      # the bars:                                                                                                        \
    print "_d1[]"
    print _d1                                                                                                                          
    rects1=ax.bar(ind+0*width,_d1,width,color='red')
    rects2=ax.bar(ind+1*width,_d2,width,color='green')
      # axes and labels                                                                                                  \
                                                                                                                          
    rects3=ax.bar(ind+2*width,_d3,width,color='yellow')
    rects4=ax.bar(ind+3*width,_d4,width,color='blue')
      # axes and labels                                                                                                  \
                                                                                                                          
    rects5=ax.bar(ind+4*width,_d5,width,color='orange')
    rects6=ax.bar(ind+5*width,_d6,width,color='violet')
      # axes and labels                                                                                                  \
                                                                                                                          
    ax.set_xlim(-width,len(ind)+width)
    ax.set_ylim(0,max(_d1+_d2+_d3+_d4+_d5+_d6)+1)
    ax.set_ylabel('count')
    ax.set_xlabel('range of watt hours consumed in one minute')
    ax.set_title("Energy Histogram site "+site+" ckt "+ckt)

    xTickMarks=alphab
#    ax.set_xticks(ind+width)

    xtickNames=ax.set_xticklabels(xTickMarks)
    plt.setp(xtickNames,fontsize=10,horizontalalignment='left')
    ax.legend( (rects1[0],rects2[0] ,rects3[0],rects4[0],rects5[0],rects6[0] ),('AM:00-04','AM:04-08','AM:08-12','PM:12-16','PM:16-20','PM:20-24'  ) )
    plt.subplots_adjust(bottom=0.2)

 
    plt.savefig("/root/ts/timeseries/timeseries/static/images/emintod"+site+".png")

