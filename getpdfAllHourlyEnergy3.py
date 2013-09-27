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
      print "Usage: python getpdfAllHourlyEnergy3.py <site> <circuit> "
      print "example:  python getpdfAllHourlyEnergy3.py ug03 0 "
      exit(0)

    site=sys.argv[1]
    ckt= sys.argv[2]


    _d=[]

    v={}
    print "site="+site
    print "ckt="+ckt

    v={}

    for basedir in ['apan','apan2','apan3']:
      for year in ['2010',  '2011', '2012', '2013']:
  
        try:
 
           conn=sqlite3.connect("/home/"+basedir+"/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
           c=conn.cursor()
           for (ts,w) in c.execute("select ts,wh  from hour_res_watthours"):
             v[ts]=w

        except Exception,e:
           print "Exception caught!!!"
           print e
           
           
        finally:
           conn.close()

    print "DONE!!!"
    print "len(v.values())"
    print len(v.values())  

    r = [(0,10),(10,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,80),(80,90),(90,100),(100,110),(110,120),(120,130),(130,140),(140,150),(150,160),(160,170),(170,180),(180,190),(190,200),(200,210),(210,220),(220,230),(230,240),(240,250),(250,260),(260,270),(270,280),(280,290),(290,300)]

    _d1=[]
    _d2=[]

    _MIN=-200
    _MAX=-205
    xpos=[]
    for (_min,_max) in r:
    #for i in range(80):
      cnt=0
      cnt=sum([1   for ts,wh in v.items() if wh>=_min and wh<=_max   ] )
      _d1.append(cnt)
      #xpos.append(str(_MIN)+"-"+str(_MAX))
      #_MIN=_MIN+5
      #_MAX=_MAX+5

    print "len(_d1):"
    print len(_d1)


    ##alphab = ['0-25', '25-50', '50-75', '75-100', '100-200','200-400', '400-800']


    alphab = ['0,10','10,20','20,30','30,40','40,50','50,60','60,70','70,80','80,90','90,100','100,110','110,120','120,130','130,140','140,150','150,160','160,170','170,180','180,190','190,200','200,210','210,220','220,230','230,240','240,250','250,260','260,270','270,280','280,290','290,300']

    fig=plt.figure()
    ax=fig.add_subplot(111)
    N=len(alphab)
    ind=np.arange(N)
    width=0.1

      # the bars:                                                                                                        \             

    rects1=ax.bar(ind+0*width,_d1,width,color='red')
    #rects2=ax.bar(ind+1*width,_d2,width,color='green')

    ax.set_xlim(-width,len(ind)+width)
    ax.set_ylim(0,max(_d1)+1)
    ax.set_ylabel('count')
    ax.set_xlabel('range of watt hours consumed in one hour')
    ax.set_title("Energy Histogram site "+site+" ckt "+ckt)

    xTickMarks=alphab
    ax.set_xticks(ind+width)

    xtickNames=ax.set_xticklabels(xTickMarks)
    plt.setp(xtickNames,fontsize=10,horizontalalignment='left')
    #ax.legend( (rects1 ),('all hours',  ) )
    plt.subplots_adjust(bottom=0.2)





    plt.savefig("/root/ts/timeseries/timeseries/static/images/DetailedEnergyHist"+site+".png")

