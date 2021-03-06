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
      print "Usage: python getpdfEnergy.py <site> <circuit> "
      print "example:  python getpdfEnergy.py ug03 0 "
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

           #for (ts,w) in c.execute("select ts,wh  from minute_res_watthours"):
           #  v[ts]=w

           for (ts,w) in c.execute("select ts,wh  from hour_res_watthours"):
             v[ts]=w

        except Exception,e:
           print "Exception caught!!!"
           print e
           
           
        finally:
           ###c.close()
           conn.close()

    print "DONE!!!"
    print "len(v.values())"
    print len(v.values())
 

#    r = [(0,0.2), (0.2,0.4), (0.4,0.6), (0.6,0.8), (0.8,1.0),(1.0,1.2), (1.2,1.4), (1.4,1.6),(1.6,1.8),(1.8,2.0),(2.0,2.2),(2.2,2.4),(2.4,2.6),(2.6,2.8),(2.8,3.0),(3.0,3.2)]
 
  #  r = [(0,10),(10,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,80),(80,90),(90,100),(100,110),(110,120),(120,130),(130,140),(140,150),(150,160),(160,170),(170,180),(180,190),(190,200),(200,210),(210,220),(220,230),(230,240),(240,250),(250,260),(260,270),(270,280),(280,290),(290,300)]

    _d1=[]
    _d2=[]

    r=[]
    alphab=[]
    _min=0
    _max=5
    for i in range(100):
      r.append((_min,_max))
      alphab.append(str(_min)+"-"+str(_max) ) 
      _min=_min+5
      _max=_max+5

    for (_min,_max) in r:

      cnt=0
      cnt=sum([1   for ts,wh in v.items() if wh>=_min and wh<=_max  ] )
      if cnt==0:
         _d.append(0)
      else:
         _d.append(cnt)


    print "len(_d1):"
    print len(_d1)


    #alphab =  ['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1.0','1.0-1.2', '1.2-1.4', '1.4-1.6','1.6-1.8','1.8-2.0','2.0-2.2','2.2-2.4','2.4-2.6','2.6-2.8','2.8-3.0','3.0-3.2']

    #alphab = ['0,10','10,20','20,30','30,40','40,50','50,60','60,70','70,80','80,90','90,100','100,110','110,120','120,130','130,140','140,150','150,160','160,170','170,180','180,190','190,200','200,210','210,220','220,230','230,240','240,250','250,260','260,270','270,280','280,290','290,300']
      
    frecuencies = _d

    pos = np.arange(len(alphab))

    width = 1.0     # gives histogram aspect to the bar diagram

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))

    xtickNames=ax.set_xticklabels(alphab)
    plt.setp(xtickNames,rotation=60,fontsize=10)
    ax.set_ylabel('count')

    ax.set_xlabel('range of hourly watt hours')
    ax.set_title("Hourly Energy Histogram site "+site+"(comprehensive)")

    plt.bar(pos, frecuencies, width, color='r')
    plt.subplots_adjust(bottom=0.2)      
    plt.savefig("/root/ts/timeseries/timeseries/static/images/NewHourlyEnergyHist"+site+".png")
