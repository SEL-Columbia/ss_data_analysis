import matplotlib
matplotlib.use('Agg')
import pylab



import os
import datetime
import time
import sqlite3
import pywt
from pylab import *
import fnmatch, gzip, os,  re, sys, time
import numpy as np
  

cdf=[]
tsArr=[]
v={}

def calcWH(cdf,tsArr):
    if len(cdf)==0:
       return 0
    _sum=0
    _start=cdf[0]
    _startTS=tsArr[0]
    for i in range(len(cdf)):
       if i>=1:
         #print "sum so far="+str(_sum)   
         if cdf[i]>_start:
            _sum=_sum+(cdf[i]-_start)
            _start=cdf[i]
            _startTS=tsArr[i]
         if (cdf[i]<_start) and (cdf[i]>(0.8*_start)):## oscillation
            #_start=_start
            #_sum=_sum
            continue
         if (cdf[i]<_start) and (cdf[i]<(0.2*_start)):## reset
            _sum=_sum+cdf[i]
            _start=cdf[i]
            _startTS=tsArr[i]
         if (cdf[i]<_start) and (cdf[i]>(0.2*_start)) and (cdf[i]<(0.8*_start))  :#
            print "***************  COULD NOT INTERPRET WATT HOURS DATA ********************************************"
            print "_start="+str(_start)
            print "_startTS="+str(_startTS)
            print "cdf[i]="+str(cdf[i])
            print "tsArr[i]="+str(tsArr[i])
         
            #_sum=_sum+cdf[i]
            #_start=cdf[i]
            #_startTS=tsArr[i]
          
            return -666
    return _sum 



def getWatthours(site,year,criteria):
          
    try:

       conn=sqlite3.connect("/home/apan/sqllite/"+site+"/"+site+"_0_"+year+".db")

       c=conn.cursor()
       prev="0123456789z"
       curr=""
    
       cmd="select timestamp,watthourssc20 from "+site+"_0_"+year+" where timestamp like '"+criteria+"' order by timestamp"
    
       for (ts,w)  in c.execute(cmd):
           print "ts, watthourss=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              #cdf.append(w)
              #tsArr.append(ts)
              v[ts]=w
              prev=curr
       c.close()
       conn.close()
       conn=None
       
       conn=sqlite3.connect("/home/apan2/sqllite/"+site+"/"+site+"_0_"+year+".db")

       c=conn.cursor()
       prev="0123456789z"
       curr=""
    
       cmd="select timestamp,watthourssc20 from "+site+"_0_"+year+" where timestamp like '"+criteria+"' order by timestamp"
    
       for (ts,w)  in c.execute(cmd):
           print "ts, watthourss=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              #cdf.append(w)
              #tsArr.append(ts)
              v[ts]=w
              prev=curr
       print "finished watt hours"      
       
       c.close()      
       conn.close()
       conn=None
       for ts in sorted(v.keys()):
         cdf.append(v[ts])
         tsArr.append(ts)


       return cdf       
    except Exception,e:
       print e
       return []
#### assuming for Uganda only
#### syntax python cdfwatthours.py ug03 2012 

if __name__=="__main__":
    site=sys.argv[1]
    year=sys.argv[2]
  
    whHash={}

    for i in ["01","02","03","04","05","06","07","08","09","10","11","12"]:

       cdf=[]
       tsArr=[]
       criteria=str(year)+ "-"+i+"-%"
       getWatthours(site,year,criteria)
       totalwh=calcWH(cdf,tsArr)
       whHash[i]=totalwh
   
    print "+++++++++++++++++++++++++++++++++  FINAL RESULTS +++++++++++++++++++++++++++++++++++++++++++++++"

    print whHash
    print "+++++++++++++++++++++++++++++++++  FINAL RESULTS +++++++++++++++++++++++++++++++++++++++++++++++"

    #N=len(cdf)
    #t=arange(0,1,1./N)
    #x=cdf
    #plot(t,x)
    #ylim(0,max(cdf))  
    #savefig('/root/ml00_june2011_watthours.pdf')
