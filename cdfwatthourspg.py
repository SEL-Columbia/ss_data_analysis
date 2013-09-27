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
import psycopg2  

cdf=[]
tsArr=[]

def calcWH(cdf):
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
         if (cdf[i]<_start):
            print "DATA UNRELIABLE!!!!!!!"
            return -666
    return _sum 



def getWatthours():
          
    try:


 
       conn=psycopg2.connect("dbname='productionclone' user='postgres' host='localhost' password='postgres'")
       print "got pg connection"

       cur2=conn.cursor()
       print "obtained cursor"
       cur2.execute("select l.date,pl.watthours from log l,primary_log pl where (l.id=pl.id) and  (l.date between '2012-11-01' and '2012-11-30') and (pl.circuit_id=25) order by l.date asc")

       print "finished execute"
       rows=cur2.fetchall()
       print "finished fetch all"
       print "number of rows from database="+str(len(rows))

       prev="0123456789z"
       curr=""
    

       for row in rows:
           (ts,w) = row

           print "ts, watthourss=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              cdf.append(w)
              tsArr.append(ts)
              prev=curr
       print "finished watt hours"      
       print "cdf[0]="+str(cdf[0])
       print "cdf[last]="+str(cdf[len(cdf)-1])      
       cur2.close()             
       conn.close()
       conn=None
       return cdf       
    except Exception,e:
       print e
       return []

#### syntax python cdfwatthours.py /home/apan/sqllite/ml00 

if __name__=="__main__":
 

    getWatthours()
    totalwh=calcWH(cdf)
   
    print "++++++++++++++++++++++++ TOTAL COMPUTED WATT HOUR++++++++++++++++++++++++++++++++++++++++++"
    print str(totalwh)+" watthours "
    print "++++++++++++++++++++++++ TOTAL COMPUTED WATT HOUR++++++++++++++++++++++++++++++++++++++++++"

    #N=len(cdf)
    #t=arange(0,1,1./N)
    #x=cdf
    #plot(t,x)
    #ylim(0,max(cdf))  
    #savefig('/root/ml00_june2011_watthours.pdf')
