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

d={}
wd={}
 
def getCredit(dirpath,_file):
           
    try:
       conn=sqlite3.connect(dirpath+"/"+_file)
       c=conn.cursor()



       prev="0123456789z"
       curr=""
      
     
       cmd="select ts,watts from daily_res_credit where ts like '2011-04%'order by ts";
       for (ts,c)  in c.execute(cmd):  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              day=ts[0:10]
              if d.has_key(day):
                 d[day]+=c
              else:
                 d[day]=c
              prev=curr
       print "finished loading credit for"+_file      

       conn.close()
       conn=None
    except Exception,e:
             print e

if __name__=="__main__":

    dirpath='/home/apan/sqllite/ml00'           
    
    for i in range(12):
       getCredit(dirpath,"ml00_"+str(i+1)+"_2011.db")

    print "finished summing credit over all circuits"

    _d=[]
  
  
    dt=datetime.datetime(2011,04,01)
    for i in range(30): 
       a=dt.strftime("%Y-%m-%d")
       if d.has_key(a):    
          _d.append(d[a])
       else:
          print "d[] does not contain key="+str(a)
       dt=dt+datetime.timedelta(days=1)
    N=len(_d)
    t=arange(0,1,1./N)
    x=_d
    plot(t,x)
    ylim(0,max(_d))  
    savefig('/root/cumulative_credit_ml00_apr2011.pdf')

