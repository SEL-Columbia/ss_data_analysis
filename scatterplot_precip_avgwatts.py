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
 
def getAvgWatts(ymd):
           
    try:
       conn=sqlite3.connect("/home/apan/sqllite/ml00/ml00_0_2011.db")
       c=conn.cursor()

       print "averaging watts"+ymd

       prev="0123456789z"
       curr=""
      
       count=0

       cmd="select ts,watts from hour_res_watts where ts like '"+ymd+"%' order by ts";
       for (ts,w)  in c.execute(cmd):
           print "avg watts=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              count=count+1
              if wd.has_key(ymd):
                 wd[ymd]+=w
              else:
                 wd[ymd]=w
              prev=curr
       print "finished avg watts"      
       wd[ymd]=wd[ymd]/count
       print wd
       
       conn.close()
       conn=None
       return count
    except Exception,e:
             print e


if __name__=="__main__":

    dirpath='/home/apan/sqllite/ml00'           
    #avg precipitation in mm
    d['2011-01-']=1
    d['2011-02-']=0.5

    d['2011-03-']=5

    d['2011-04-']=16

    d['2011-05-']=60

    d['2011-06-']=150

    d['2011-07-']=246

    d['2011-08-']=311

    d['2011-09-']=230

    d['2011-10-']=70

    d['2011-11-']=8

    d['2011-12-']=1

    print "finished summing credit over all circuits"

    for (a,b) in d.items():
          count=getAvgWatts(str(a))
          if count==0:
             print "warning count==0 for date="+a
             

    #print "********************credit dictionary start**********************************"
    #print d
    #print "********************credit dictionary end  **********************************"

    #print "********************watts dictionary start**********************************"
    #print wd
    #print "********************watts dictionary end  **********************************"

    x=[]
    y=[]

    print "++++++++++++++++++++++++  starting to build scatter plot+++++++++++++++++++++++++++++"

    for (a,b) in d.items():
       if d.has_key(a):
          if wd.has_key(a):
             x.append(d[a])
             y.append(wd[a])


    print "++++++++++++++++++++++++  finished building scatter plot+++++++++++++++++++++++++++++"
    matplotlib.pyplot.scatter(x,y)

    matplotlib.pyplot.show()

    pylab.savefig("/root/ml00_scatterplot_precip_avgwatts.pdf")
