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
      
     
       cmd="select ts,watts from daily_res_credit order by ts";
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
 
def getAvgWatts(ymd):
           
    try:
       conn=sqlite3.connect("/home/apan/sqllite/ml02/ml02_0_2011.db")
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

    dirpath='/home/apan/sqllite/ml02'           
    
    for i in range(10):
       getCredit(dirpath,"ml02_"+str(i+1)+"_2011.db")
    getCredit(dirpath,"ml02_12_2011.db")
    getCredit(dirpath,"ml02_19_2011.db")


    print "finished summing credit over all circuits"

    for (a,b) in d.items():
       count=0
       while count==0 :
          dt=datetime.datetime(int(a[0:4]),int(a[5:7]),int(a[8:10]))
          dt=dt+datetime.timedelta(days=1)
          a=dt.strftime("%Y-%m-%d")
          count=getAvgWatts(str(a))
          if count==0:
             print "warning count==0 for date="+a
             

    print "********************credit dictionary start**********************************"
    print d
    print "********************credit dictionary end  **********************************"

    print "********************watts dictionary start**********************************"
    print wd
    print "********************watts dictionary end  **********************************"
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

    pylab.savefig("/root/ug08_scatterplot_credit_avgwatts.pdf")
