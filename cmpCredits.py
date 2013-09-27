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
         if cdf[i]>cdf[i-1]:
            _sum=_sum+(cdf[i]-cdf[i-1])
 
 
    return _sum 





def getCreditsTablet(circuitid,startdate,enddate,db):
          
    try:


 
       conn=psycopg2.connect("dbname='"+db+"' user='postgres' host='localhost' password='postgres'")
       print "got pg connection"

       cur2=conn.cursor()
       print "obtained cursor"

       cur2.execute("select n1.start,n1.credit from (select j.id, j.start,t.credit from jobs j,tabletcredit t where  (j.id=t.id) and j.circuit_id="+str(circuitid)+") n1 where n1.start between '"+str(startdate)+"' and '"+str(enddate)+"'")


       print "finished execute"
       rows=cur2.fetchall()
       print "finished fetch all"
       print "number of rows from database="+str(len(rows))

       prev="0123456789z"
       curr=""
    
       _sum=0
       for row in rows:
           (ts,w) = row

           print "TABLET ts, credits=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              _sum=_sum+w
              prev=curr
   
       cur2.close()             
       conn.close()
       conn=None
       return _sum
    except Exception,e:
       print e
       return []


def getCreditsScratchcard(circuitid,startdate,enddate,db):
          
    try:


 
       conn=psycopg2.connect("dbname='"+db+"' user='postgres' host='localhost' password='postgres'")
       print "got pg connection"

       cur2=conn.cursor()
       print "obtained cursor"

       cur2.execute("select n1.start,n1.credit from (select j.id, j.start,t.credit from jobs j,addcredit t where  (j.id=t.id) and j.circuit_id="+str(circuitid)+") n1 where n1.start between '"+str(startdate)+"' and '"+str(enddate)+"'")


       print "finished execute"
       rows=cur2.fetchall()
       print "finished fetch all"
       print "number of rows from database="+str(len(rows))

       prev="0123456789z"
       curr=""
    
       _sum=0
       for row in rows:
           (ts,w) = row

           print "SCRATCHCARD: ts, credits=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              _sum=_sum+w
              prev=curr
   
       cur2.close()             
       conn.close()
       conn=None
       return _sum
    except Exception,e:
       print e
       return []

def getCreditsPrimaryParameters(circuitid,startdate,enddate,db):
          
    try:


 
       conn=psycopg2.connect("dbname='"+db+"' user='postgres' host='localhost' password='postgres'")
       print "got pg connection"

       cur2=conn.cursor()
       print "obtained cursor"
       cur2.execute("select l.date,pl.credit from log l,primary_log pl where (l.id=pl.id) and  (l.date between '"+
       str(startdate)+"' and '"+str(enddate)+"') and (pl.circuit_id="+str(circuitid)+") order by l.date asc")

       print "finished execute"
       rows=cur2.fetchall()
       print "finished fetch all"
       print "number of rows from database="+str(len(rows))

       prev="0123456789z"
       curr=""
    

       for row in rows:
           (ts,w) = row

           print "PRIMARY PARAMETERS: ts, credits=",str(ts),str(w)  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              if w<0:
                w=0
              cdf.append(w)
              tsArr.append(ts)
              prev=curr

       cur2.close()             
       conn.close()
       conn=None
       return cdf       
    except Exception,e:
       print e
       return []

#### syntax python cmpCredits.py <circuit id> <start date> <end date> <database name>
####   example:   python cmpCredits.py 42 2013-02-01 2013-02-19 productionclone

if __name__=="__main__":
    circuitid=sys.argv[1]
    startdate=sys.argv[2]
    enddate=sys.argv[3]
    db=sys.argv[4]
    
    print "circuitid="
    print circuitid

    cdf=[]
    tsArr=[]
    getCreditsPrimaryParameters(circuitid,startdate,enddate,db)
    measuredcredit=calcWH(cdf)

    
    
    tabletcredit=getCreditsTablet(circuitid,startdate,enddate,db)
    
   
    scratchcardcredit=getCreditsScratchcard(circuitid,startdate,enddate,db)
    
   
    print "++++++++++++++++++++++++ TOTAL COMPUTED CREDITS++++++++++++++++++++++++++++++++++++++++++"
    print "total measured credit="+str(measuredcredit)+" credits "

    print "total credit(gateway recorded)="+str(tabletcredit+scratchcardcredit)

    if float(measuredcredit) - (float(tabletcredit)+float(scratchcardcredit)) >0:
      print "DISCREPANCY DETECTED===================>"+str(float(measuredcredit) - (float(tabletcredit)+float(scratchcardcredit)))

    print "++++++++++++++++++++++++ TOTAL COMPUTED CREDITS++++++++++++++++++++++++++++++++++++++++++"
  
    #N=len(cdf)
    #t=arange(0,1,1./N)
    #x=cdf
    #plot(t,x)
    #ylim(0,max(cdf))  
    #savefig('/root/ml00_june2011_watthours.pdf')
