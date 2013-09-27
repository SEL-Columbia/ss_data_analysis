import matplotlib
matplotlib.use('Agg')



import os
import datetime
import time
import sqlite3
import pywt
from pylab import *
import fnmatch, gzip, os,  re, sys, time


def approx(x, wavelet, level):
    ca = pywt.wavedec(x, wavelet, level=level)
    ca = ca[0]
    return pywt.upcoef('a', ca, wavelet, level, take=len(x))  
  

if __name__=="__main__":
   for (dirpath,dirnames,filenames) in os.walk('/home/apan/sqllite/ml00'):

      print "*************************************************************************************************"
      for _file in filenames:

         if _file=="ml00_0_2011.db":
        
           print "FOUND====>"+  dirpath+"/"+_file
           cktstart=_file.find("_")
           cktend=_file.rfind("_")
           site=_file[0:cktstart]
           print "site="+site
           ckt=_file[cktstart+1:cktend]
           print "circuit="+ckt
           tableend=_file.rfind(".db")
           _table=_file[0:tableend]
           print "tablename="+_table
           yrstart=_file.rfind("_")
           yrend=_file.rfind(".")
           yr=_file[yrstart+1:yrend]
           
           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             prev="0123456789z"
             curr=""
             d=[]
             
             #cmd="select ts,watts from hour_res_watts where ts between '2011-02-01' and '2011-02-28'"
             cmd="select timestamp,watts from ml00_0_2011 where timestamp between '2011-02-01' and '2011-02-28' order by timestamp"
             for (ts,w)  in c.execute(cmd):  
                    curr=str(ts) 
                    if curr==prev:
                      print "WARNING: Duplicate timestamp detected:"+str(ts)
                    else:
                      d.append(w)
                    prev=curr

        
             conn.close()
             conn=None
           except Exception,e:
             print e



   print "len(d)="+str(len(d))


   coeffs=pywt.wavedec(d, 'db2', level=10)
   print "len(cA)="+str(len(coeffs[0]))
   
   #_d=approx(d,'db2',13)
   #print "len(_d)="+str(len(_d))

   #N=len(_d)
   #t=arange(0,1,1./N)
   #x=_d
   #plot(t,x)
   #ylim(0,max(_d))
   #savefig('db2_signal_onemonthspan_level13from3secres.pdf')
