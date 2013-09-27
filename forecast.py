import matplotlib
matplotlib.use('Agg')



import os
import datetime
import time
import sqlite3
import pywt
from pylab import *
import fnmatch, gzip, os,  re, sys, time
import numpy as np
import pybrain



def approx(x, wavelet, level):
    ca = pywt.wavedec(x, wavelet, level=level)
    ca = ca[0]
    return pywt.upcoef('a', ca, wavelet, level, take=len(x))  
dirpath=''
_file=''  
d=[]
_d=[]

wk=[]
coeffs=[]

def getSignal(startdate,enddate):
           
    try:
       conn=sqlite3.connect(dirpath+"/"+_file)
       c=conn.cursor()



       prev="0123456789z"
       curr=""
      
                         
       #cmd="select timestamp,watts from ml00_0_2011 where timestamp between '2011-01-01' and '2011-01-07' order by timestamp"
       #startdate="2011-01-01"
       #enddate="2011-01-07"
       cmd="select timestamp,watts from ml00_0_2011 where timestamp between '"+startdate+"' and '"+enddate+"' order by timestamp"
       for (ts,w)  in c.execute(cmd):  
           curr=str(ts) 
           if curr==prev:
              print "WARNING: Duplicate timestamp detected:"+str(ts)
           else:
              d.append(w)
              prev=curr
       print "len(data)="+str(len(d))
       print "len(data)/32="+str(ceil(len(d)/32))
       coeffs=pywt.wavedec(d, 'db2', mode='per',level=5)
       print "len(cA)="+str(len(coeffs[0]))
       print "len(cD5)="+str(len(coeffs[1]))
       print "len(cD4)="+str(len(coeffs[2]))
       print "len(cD3)="+str(len(coeffs[3]))
       print "len(cD2)="+str(len(coeffs[4]))
       print "len(cD1)="+str(len(coeffs[5]))
       wk.append(coeffs[0])
      

       conn.close()
       conn=None
    except Exception,e:
             print e


if __name__=="__main__":

    dirpath='/home/apan/sqllite/ml00'           
    _file="ml00_0_2011.db"
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


    getSignal("2011-01-01","2011-01-07")
    getSignal("2011-01-08","2011-01-14")
    print "++++++++++++++++++++++++++++ wk[] start +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"


    print wk
    print "++++++++++++++++++++++++++++ wk[] end   +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    #print "len(d)="+str(len(d))


    #coeffs=pywt.wavedec(d, 'db2', level=5)
    #print "len(cA)="+str(len(coeffs[0]))
   
    #_d=approx(d,'db2',5)
    #print "len(_d)="+str(len(_d))

    #N=len(_d)
    #t=arange(0,1,1./N)
    #x=_d
    #plot(t,x)
    #ylim(0,max(_d))  
    #savefig('/home/alp4/plots/db2_oneweek_level5from3secres.pdf')

    #ds=SupervisedDataSet(1,1)
    #indata=wk[0]
    #outdata=wk[1]
    #ds.addSample(indata,outdata)

    #n = buildNetwork(ds.indim,8,8,ds.outdim,recurrent=True)
    #t = BackpropTrainer(n,learningrate=0.01,momentum=0.5,verbose=True)
    #t.trainOnDataset(ds,1000)
    #t.testOnData(verbose=True)


    #import pybrain.datasets.supervised as s
    #ds=s.SupervisedDataSet(2,1)
    #ds.setVectorFormat('list')
    #ds.addSample((0,0), (0,))
    #ds.addSample((0,1), (1,))
    #ds.addSample((1,0), (1,))
    #ds.addSample((1,1), (0,))




    #from pybrain.tools.shortcuts import buildNetwork
    # net     = buildNetwork(2, 3, 1, bias=True)
    #from pybrain.supervised.trainers import BackpropTrainer
