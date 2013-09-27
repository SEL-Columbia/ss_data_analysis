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
    if len(li)!= 4:
      print "Invalid syntax"
      print "Usage: python getpdf.py <site> <circuit> <year>"
      print "example:  python getpdf.py ug03 0 2012"
      exit(0)

    site=sys.argv[1]
    ckt= sys.argv[2]
    year=sys.argv[3]

    _d=[]


    print "site="+site
    print "ckt="+ckt
    try:
      conn=sqlite3.connect("/home/apan2/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
      c=conn.cursor()
      print "obtained cursor"

      #for i in range(35):
      #  cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str((i*30))+ " and "+ str(((i+1)*30))
      #  for (cnt,) in c.execute(cmd):
      #     print "count="+str(cnt)+"  SQL="+ str(cmd)
      #     _d.append(cnt)

      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           _d.append(cnt)
      
      #cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "
      #for (cnt,) in c.execute(cmd):
      #     print "count="+str(cnt)+"  SQL="+ str(cmd)
      #     _d.append(cnt)
      
      c.close()
      conn.close()
      alphab = ['0-1', '1-5', '5-10', '10-20', '20-30', '30-40','40-60','60-80','80-1h','1h-2h','2h-4h','4h-8h']
      #frecuencies = [23, 44, 12, 11, 2, 10]
      frecuencies = _d

      pos = np.arange(len(alphab))

      width = 1.0     # gives histogram aspect to the bar diagram

      #width = 4.0     # gives histogram aspect to the bar diagram

      ax = plt.axes()
      ax.set_xticks(pos + (width / 2))
#      ax.set_xticks( [2,6,10,14,18,22, 26,30,34,38,42,46]  )
      ax.set_xticklabels(alphab)

      plt.bar(pos, frecuencies, width, color='r')
      #plt.show()
      plt.savefig("/root/BAR"+site+"circuit"+ckt+"_2012.pdf")


    except Exception,e:
      print "Exception caught!!!"
      print e

