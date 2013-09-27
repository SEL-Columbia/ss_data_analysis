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

    _am=[]
    _pm=[]

    _am2=[]
    _pm2=[]


    print "site="+site
    print "ckt="+ckt
    try:
      conn=sqlite3.connect("/home/apan2/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
      c=conn.cursor()
      print "obtained cursor"

      #### START EARLY morning 

      a1=" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%' or timestamp like '% 00:%'  )"

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+a1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))


      #####   END OF EARLY MORNING HISTOGRAM

      #### START LATE morning 

      a2=" and (timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  )"
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+a2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am2.append(0)
           else:
             _am2.append(math.log10(cnt))


      #####   END OF LATE MORNING HISTOGRAM

      ##  START of EARLY evening 
      p1=" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  )"


      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+p1
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      ##  START of LATE evening 
      p2=" and (timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"


      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+p2
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm2.append(0)
           else:
             _pm2.append(math.log10(cnt))

      c.close()
      conn.close()
      alphab = ['0-1', '1-5', '5-10', '10-20', '20-30', '30-40','40-60','60-80','80-1h','1h-2h','2h-4h','4h-8h','>8h']

      frecuencies = _am

      pos = np.arange(len(alphab))

      width = 1.0     # gives histogram aspect to the bar diagram

      ax = plt.axes()
      ax.set_xticks(pos + (width / 2))

      ax.set_xticklabels(alphab)
      ax.set_xlabel('range of watts(h=hundred)')
      ax.set_ylabel('log(count)')

      p1=plt.bar(pos, _am, width, color='r')

      p2=plt.bar(pos, _am2, width, color='b')

      p3=plt.bar(pos, _pm, width, color='y')

      p4=plt.bar(pos, _pm2, width, color='g')
 
      plt.legend( (p1[0], p2[0],p3[0],p4[0]), ('early AM', 'late AM', 'early PM' ,'late PM') )
      #plt.show()
      plt.savefig("/root/BAR_stacked"+site+"circuit"+ckt+"_2012.pdf")


    except Exception,e:
      print "Exception caught!!!"
      print e

