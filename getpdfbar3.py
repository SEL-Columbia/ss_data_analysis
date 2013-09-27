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
    for basedir in ["apan","apan2"]: 
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
      alphab = ['0w-1w', '1w-5w', '5w-10w', '10w-20w', '20w-30w', '30w-40w','40w-60w','60w-80w','80w-100w','100w-200w','200w-400w','400w-800w','>800w']

      fig=plt.figure()
      ax=fig.add_subplot(111)
      N=len(alphab)
      ind=np.arange(N)
      width=0.15

      # the bars:                                                                                                              
      rects1=ax.bar(ind,_am,width,color='red')
      rects2=ax.bar(ind+width,_am2,width,color='green')
      # axes and labels                                                                                                        
      rects3=ax.bar(ind+2*width,_pm,width,color='yellow')
      rects4=ax.bar(ind+3*width,_pm2,width,color='blue')
      # axes and labels                                                                                                        
      ax.set_xlim(-width,len(ind)+width)
      ax.set_ylim(0,8)
      ax.set_ylabel('log(count)')
      ax.set_xlabel('range of watts')
      ax.set_title("Power Histogram site "+site+" ckt "+ckt+ " year 2012")

      xTickMarks=alphab
      ax.set_xticks(ind+width)

      xtickNames=ax.set_xticklabels(xTickMarks)
      plt.setp(xtickNames,rotation=60,fontsize=10)
      ax.legend( (rects1[0],rects2[0] ,rects3[0],rects4[0] ),('early AM:00-06','late AM:06-12','early PM:12-18','late PM:18-24'  ) )
      plt.subplots_adjust(bottom=0.2)
      plt.savefig("/root/ts/timeseries/timeseries/static/images/barplot"+site+"circuit"+ckt+"_2012.png")


  except Exception,e:
      print "Exception caught!!!"
      print e

