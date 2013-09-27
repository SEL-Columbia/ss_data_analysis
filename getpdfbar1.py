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


    print "site="+site
    print "ckt="+ckt
    try:
      conn=sqlite3.connect("/home/apan2/sqllite/"+site+"/"+site+"_"+ckt+"_"+year+".db")
      c=conn.cursor()
      print "obtained cursor"
      #### START morning 
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+" and (timestamp like '% 01:%' or timestamp like '% 02%' or timestamp like '% 03:%'  or timestamp like '% 04:%'  or timestamp like '% 05:%' or timestamp like '% 06:%'  or timestamp like '% 07:%'  or timestamp like '% 08:%'  or timestamp like '% 09:%'  or timestamp like '% 10:%'  or timestamp like '% 11:%'  or timestamp like '% 00:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _am.append(0)
           else:
             _am.append(math.log10(cnt))


      #####   END OF MORNING HISTOGRAM
      ##  START of evening 

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(0)+ " and "+ str(1)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(1)+ " and "+ str(5)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))
      
      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(5)+ " and "+ str(10)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(10)+ " and "+ str(20)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(20)+ " and "+ str(30)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(30)+ " and "+ str(40)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(40)+ " and "+ str(60)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(60)+ " and "+ str(80)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(80)+ " and "+ str(100)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(100)+ " and "+ str(200)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(200)+ " and "+ str(400)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts between "+ str(400)+ " and "+ str(800)+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      cmd="select count(*) from "+site+"_"+ckt+"_2012"+ " where watts > 800 "+" and (timestamp like '% 12:%' or timestamp like '% 13%' or timestamp like '% 14:%'  or timestamp like '% 15:%'  or timestamp like '% 16:%' or timestamp like '% 17:%'  or timestamp like '% 18:%'  or timestamp like '% 19:%'  or timestamp like '% 20:%'  or timestamp like '% 21:%'  or timestamp like '% 22:%'  or timestamp like '% 23:%'  )"
      for (cnt,) in c.execute(cmd):
           print "count="+str(cnt)+"  SQL="+ str(cmd)
           if cnt<=1:
             _pm.append(0)
           else:
             _pm.append(math.log10(cnt))

      c.close()
      conn.close()

      alphab = ['0-1', '1-5', '5-10', '10-20', '20-30', '30-40','40-60','60-80','80-100','100-200','200-400','400-800','>800']
      #frecuencies = _am
      #pos = np.arange(len(alphab))
      #width = 1.0  
      #ax = plt.axes()
      #ax.set_xticks(pos + (width / 2))
      #ax.set_xticklabels(alphab)
      #ax.set_xlabel('range of watts(h=hundred)')
      #ax.set_ylabel('log(count)')
      #p1=plt.bar(pos, _am, width, color='r')
      #p2=plt.bar(pos, _pm, width, color='y',bottom=_am)
      #plt.legend( (p1[0], p2[0]), ('AM', 'PM') )
      

      fig=plt.figure()
      ax=fig.add_subplot(111)
      N=len(alphab)
      ind=np.arange(N)
      width=0.15

      # the bars:
      rects1=ax.bar(ind,_am,width,color='red')
      rects2=ax.bar(ind+width,_pm,width,color='green')
      # axes and labels
      ax.set_xlim(-width,len(ind)+width)
      ax.set_ylim(0,10)
      ax.set_ylabel('log(count)')
      ax.set_xlabel('range of watts')
      ax.set_title("Power Histogram site "+site+" ckt "+ckt+ "year 2012")

      xTickMarks=alphab
      ax.set_xticks(ind+width)

      xtickNames=ax.set_xticklabels(xTickMarks)
      plt.setp(xtickNames,rotation=45,fontsize=10)
      ax.legend( (rects1[0],rects2[0]),('AM','PM') )

      plt.savefig("/root/barplot"+site+"circuit"+ckt+"_2012.pdf")


    except Exception,e:
      print "Exception caught!!!"
      print e

