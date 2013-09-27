import datetime
import time
import sqlite3

def print_timing(func):
   def wrapper(*arg):
     t1=time.time()
     res=func(*arg)
     t2=time.time()
     print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
     return res
   return wrapper




@print_timing
def range_query():
   d=[]
   print "before sqlite connect"
   conn=sqlite3.connect('/home/apan/ml00_0_2011.db')
   print "aftewr sqlite connect"
   c=conn.cursor()
   print "successfully obtained cursor"
   for (ts,w)  in c.execute("select * from daily_resolution"):
       d.append((ts,w,))

   print d
   print "length(d)="
   print len(d)
   print "query has finished"


if __name__=="__main__":
   range_query()

