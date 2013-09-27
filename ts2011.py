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
   tsList=[]

   print "before sqlite connect"
   conn=sqlite3.connect('/home/apan/ml00_0_2011.db')
   print "after sqlite connect"
   c=conn.cursor()
   print "successfully obtained cursor"

   # start of creating resolution tables
   prev="0123456789z"
   curr=""


   print "before creating watts dict"
   for (ts,w,i)  in c.execute("select timestamp,watthourssc20,id from ml00_0_2011  order by timestamp "):
     if tsList.__contains__(ts):
        print "timestamp has already been seen before for :"+str(ts)
     else:    
        print "timestamp is NEW creating watts dict"
        curr=str(ts) 
        #if curr[0:10]!=prev[0:10]:# daily wattage resolution
        #if curr[0:13]!=prev[0:13]:# hourly wattage resolution 
        if curr[0:16]!=prev[0:16]:# minutes wattage resolution
        #  modts="2011"+ ts[4:]
          d.append((ts,w,i,))
          tsList.append(ts)
        prev=curr

   print "before inserts"

   c.executemany('insert into minute_resolution_cdf values(?,?)',d)
   conn.commit()
       

   #print d 
   print "length(d)=" 
   print len(d)
   print "query/inserts has finished"   



if __name__=="__main__":
   range_query()    

