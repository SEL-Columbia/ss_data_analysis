import os
import datetime
import time
import sqlite3




if __name__=="__main__":
   for (dirpath,dirnames,filenames) in os.walk('/home/apan/sqllite'):

      print "*************************************************************************************************"
      for _file in filenames:

         if _file.find("_ts.db")==-1:

           print dirpath+"/"+_file
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
         
             cmd="select timestamp,credit from "+_table+" order by timestamp"
             for (ts,w)  in c.execute(cmd):  
                    curr=str(ts) 
                    if curr==prev:
                      print "WARNING: Duplicate timestamp detected:"+str(ts)
                    #if curr[0:10]!=prev[0:10]:# daily wattage resolution
                    if curr[0:13]!=prev[0:13]:# hourly wattage resolution 
                    #if curr[0:16]!=prev[0:16]:# minutes wattage resolution
                         d.append((ts,w,))
                    prev=curr

          
             c.executemany('insert into hour_res_credit values(?,?)',d)

        

             conn.commit()


 
             print "finished sql cmd on "+_table
        
             conn.close()
             conn=None
           except Exception,e:
             print e


      print "*************************************************************************************************"
