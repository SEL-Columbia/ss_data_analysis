import os
import datetime
import time
import sqlite3


   

if __name__=="__main__":
   for (dirpath,dirnames,filenames) in os.walk('/home/apan2/sqllite'):

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
           #if yr == "2011":
           #  suffix=""
           #if yr == "2012":
           #  suffix="2"
           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()
             cmd="create index tsidx on "+_table+"(timestamp)"
             c.execute(cmd)
             conn.commit()


             cmd="CREATE TABLE daily_res_credit"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)
             conn.commit()

             cmd="CREATE TABLE hour_res_credit"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)
             conn.commit()


             cmd="CREATE TABLE minute_res_credit"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)  
             conn.commit()



             cmd="CREATE TABLE daily_res_watts"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)
             conn.commit()

             cmd="CREATE TABLE hour_res_watts"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)
             conn.commit()


             cmd="CREATE TABLE minute_res_watts"+"(ts timestamp primary key,watts float)"
             c.execute(cmd)  
             conn.commit()

#############################################################
             #cmd="DROP TABLE daily_res"+suffix+"_credit"
             #c.execute(cmd)
             #conn.commit()
 
             #cmd="DROP TABLE hour_res"+suffix+"_credit"
             #c.execute(cmd)
             #conn.commit()


             #cmd="DROP TABLE minute_res"+suffix+"_credit"
             #c.execute(cmd)
             #conn.commit()


 
             print "finished creating index and creating resolution tables  on "+_table
        
             conn.close()
             conn=None
           except Exception,e:
             print e


      print "*************************************************************************************************"
