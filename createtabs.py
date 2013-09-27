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

             cmd="CREATE TABLE delta_hourlywatthours"+"(ts timestamp primary key,wh float)"
             c.execute(cmd)
             conn.commit()

 
             print "apan finished sqlcommands on "+_table
        
             conn.close()
             conn=None
           except Exception,e:
             print e


      print "*************************************************************************************************"

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

           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             cmd="CREATE TABLE delta_hourlywatthours"+"(ts timestamp primary key,wh float)"
             c.execute(cmd)
             conn.commit()
 

 
             print "apan2:  finished sqlcommands on "+_table
        
             conn.close()
             conn=None
           except Exception,e:
             print e
    

      print "*************************************************************************************************"
