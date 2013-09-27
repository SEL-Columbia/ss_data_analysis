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
           if yr == "2011":
             suffix=""
           if yr == "2012":
             suffix="2"
           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             prev="0123456789z"
             curr=""
             d=[]
             tsList=[]
             cmd="select timestamp,watts from "+_table+" order by timestamp"
             for (ts,w)  in c.execute(cmd):  

                 if tsList.__contains__(ts):
                    print "timestamp has already been seen before for :"+str(ts)
                 else:    
                    curr=str(ts) 
                    #if curr[0:10]!=prev[0:10]:# daily wattage resolution
                    if curr[0:13]!=prev[0:13]:# hourly wattage resolution 
                    #if curr[0:16]!=prev[0:16]:# minutes wattage resolution
                       if yr=="2012":
                          ts="2011"+ ts[4:]
                       d.append((ts,w,))
                       tsList.append(ts)
                    prev=curr

             if yr=="2011":
                c.executemany('insert into hour_resolution values(?,?)',d)

             if yr=="2012":
                c.executemany('insert into hour_resolution2 values(?,?)',d)

             conn.commit()


 
             print "finished sql cmd on "+_table
        
             conn.close()
             conn=None
           except Exception,e:
             print e


      print "*************************************************************************************************"
