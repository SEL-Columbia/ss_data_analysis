import os
import datetime
import time
import sqlite3

  

if __name__=="__main__":
   _e=None
   u={}
   v={}
   for (dirpath,dirnames,filenames) in os.walk('/home/apan/sqllite'):

      #print "*************************************************************************************************"
      for _file in filenames:

         if _file.find("_ts.db")==-1:
 
           #print dirpath+"/"+_file
           cktstart=_file.find("_")
           cktend=_file.rfind("_")
           site=_file[0:cktstart]
           #print "site="+site
           ckt=_file[cktstart+1:cktend]
           #print "circuit="+ckt
           tableend=_file.rfind(".db")
           _table=_file[0:tableend]
           #print "tablename="+_table
           yrstart=_file.rfind("_")
           yrend=_file.rfind(".")
           yr=_file[yrstart+1:yrend]
           #print "year"+str(yr)


           try:
            if site[0:2]=='ug':

             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             #prev="0123456789z"
             prev="YYYY-mm-dd HH:MM:SS"
 
             curr=""
             d=[]
             v={}
             cdf=[]
             tsArr=[]
             # for pmax                                                                            
             # cmd="select max(watts) from "+_table                                           
             #for emax     
             cmd="select max(wh) from minute_res_watthours2"
             for (w,)  in c.execute(cmd):
                #print "_table="+_table
                #print "pmax="+str(w)+"\n"
                u[str(_table)]=w


             conn.close()
             conn=None
           except Exception,e:
             #print e
             _e=e

      #print "*************************************************************************************************"

   for (dirpath,dirnames,filenames) in os.walk('/home/apan2/sqllite'):

      #print "*************************************************************************************************"
      for _file in filenames:

         if _file.find("_ts.db")==-1:
 
           #print dirpath+"/"+_file
           cktstart=_file.find("_")
           cktend=_file.rfind("_")
           site=_file[0:cktstart]
           #print "site="+site
           ckt=_file[cktstart+1:cktend]
           #print "circuit="+ckt
           tableend=_file.rfind(".db")
           _table=_file[0:tableend]
           #print "tablename="+_table
           yrstart=_file.rfind("_")
           yrend=_file.rfind(".")
           yr=_file[yrstart+1:yrend]
           #print "year"+str(yr)


           try:
            if site[0:2]=='ug':

             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             #prev="0123456789z"
             prev="YYYY-mm-dd HH:MM:SS"

             curr=""
             d=[]
             v={}
             cdf=[]
             tsArr=[]

             # for pmax
             # cmd="select max(watts) from "+_table
             #for emax
             cmd="select max(wh) from minute_res_watthours2"
             for (w,)  in c.execute(cmd):  
                #print "_table="+_table
                #print "pmax="+str(w)+"\n"
                try:
                  _x=u[str(_table)]
                  #key found in hash u
                  v[str(_table)]=max(w,_x)
                except Exception,e:
                  #key not found in hash u
                  v[str(_table)]=w
                print "site_circuit_year="+str(_table)
                print "EMAX="+str(v[str(_table)])

             conn.close()
             conn=None
           except Exception,e:
             #print e
             _e=e

      #print "*************************************************************************************************"
