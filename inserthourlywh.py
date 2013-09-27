import os
import datetime
import time
import sqlite3

def calcWH(cdf,tsArr,_table):
    if len(cdf)==0:
       return 0
    _sum=0
    _start=cdf[0]
    _startTS=tsArr[0]
    for i in range(len(cdf)):
       if i>=1:
         #print "sum so far="+str(_sum)                                                                               
         if cdf[i]>_start:
            _sum=_sum+(cdf[i]-_start)
            _start=cdf[i]
            _startTS=tsArr[i]
         
         if (cdf[i]<_start) and (cdf[i]>(0.8*_start)):## oscillation                                                  

            print "***************  OSCILLATION  DETECTED IN WH CDF ********************************************"
            print "table containing oscillation="+_table
            print "_start="+str(_start)
            print "_startTS="+str(_startTS)
            print "cdf[i]="+str(cdf[i])
            print "tsArr[i]="+str(tsArr[i])
            
            #_start=_start                                                                                            
            #_sum=_sum                                                                                                
            continue
         if (cdf[i]<_start) and (cdf[i]<(0.2*_start)):## reset                                                        
            _sum=_sum+cdf[i]
            _start=cdf[i]
            _startTS=tsArr[i]
         if (cdf[i]<_start) and (cdf[i]>(0.2*_start)) and (cdf[i]<(0.8*_start))  :#                                   

            #print "***************  COULD NOT INTERPRET WATT HOURS DATA ********************************************"
            #print "_start="+str(_start)
            #print "_startTS="+str(_startTS)
            #print "cdf[i]="+str(cdf[i])
            #print "tsArr[i]="+str(tsArr[i])

            _sum=_sum+cdf[i]                                                                                         
            _start=cdf[i]                                                                                            
            _startTS=tsArr[i]                                                                                        

            #return -666
    return _sum

  

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
           print "year"+str(yr)


           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             #prev="0123456789z"
             prev="YYYY-mm-dd HH:MM:SS"

             curr=""
             d=[]
             v={}
             cdf=[]
             tsArr=[]

             cmd="select timestamp,watthourssc20 from "+_table+" order by timestamp"
             for (ts,w)  in c.execute(cmd):  
                    curr=str(ts) 
                    if curr==prev:
                      print "WARNING: Duplicate timestamp detected:"+str(ts)
                    else:
                      v[ts]=w

                    if  prev!="YYYY-mm-dd HH:MM:SS" and  curr[0:13]!=prev[0:13]:# hourly wattage resolution
                         cdf=[]
                         tsArr=[]
                         for ts in sorted(v.keys()):
                            cdf.append(v[ts])
                            tsArr.append(ts)
                         totalwh=calcWH(cdf,tsArr,_table)
                         v={}
                         d.append((prev[0:13]+":00:00",totalwh,))
                    prev=curr


             ##c.executemany('insert into hour_res_watthours values(?,?)',d)

        

             ##conn.commit()


             print "******************************************************************************************"
             print "finished sql cmd on "+_table
             print d
             print "******************************************************************************************"
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
           print "year"+str(yr)


           try:
             conn=sqlite3.connect(dirpath+"/"+_file)
             c=conn.cursor()

             
             prev="YYYY-mm-dd HH:MM:SS"

             curr=""
             d=[]
             v={}
             cdf=[]
             tsArr=[]

             cmd="select timestamp,watthourssc20 from "+_table+" order by timestamp"
             for (ts,w)  in c.execute(cmd):  
                    curr=str(ts) 
                    if curr==prev:
                      print "WARNING: Duplicate timestamp detected:"+str(ts)
                    else:
                      v[ts]=w

                    if  prev!="YYYY-mm-dd HH:MM:SS" and  curr[0:13]!=prev[0:13]:# hourly wattage resolution
                         cdf=[]
                         tsArr=[]
                         for ts in sorted(v.keys()):
                            cdf.append(v[ts])
                            tsArr.append(ts)
                         totalwh=calcWH(cdf,tsArr,_table)
                         v={}
                         d.append((prev[0:13]+":00:00",totalwh,))
                    prev=curr


             #c.executemany('insert into hour_res_watthours values(?,?)',d)

        

             #conn.commit()


             print "******************************************************************************************"
             print "finished sql cmd on "+_table
             print d
             print "******************************************************************************************"
             conn.close()
             conn=None
           except Exception,e:
             print e


      print "*************************************************************************************************"
