import os
import datetime
import time
import sqlite3

def calcWH(cdf,tsArr):
    if len(cdf)==0:
       return 0
    _sum=0
  
    resetCount=0
    for i in range(len(cdf)):
       if i>=1:
         if cdf[i]< cdf[i-1]:
            resetCount=resetCount+1
       
         if cdf[i]>cdf[i-1]:
            _sum=_sum+(cdf[i]-cdf[i-1])

         if (cdf[i]<cdf[i-1]) :## reset                                                        
            _sum=_sum+cdf[i]


  
    if resetCount>=2:
       print "multiple resets detected in one hour interval . resetCount="+str(resetCount)
       _count=0
       for i in range(len(cdf)):
          if cdf[i]<cdf[i-1]:
             if cdf[i]>=0.9*cdf[i-1]:
                continue
             else:
                oscillation=False
                _count=_count+1

       if oscillation==False and _count>=2:
          print "SEVERAL TRUE RESETS" ## 67 occurences

       if oscillation==False and _count==1:
          print "SINGLE TRUE RESET but many dips" # 2 occurences


       if oscillation==False and _count==0:
          print "ZERO TRUE RESET but many dips"


       if oscillation==False:
          print "NON OSCILLATING RESETS "##### this happens 69 times up to Dec 2012 sd card data
       if oscillation==True:
          print "MULTIPLE RESETS BUT OSCILLATING"# zero occurences

       return -1
    else:
       if resetCount==1:
          for i in range(len(cdf)):
            if cdf[i]<cdf[i-1] and cdf[i]>=0.9*cdf[i-1]:
               print "SINGLE RESET OSCILLATION DETECTED"+"cdf[i]="+str(cdf[i])+"cdf[i-1]="+str(cdf[i-1])#217 occurences
               break
       return _sum


if __name__=="__main__":

  for baseDir in ["apan","apan2"]:

  #for baseDir in ["apan3"]:

    for (dirpath,dirnames,filenames) in os.walk("/home/"+baseDir+"/sqllite"):

      #print "*************************************************************************************************"
      for _file in filenames:

         if _file.find("_ts.db")==-1:
 
           print dirpath+"/"+_file
           cktstart=_file.find("_")
           cktend=_file.rfind("_")
           site=_file[0:cktstart]
           #print "site="+site
           ckt=_file[cktstart+1:cktend]
           #print "circuit="+ckt
           tableend=_file.rfind(".db")
           _table=_file[0:tableend]
           print "************************************* START TO READ********************************************"
           print "tablename="+_table
           yrstart=_file.rfind("_")
           yrend=_file.rfind(".")
           yr=_file[yrstart+1:yrend]
           #print "year"+str(yr)

           if site=="ug01" or site=="ug02" or site == "ug03" or site=="ug04" or site=="ug05" or site=="ug06" or site == "ug07" or site=="ug08":
             print "Updating correction sites......"

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
                      if  prev!="YYYY-mm-dd HH:MM:SS" and  curr[0:13]!=prev[0:13]:
                          print "hour boundary"
                      else:
                          v[ts]=w
  
                    if  prev!="YYYY-mm-dd HH:MM:SS" and  curr[0:13]!=prev[0:13]:# 
                         cdf=[]
                         tsArr=[]
                         for ts in sorted(v.keys()):
                            cdf.append(v[ts])
                            tsArr.append(ts)
                         totalwh=calcWH(cdf,tsArr)
                         v={}
                         v[ts]=w
                         if totalwh!=-1 :
                           d.append((prev[0:13]+":00:00",totalwh,))


                    prev=curr


               ##c.executemany('insert into hour_res_watthours values(?,?)',d)
               ##conn.commit()


               print "******************************************************************************************"
               print "finished successfully sql cmd on "+_table
             
               print "******************************************************************************************"
               conn.close()
               conn=None
             except Exception,e:
               print e


      #print "*************************************************************************************************"
