import subprocess
import psycopg2


def print_timing(func):
   def wrapper(*arg):
     t1=time.time()
     res=func(*arg)
     t2=time.time()
     print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
     return res
   return wrapper



if __name__ == '__main__':
   nextline=""
   site=""
   try:
     process=subprocess.Popen('find /home/alp4/raw_sd_card/ -type f -name "*.log"',shell=True,
     stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

  
     conn=psycopg2.connect("dbname='timeseries' user='postgres' host='localhost' password='postgres'")
     cur=conn.cursor()


     while True:
       nextline=process.stdout.readline()
       nextline=nextline.strip()

       print "***filename start***"
       print nextline
       print "***filename end  ***"

       acctstart=nextline.rindex("/")

       acctstart=acctstart+1
       acctend=nextline.rindex(".")

       account=nextline[acctstart:acctend]
       account=account.replace("_",".")
       account=str(account)

       s=nextline
       start=s.rfind("raw_sd_card")+12
       s=s[start:]
       end=s.find("/")
       s=s[:end]
       site=str(s)
       

       log=open(nextline)

       for line in log.readlines():
         
         line=line.strip()
         li=line.split(",")
         
#         print "inserting tuple"+str(li)
         if li[0]=="Time Stamp":
           print "skipping record..."
           continue
         datestr=li[0]
         yyyy=str(datestr[0:4])
         
         mm=str(datestr[4:6])
         
         dd=str(datestr[6:8])
         
         hh=str(datestr[8:10])
         _mm=str(datestr[10:12])
         ss=str(datestr[12:14])
         TS = yyyy+"-"+mm+"-"+dd+" "+hh+":"+_mm+":"+ss
         print "TS="+TS
         #cur.execute("select count(*) from meterevents where timestamp=%s and site=%s and account=%s",(TS,site,account,))
         #(cnt,)=cur.fetchone()
         #if cnt==0:
         if len(li)==21:
             print "inserting...."
             print (TS,site,account,li[1],li[2],li[3],li[4],li[5],li[6],li[7],li[8],li[9],li[10],li[11],li[12],li[13],li[14],li[15],li[16],li[17],li[18],li[19],li[20]    )
             cur.execute("insert into meterevents values(%s,%s,%s,%s,%s,  %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,  %s,%s,%s,%s,%s, %s,%s,%s)",
               (TS,site,account,li[1],li[2],li[3],li[4],li[5],li[6],li[7],li[8],li[9],li[10],li[11],li[12],li[13],li[14],li[15],li[16],li[17],li[18],li[19],li[20],    ))
             conn.commit()
         if len(li)==20:
             print "inserting...."
             print (TS,site,account,li[1],li[2],li[3],li[4],li[5],li[6],li[7],li[8],li[9],li[10],li[11],li[12],li[13],li[14],li[15],li[16],li[17],li[18],li[19],0.0,    )
             cur.execute("insert into meterevents values(%s,%s,%s,%s,%s,  %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,  %s,%s,%s,%s,%s, %s,%s,%s)",
               (TS,site,account,li[1],li[2],li[3],li[4],li[5],li[6],li[7],li[8],li[9],li[10],li[11],li[12],li[13],li[14],li[15],li[16],li[17],li[18],li[19],0.0,    ))
             conn.commit()
         #else:
         #   print "===============================  DUPLICATE RECORD FOUND !!! +++++++++++++++++++++++++++++++++++++++++++++++++++"


#         print "length="+str(len(li))

   except Exception,e:
       print "exception caught"
       print e
       print "current file="+str(nextline)
   
   finally:
          if cur is not None:
             cur.close()
          if conn is not None:
             conn.close()
