import time
import subprocess




if __name__=="__main__":

    try:     


         while True:

             print "refreshDbCalculations before calling stored proc"

             subprocess.Popen('psql productiongw2 -c "select calculateCircuitAnalytics()"',shell=True )

             print "refreshDbCalculations AFTER calling stored proc"
         

             time.sleep(1200)
             print "refreshDbCalculations after sleep==> recompute database analytics"

    except Exception,e:
         print "exception caught"
         print e
