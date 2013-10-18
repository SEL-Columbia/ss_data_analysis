import os
import datetime
import csv

def add_site_ip_to_csv_file(logfile, site, ip):
    outfile = logfile.replace(".log", ".csv")
    with open(logfile,'r') as csvinput:
        with open(outfile, 'w') as csvoutput:
    
            first_line = csvinput.readline()
            # Simple check for properly formatted file (NOTE:  MAINS files will not have a credit field at the end)
            if (first_line.startswith("Time Stamp,Watts,Volts,Amps,Watt Hours SC20,Watt Hours Today,Max Watts,Max Volts,Max Amps,Min Watts,Min Volts,Min Amps,Power Factor,Power Cycle,Frequency,Volt Amps,Relay Not Closed,Send Rate,Machine ID,Type")):
		# reset read ptr
		csvinput.seek(0)
                reader = csv.reader(csvinput)
                writer = csv.writer(csvoutput, lineterminator='\n')
                all = []
                row = next(reader)
                row.insert(0, 'line')
                row.insert(1, 'site')
                row.insert(2, 'ip')
                all.append(row)
        
		line_num = 0
                for row in reader:
                    row.insert(0, line_num)
                    row.insert(1, site)
                    row.insert(2, ip)
                    all.append(row)
                    line_num = line_num + 1
        
                writer.writerows(all)
                line_num = 0

            else:
		print "Empty or corrupted file: %s" % logfile
    

def add_site_ip_to_logs(logs_dir):

    for (dirpath,dirnames,filenames) in os.walk(logs_dir):
        for f in filenames:
            if f.endswith(".log"):
		dir_info = dirpath.split("/")
		site = dir_info[len(dir_info) - 5] # get the site from the dir
		ip = f[0:f.find(".")] # get the ip from the filename
		full_filename = os.path.join(dirpath, f)
		add_site_ip_to_csv_file(full_filename, site, ip)
 


if __name__=="__main__":

    import sys
    assert len(sys.argv) == 2, \
	"Usage: python cat_site_to_csv.py logs_dir"
    logs_dir = sys.argv[1]
    add_site_ip_to_logs(logs_dir) 
