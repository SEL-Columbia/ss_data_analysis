import os
import datetime
import csv

def add_site_ip_to_csv_file(logfile, site, ip):
    outfile = logfile.replace(".log", ".csv")
    with open(logfile,'r') as csvinput:
        with open(outfile, 'w') as csvoutput:
            writer = csv.writer(csvoutput, lineterminator='\n')
            reader = csv.reader(csvinput)
    
            all = []
            row = next(reader)
            row.insert(0, 'site')
            row.insert(1, 'ip')
            all.append(row)
    
            for row in reader:
                row.insert(0, site)
                row.insert(1, ip)
                all.append(row)
    
            writer.writerows(all)


def add_site_ip_to_logs(logs_dir):

    for (dirpath,dirnames,filenames) in os.walk(logs_dir):
        for f in filenames:
            if f.endswith(".log"):
		dir_info = dirpath.split("/")
		site = dir_info[len(dir_info) - 5] # get the site from the dir
		ip = f[0:f.find(".")] # get the ip from the filename
		add_site_ip_to_csv_file(os.path.join(dirpath, f), site, ip)
 


if __name__=="__main__":

    import sys
    assert len(sys.argv) == 2, \
	"Usage: python cat_site_to_csv.py logs_dir"
    logs_dir = sys.argv[1]
    add_site_ip_to_logs(logs_dir) 
