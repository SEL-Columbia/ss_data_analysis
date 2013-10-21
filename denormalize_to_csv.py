import os
import datetime
import csv

"""
denormalize_to_csv.py

usage:  python denormalize_to_csv.py logs_dir

description:  Script to take a directory of sharedsolar log files
              in csv format and denormalizes them such that they
              can be concatenated together into "one big table"
              of the same structure without losing any information
              (while duplicating some...hence "denormalize")
"""

def write_denormalized_csv(logfile, site, ip):
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
                row.insert(0, 'line_num')
                row.insert(1, 'site_id')
                row.insert(2, 'ip_addr')
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
    

def denormalize_to_csv(logs_dir):

    for (dirpath,dirnames,filenames) in os.walk(logs_dir):
        for f in filenames:
            if f.endswith(".log"):
		dir_info = dirpath.split("/")
                # Note:  dir_info contents are blah/Site/YYYY/MM/DD/HH
		site = dir_info[-5] # get the site from the dir (site is always 5 dirs up in the path)
		ip = f[0:f.find(".")] # get the ip from the filename
		full_filename = os.path.join(dirpath, f)
		write_denormalized_csv(full_filename, site, ip)
 


if __name__=="__main__":
    import sys
    assert len(sys.argv) == 2, \
	"Usage: python denormalize_to_csv.py logs_dir"
    logs_dir = sys.argv[1]
    denormalize_to_csv(logs_dir) 
