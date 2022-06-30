import sys
import os

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


if (len(sys.argv) != 2) :
    print bcolors.WARNING + 'Usage:'
    print 'workload file' + bcolors.ENDC

config_file = sys.argv[1]

args = []
f_config = open (config_file, 'r')
for line in f_config :
    args.append(line[:-1])

ycsb_dir = 'YCSB/'
workload_dir = 'workload_spec/'
output_dir='workloads/'

workload = args[0]
key_type = args[1]

print bcolors.OKGREEN + 'workload = ' + workload
print 'key type = ' + key_type + bcolors.ENDC

ycsb_load = output_dir + 'Rycsb_load_' + key_type + '_' + workload
ycsb_run = output_dir + 'Rycsb_run_' + key_type + '_' + workload
load = output_dir +  'ycsb_load_' + workload
run = output_dir + 'ycsb_run_' + workload

cmd_ycsb_load = ycsb_dir + 'bin/ycsb.sh load basic -P ' + workload_dir + workload + ' -s > ' + ycsb_load
cmd_ycsb_run = ycsb_dir + 'bin/ycsb.sh run basic -P ' + workload_dir + workload + ' -s > ' + ycsb_run

print(cmd_ycsb_load)
print(cmd_ycsb_run)
os.system(cmd_ycsb_load)
os.system(cmd_ycsb_run)
#####################################################################################



f_load = open (ycsb_load, 'r')
f_load_out = open (load, 'w')
for line in f_load :
    cols = line.split(' ')
    if len(cols) > 0 and (cols[0] == "INSERT"):   
        value = line.split('[',1)
        value=value[1][8:-3]       
        # for r in value:
        #     if ord(r)<33 or ord(r)>126:
        #         value=value.replace(r,'.')
        f_load_out.write (cols[0] + " " + cols[2][4:] +" "+str(len(value))+ "\n")
f_load.close()
f_load_out.close()


f_run = open (ycsb_run, 'r')
f_run_out = open (run, 'w')
for line in f_run :
    cols = line.split()
    if len(cols) > 0 and (cols[0] == "INSERT" or cols[0] == "READ" or cols[0] == "UPDATE"):
        if cols[0] == "READ":
            f_run_out.write (cols[0] + " " + cols[2][4:] + "\n")
        elif cols[0] == "INSERT" or cols[0]=="UPDATE":
            value = line.split('[',1)
            value=value[1][8:-3]
            # for r in value:
            #     if ord(r)<33 or ord(r)>126:
            #         value=value.replace(r,'.')
            f_run_out.write (cols[0] + " " + cols[2][4:] +" "+str(len(value))+ "\n")
f_run.close()
f_run_out.close()

# cmd = 'rm -f ' + ycsb_load
# os.system(cmd)
# cmd = 'rm -f ' + ycsb_run
# os.system(cmd)
