#!/usr/bin/env python
""" Flink Bootstrap Script (based on Spark 1.1 release) """
import os, sys
import pdb
import urllib
import subprocess
import logging
import re
import uuid
import shutil
import time
import signal
import socket
import hostlist
import datetime
import json
from optparse import OptionParser
import pkg_resources


logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
VERSION="1.1.4"
FLINK_DOWNLOAD_URL = "http://mirrors.sonic.net/apache/flink/flink-%s/flink-%s-bin-hadoop27-scala_2.11.tgz" % (VERSION, VERSION)
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

flink_directory = ("-").join(os.path.basename(FLINK_DOWNLOAD_URL).split("-")[:2])
FLINK_HOME=os.path.join(os.getcwd(), "work/", flink_directory)
print "Flink Home: %s"%FLINK_HOME

FLINK_CONF_DIR=os.path.join(FLINK_HOME, "conf")

STOP=False


def handler(signum, frame):
    logging.debug("Signal catched. Stop Hadoop")
    global STOP
    STOP=True

    

class FlinkBootstrap(object):


    def __init__(self, working_directory, flink_home):
        self.working_directory=working_directory
        self.jobid = "flink-conf-"+str(uuid.uuid1())
        #self.job_working_directory = os.path.join(WORKING_DIRECTORY, self.jobid)
        self.job_working_directory=flink_home
        self.job_conf_dir = os.path.join(self.job_working_directory, "conf")
        self.master="localhost"


    ###################################################################################################### 
    @staticmethod
    def get_pbs_allocated_nodes():
        print "Init PBS"
        pbs_node_file = os.environ.get("PBS_NODEFILE")    
        if pbs_node_file == None:
            return ["localhost"]
        f = open(pbs_node_file)
        nodes = f.readlines()
        for i in nodes:
            i.strip()
        f.close()    
        return list(set(nodes))


    @staticmethod
    def get_sge_allocated_nodes():
        print "Init SGE"
        sge_node_file = os.environ.get("PE_HOSTFILE")    
        if sge_node_file == None:
            #return [socket.gethostname()]
            return [socket.gethostbyname(socket.gethostname())]
            #return ["localhost"]
        f = open(sge_node_file)
        sgenodes = f.readlines()
        f.close() 
        nodes = []
        for i in sgenodes:    
            columns = i.split()                
            try:
                for j in range(0, int(columns[1])):
                    print("add host: " + columns[0].strip())
                    nodes.append(columns[0]+"\n")
            except:
                    pass
        nodes.reverse()
        return list(set(nodes))

    @staticmethod
    def get_slurm_allocated_nodes():
        print("Init nodefile from SLURM_NODELIST")
        hosts = os.environ.get("SLURM_NODELIST") 
        if hosts == None:
            self.init_local()
            return

        print "***** Hosts: " + str(hosts) 
        hosts=hostlist.expand_hostlist(hosts)
        number_cpus_per_node = 1
        if os.environ.get("SLURM_CPUS_ON_NODE") != None:
            number_cpus_per_node=int(os.environ.get("SLURM_CPUS_ON_NODE"))
        freenodes = []
        for h in hosts:
            #for i in range(0, number_cpus_per_node):
            freenodes.append((h + "\n"))
        return list(set(freenodes))

    @staticmethod
    def get_nodelist_from_resourcemanager():
        if (os.environ.get("PBS_NODEFILE") != None and os.environ.get("PBS_NODEFILE") != ""):
            nodes = FlinkBootstrap.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST") != None):
            nodes = FlinkBootstrap.get_slurm_allocated_nodes()
        else:
            nodes = FlinkBootstrap.get_sge_allocated_nodes()
        return nodes


    def get_flink_conf_yaml(self, hostname):
        module = "flink.configs.default"
        print("Access config in module: " + module + " File: flink-conf.yaml")
        my_data = pkg_resources.resource_string(module, "flink-conf.yaml")
        my_data = my_data%(hostname)
        my_data = os.path.expandvars(my_data)
        return my_data



    #################################################################################################################
    def configure_flink(self):
        nodes = self.get_nodelist_from_resourcemanager()
        if nodes!=None:
            #master = socket.gethostname().split(".")[0]
            master = socket.gethostbyname(socket.gethostname())
            self.master=master
            master_file = open(os.path.join(self.job_conf_dir, "masters"), "w")
            master_file.write(master) 
            master_file.close()
            master_file2=open(os.path.join(WORKING_DIRECTORY, 'flink_master'), 'w')
            master_file2.write(master)
            master_file2.close()
            self.master=master

            flink_conf_yaml_file = open(os.path.join(self.job_conf_dir, "flink-conf.yaml"), "w")
            flink_conf_yaml_file.write(self.get_flink_conf_yaml(master))
            flink_conf_yaml_file.close()

            slave_file = open(os.path.join(self.job_conf_dir, "slaves"), "w")
            slave_file.writelines(nodes) 
            slave_file.close()
            logging.debug("Flink cluster nodes: " + str(nodes))


    def start_flink(self):
        logging.debug("Start Flink")
        self.set_env()
        start_command = os.path.join(FLINK_HOME, "bin/start-cluster.sh &")
        logging.debug("Execute with os: %s"%start_command)
        #os.system(". ~/.bashrc & " + start_command)
        os.system(start_command)
        #status = subprocess.call(start_command, shell=True)
        #status = subprocess.Popen(start_command, os.P_NOWAIT)
        print("Flink started, please set FLINK_CONF_DIR to:\nexport FLINK_CONF_DIR=%s"%self.job_conf_dir)
        
        
    def stop_flink(self):
        logging.debug("Stop Flink")
        self.set_env() 
        stop_command = os.path.join(FLINK_HOME, "bin/stop-all.sh")
        logging.debug("Execute: %s"%stop_command)
        os.system(stop_command)
    
    
    def start(self):
        if not os.environ.has_key("FLINK_CONF_DIR") or os.path.exists(os.environ["FLINK_CONF_DIR"])==False:
            self.configure_flink()
        else:
            logging.debug("Existing Flink Conf dir? %s"%os.environ["FLINK_CONF_DIR"])
            self.job_conf_dir=os.environ["FLINK_CONF_DIR"]

        self.start_flink()
        

    def stop(self):
        if os.environ.has_key("FLINK_CONF_DIR") and os.path.exists(os.environ["FLINK_CONF_DIR"])==True:
            self.job_conf_dir=os.environ["FLINK_CONF_DIR"]
            self.job_log_dir=os.path.join(self.job_conf_dir, "../log")
        self.stop_flink()


    def set_env(self):
        logging.debug("Export FLINK_CONF_DIR to %s"%self.job_conf_dir)
        os.environ["FLINK_CONF_DIR"]=self.job_conf_dir
        #os.environ["FLINK_MASTER_IP"]=socket.gethostname().split(".")[0]
        os.environ["FLINK_MASTER_IP"]=socket.gethostbyname(socket.gethostname())
        print "Flink conf dir: %s; MASTER_IP: %s"%(os.environ["FLINK_CONF_DIR"],os.environ["FLINK_MASTER_IP"])
        os.system("pkill -9 java")


    def check_flink(self):
        number_taskmanagers=0
        try:
            url = "http://" + self.master + ":8081/overview"
            logging.debug("Check flink at: %s"%url)
            response = urllib.urlopen(url)
            data = response.read()
            logging.debug(data)
            dict_data = json.loads(data)
            logging.debug(str(dict_data))
            number_taskmanagers=dict_data["taskmanagers"]
        except:
            pass # REST Web Service not up yet
        return number_taskmanagers

#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :

    signal.signal(signal.SIGALRM, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGINT, handler)

    parser = OptionParser()
    parser.add_option("-s", "--start", action="store_true", dest="start",
                  help="start Flink", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                  help="terminate Flink")

    logging.debug("Bootstrap Flink on " + socket.gethostname())
    node_list = FlinkBootstrap.get_nodelist_from_resourcemanager()
    number_nodes = len(node_list)
    print "nodes: %s"%str(node_list)

    try:
        os.makedirs(WORKING_DIRECTORY)
    except:
        pass

    run_timestamp=datetime.datetime.now()
    performance_trace_filename = "flink_performance_" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
    performance_trace_file = open(os.path.join(WORKING_DIRECTORY, performance_trace_filename), "a")
    start = time.time()

 
    if not os.path.exists(FLINK_HOME):
        download_destination = os.path.join(WORKING_DIRECTORY,"flink.tar.gz")
        if os.path.exists(download_destination)==False:
            logging.debug("Download: %s to %s" % (FLINK_DOWNLOAD_URL, download_destination))
            opener = urllib.FancyURLopener({})
            opener.retrieve(FLINK_DOWNLOAD_URL, download_destination);
        else:
            logging.debug("Found existing Flink binaries at: " + download_destination)
        logging.debug("Install Flink " + VERSION)

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf flink.tar.gz")

    end_download = time.time() 
    performance_trace_file.write("download,flink, %d, %.5f\n"%(number_nodes, end_download-start))
    performance_trace_file.flush() 
    (options, args) = parser.parse_args()
    
    flink = FlinkBootstrap(WORKING_DIRECTORY, FLINK_HOME)
    if options.start:
        flink.start()
        number_workers=0
        while number_workers!=number_nodes:
            number_taskmanagers=flink.check_flink()
            number_workers=number_taskmanagers
            logging.debug("Number workers: %d, number nodes: %d"%(number_workers,number_nodes))
            time.sleep(1)
        end_start=time.time()
        performance_trace_file.write("startup,flink, %d, %.5f\n"%(number_nodes, (end_start-end_download)))
        performance_trace_file.flush()
    else:
        flink.stop()
        if options.clean:
            directory = "/tmp/hadoop-"+os.getlogin()
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)
    
    print "Finished launching of Flink Cluster - Sleeping now"
    f = open(os.path.join(WORKING_DIRECTORY, 'flink_started'), 'w')
    f.close()

    while STOP==False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    flink.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "flink_started"))
        
        
    
    
    
