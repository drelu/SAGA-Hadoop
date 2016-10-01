#!/usr/bin/env python
""" Spark Bootstrap Script (based on Spark 1.2 release) """
import os, sys
import pdb
import urllib
import subprocess
import logging
import uuid
import shutil
import time
import signal
import socket
import hostlist
from optparse import OptionParser

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
VERSION="2.0.0"
SPARK_DOWNLOAD_URL = "http://mirror.reverse.net/pub/apache/spark/spark-"+ VERSION + "/spark-" + VERSION+"-bin-hadoop2.6.tgz"
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

# For using an existing installation
#if not os.environ.has_key("SPARK_HOME"):
SPARK_HOME=os.path.join(os.getcwd(), "work/", os.path.basename(SPARK_DOWNLOAD_URL).rpartition(".")[0])
#else:
# SPARK_HOME=os.environ["SPARK_HOME"]

SPARK_CONF_DIR=os.path.join(SPARK_HOME, "conf")

STOP=False

# Spark Configuration
# https://spark.apache.org/docs/latest/spark-standalone.html
#
# SPARK_MASTER_IP	Bind the master to a specific IP address, for example a public one.
# SPARK_MASTER_PORT	Start the master on a different port (default: 7077).
# SPARK_MASTER_WEBUI_PORT	Port for the master web UI (default: 8080).
# SPARK_MASTER_OPTS	Configuration properties that apply only to the master in the form "-Dx=y" (default: none). See below for a list of possible options.
# SPARK_LOCAL_DIRS	Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks.
# SPARK_WORKER_CORES	Total number of cores to allow Spark applications to use on the machine (default: all available cores).
# SPARK_WORKER_MEMORY	Total amount of memory to allow Spark applications to use on the machine, e.g. 1000m, 2g (default: total memory minus 1 GB); note that each application's individual memory is configured using its spark.executor.memory property.
# SPARK_WORKER_PORT	Start the Spark worker on a specific port (default: random).
# SPARK_WORKER_WEBUI_PORT	Port for the worker web UI (default: 8081).
# SPARK_WORKER_INSTANCES	Number of worker instances to run on each machine (default: 1). You can make this more than 1 if you have have very large machines and would like multiple Spark worker processes. If you do set this, make sure to also set SPARK_WORKER_CORES explicitly to limit the cores per worker, or else each worker will try to use all the cores.
# SPARK_WORKER_DIR	Directory to run applications in, which will include both logs and scratch space (default: SPARK_HOME/work).
# SPARK_WORKER_OPTS	Configuration properties that apply only to the worker in the form "-Dx=y" (default: none). See below for a list of possible options.
# SPARK_DAEMON_MEMORY	Memory to allocate to the Spark master and worker daemons themselves (default: 512m).
# SPARK_DAEMON_JAVA_OPTS	JVM options for the Spark master and worker daemons themselves in the form "-Dx=y" (default: none).
# SPARK_PUBLIC_DNS	The public DNS name of the Spark master and workers (default: none).


def handler(signum, frame):
    logging.debug("Signal catched. Stop Hadoop")
    global STOP
    STOP=True

    

class SparkBootstrap(object):


    def __init__(self, working_directory, spark_home):
        self.working_directory=working_directory
        self.jobid = "spark-conf-"+str(uuid.uuid1())
        #self.job_working_directory = os.path.join(WORKING_DIRECTORY, self.jobid)
        self.job_working_directory=spark_home
        self.job_conf_dir = os.path.join(self.job_working_directory, "conf")


    
    def get_pbs_allocated_nodes(self):
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


    def get_sge_allocated_nodes(self):
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

    def get_slurm_allocated_nodes(self):
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


    def configure_spark(self):
        #logging.debug("Copy config from " + SPARK_CONF_DIR + " to: " + self.job_conf_dir)
        #shutil.copytree(SPARK_CONF_DIR, self.job_conf_dir)
        if(os.environ.get("PBS_NODEFILE")!=None and os.environ.get("PBS_NODEFILE")!=""):
            nodes=self.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST")!=None):
            nodes=self.get_slurm_allocated_nodes()
        else:
            nodes=self.get_sge_allocated_nodes() 
        if nodes!=None:
            #master = socket.gethostname().split(".")[0]
            #master = socket.gethostbyname(socket.gethostname())
            master_file = open(os.path.join(self.job_conf_dir, "masters"), "w")
            master_file.write(nodes[0].strip()) 
            master_file.close()
            master_file2=open(os.path.join(WORKING_DIRECTORY, 'spark_master'), 'w')
            master_file2.write(nodes[0].strip())
            master_file2.close()    

            slave_file = open(os.path.join(self.job_conf_dir, "slaves"), "w")
            slave_file.writelines(nodes) 
            slave_file.close()
            logging.debug("Spark cluster nodes: " + str(nodes))


    def start_spark(self):
        logging.debug("Start Spark")
        self.set_env()
        start_command = os.path.join(SPARK_HOME, "sbin/start-all.sh")
        logging.debug("Execute: %s"%start_command)
        #os.system(". ~/.bashrc & " + start_command)
        status = subprocess.call(start_command, shell=True)
        print("SPARK started, please set SPARK_CONF_DIR to:\nexport SPARK_CONF_DIR=%s"%self.job_conf_dir)
        
        
    def stop_spark(self):
        logging.debug("Stop Spark")
        self.set_env() 
        stop_command = os.path.join(SPARK_HOME, "sbin/stop-all.sh")
        logging.debug("Execute: %s"%stop_command)
        os.system(stop_command)
    
    
    def start(self):
        if not os.environ.has_key("SPARK_CONF_DIR") or os.path.exists(os.environ["SPARK_CONF_DIR"])==False:
            self.configure_spark()
        else:
            logging.debug("Existing SPARK Conf dir? %s"%os.environ["SPARK_CONF_DIR"])
            self.job_conf_dir=os.environ["SPARK_CONF_DIR"]

        self.start_spark()
        

    def stop(self):
        if os.environ.has_key("SPARK_CONF_DIR") and os.path.exists(os.environ["SPARK_CONF_DIR"])==True:
            self.job_conf_dir=os.environ["SPARK_CONF_DIR"]
            self.job_log_dir=os.path.join(self.job_conf_dir, "../log")
        self.stop_spark()


    def set_env(self):
        logging.debug("Export SPARK_CONF_DIR to %s"%self.job_conf_dir)
        os.environ["SPARK_CONF_DIR"]=self.job_conf_dir
        #os.environ["SPARK_MASTER_IP"]=socket.gethostname().split(".")[0]
        os.environ["SPARK_MASTER_IP"]=socket.gethostbyname(socket.gethostname())
        print "Spark conf dir: %s; MASTER_IP: %s"%(os.environ["SPARK_CONF_DIR"],os.environ["SPARK_MASTER_IP"])
        os.system("pkill -9 java")


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
                  help="start Spark", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                  help="terminate Spark")

    logging.debug("Bootstrap SPARK on " + socket.gethostname())
    
    if not os.path.exists(SPARK_HOME):
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass
        

        download_destination = os.path.join(WORKING_DIRECTORY,"spark.tar.gz")
        if os.path.exists(download_destination)==False:
            logging.debug("Download: %s to %s"%(SPARK_DOWNLOAD_URL, download_destination))
            opener = urllib.FancyURLopener({})
            opener.retrieve(SPARK_DOWNLOAD_URL, download_destination);
        else:
            logging.debug("Found existing SPARK binaries at: " + download_destination)
        logging.debug("Install SPARK " + VERSION)

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf spark.tar.gz")

    
   
    (options, args) = parser.parse_args()
    
    spark = SparkBootstrap(WORKING_DIRECTORY, SPARK_HOME)
    if options.start:
        spark.start()
    else:
        spark.stop()
        if options.clean:
            directory = "/tmp/hadoop-"+os.getlogin()
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)
    
    print "Finished launching of SPARK Cluster - Sleeping now"
    f = open(os.path.join(WORKING_DIRECTORY, 'spark_started'), 'w')
    f.close()

    while STOP==False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    spark.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "started"))
        
        
    
    
    
