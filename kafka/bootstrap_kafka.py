#!/usr/bin/env python
""" Kafka Bootstrap Script (based on Kafka 0.10 release) """
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
import pkg_resources
import datetime

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
VERSION="0.10.0.0"
KAFKA_DOWNLOAD_URL = "http://www-us.apache.org/dist/kafka/" + VERSION + "/kafka_2.11-" + VERSION + ".tgz"
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

# For using an existing installation
KAFKA_HOME=os.path.join(os.getcwd(), "work/kafka-" + VERSION)
KAFKA_CONF_DIR=os.path.join(KAFKA_HOME, "etc")

STOP=False

def handler(signum, frame):
    logging.debug("Signal catched. Stop Hadoop")
    global STOP
    STOP=True
    
    

class KafkaBootstrap():


    def __init__(self, working_directory, kafka_home, config_name="default"):
        self.working_directory=working_directory
        self.kafka_home=kafka_home
        self.config_name=config_name
        self.jobid = "kafka-"+str(uuid.uuid1())
        self.job_working_directory = os.path.join(WORKING_DIRECTORY, self.jobid)
        self.job_conf_dir = os.path.join(self.job_working_directory, "config")
        self.broker_config_files = {}
        os.makedirs(self.job_conf_dir)


    
    def get_server_properties(self, master, hostname, broker_id):
        module = "kafka.configs." + self.config_name
        print("Access config in module: " + module + " File: server.properties")
        my_data = pkg_resources.resource_string(module, "server.properties")
        my_data = my_data%(broker_id, hostname, hostname, master)
        my_data = os.path.expandvars(my_data)
        return my_data




    def get_zookeeper_propertiesl(self, hostname):
        module = "kafka.configs." + self.config_name
        logging.debug("Access config in module: " + module + " File: zookeeper.properties")
        my_data = pkg_resources.resource_string(module, "zookeeper.properties")
        return my_data


    #######################################################################################
    ## Get Node List from Resource Management System
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
        logging.debug("Init SGE or Local")
        sge_node_file = os.environ.get("PE_HOSTFILE")    
        if sge_node_file == None:
            return ["localhost"]
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
            return ["localhost"]

        print "***** Hosts: " + str(hosts) 
        hosts=hostlist.expand_hostlist(hosts)
        number_cpus_per_node = 1
        if os.environ.get("SLURM_CPUS_ON_NODE")!=None:
            number_cpus_per_node=int(os.environ.get("SLURM_CPUS_ON_NODE"))
        freenodes = []
        for h in hosts:
            #for i in range(0, number_cpus_per_node):
            freenodes.append((h + "\n"))
        return list(set(freenodes))

    def get_nodelist_from_resourcemanager(self):
        if (os.environ.get("PBS_NODEFILE") != None and os.environ.get("PBS_NODEFILE") != ""):
            nodes = self.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST") != None):
            nodes = self.get_slurm_allocated_nodes()
        else:
            nodes = self.get_sge_allocated_nodes()
        return nodes


    #######################################################################################
    def configure_kafka(self):
        logging.debug("Kafka Instance Configuration Directory: " + self.job_conf_dir)
        nodes = self.get_nodelist_from_resourcemanager()
        logging.debug("Kafka nodes: " + str(nodes))
        master = socket.gethostname().split(".")[0]

        for idx, node in enumerate(nodes):
            path = os.path.join(self.job_conf_dir, "broker-%d"%idx)
            os.makedirs(path)
            server_properties_filename = os.path.join(path, "server.properties")
            server_properties_file = open(server_properties_filename, "w")
            server_properties_file.write(self.get_server_properties(master=master, hostname=node.strip(), broker_id=idx))
            server_properties_file.close()
            self.broker_config_files[node]=server_properties_filename
        
        zookeeper_properties_file = open(os.path.join(self.job_conf_dir, "zookeeper.properties"), "w")
        zookeeper_properties_file.write(self.get_zookeeper_propertiesl(master))
        zookeeper_properties_file.close()


    def start_kafka(self):
        logging.debug("Start Kafka")
        os.system("killall -s 9 java") 
        os.system("pkill -9 java") 
        time.sleep(5)

        logging.debug("Start Zookeeper")
        start_command = os.path.join(self.kafka_home, "bin/zookeeper-server-start.sh") + " -daemon " + os.path.join(self.job_conf_dir, "zookeeper.properties")
        logging.debug("Execute: %s"%start_command)
        os.system(". ~/.bashrc & " + start_command)

        logging.debug("Start Kafka Cluster")
        for node in self.broker_config_files.keys():
            config = self.broker_config_files[node] 
            start_command = os.path.join("ssh " + node.strip() + " " + self.kafka_home, "bin/kafka-server-start.sh") + \
                                        " -daemon " + config
            logging.debug("Execute: %s"%start_command)
            os.system(". ~/.bashrc & " + start_command)

        print("Kafka started with configuration: %s"%self.job_conf_dir)


        
    def stop_kafka(self):
        logging.debug("Stop Kafka")
        self.set_env() 
        stop_command = os.path.join(KAFKA_HOME, "bin/kafka-server-stop.sh")
        logging.debug("Execute: %s"%stop_command)
        stop_command = os.path.join(KAFKA_HOME, "bin/zookeeper-server-stop.sh")
        logging.debug("Execute: %s"%stop_command)
        os.system(stop_command)

    def start(self):
        self.configure_kafka()
        self.start_kafka()
        
    def stop(self):
        self.stop_kafka()
    


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
                  help="start Kafka", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                  help="terminate Hadoop")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                  help="clean Kafka topics in Zookeeper after termination")

    parser.add_option("-n", "--config_name", action="store", type="string", dest="config_name", default="default")

    (options, args) = parser.parse_args()
    config_name=options.config_name
    logging.debug("Bootstrap Kafka on " + socket.gethostname())

    filename=os.path.basename(KAFKA_DOWNLOAD_URL)
    kafka_home=""
    if not os.path.exists(KAFKA_HOME):
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass
        

        download_destination =os.path.join(WORKING_DIRECTORY, filename)
        if os.path.exists(download_destination)==False:
            logging.debug("Download: %s to %s" % (KAFKA_DOWNLOAD_URL, download_destination))
            opener = urllib.FancyURLopener({})
            opener.retrieve(KAFKA_DOWNLOAD_URL, download_destination);
        else:
            logging.debug("Found existing Kafka binaries at: " + download_destination)
        logging.debug("Install Kafka")

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf %s"%filename)
        kafka_home=os.path.join(WORKING_DIRECTORY, os.path.splitext(filename)[0])
        os.environ["KAFKA_HOME"]=kafka_home

    kafka = KafkaBootstrap(WORKING_DIRECTORY, kafka_home, config_name)
    if options.start:
        kafka.start()
    else:
        kafka.stop()
        if options.clean:
            directory = "/tmp/zookeeper/"
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)
    
    print "Finished launching of Kafka Cluster - Sleeping now"
    f = open(os.path.join(WORKING_DIRECTORY, 'kafka_started'), 'w')
    f.write(datetime.datetime.now().isoformat())
    f.close()

    while STOP==False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    kafka.stop()
    os.remove(os.path.join(WORKING_DIRECTORY, "started"))
        
        
    
    
    
