#!/usr/bin/env python
""" Hadoop Bootstrap Script (based on Hadoop 2.7.1 release) """
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

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
VERSION="2.7.1"
HADOOP_DOWNLOAD_URL = "http://apache.osuosl.org/hadoop/common/hadoop-"+ VERSION + "/hadoop-"+ VERSION + ".tar.gz"
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

# For using an existing installation
HADOOP_HOME=os.path.join(os.getcwd(), "work/hadoop-" + VERSION)
HADOOP_CONF_DIR=os.path.join(HADOOP_HOME, "etc/hadoop")

STOP=False

def handler(signum, frame):
    logging.debug("Signal catched. Stop Hadoop")
    global STOP
    STOP=True
    
    

class Hadoop2Bootstrap(object):


    def __init__(self, working_directory, config_name="default"):
        self.working_directory=working_directory
        self.config_name=config_name
        self.jobid = "hadoop-conf-"+str(uuid.uuid1())
        self.job_working_directory = os.path.join(WORKING_DIRECTORY, self.jobid)
        self.job_conf_dir = os.path.join(self.job_working_directory, "etc/hadoop/")
        self.job_name_dir = os.path.join(self.job_working_directory, "name")
        self.job_log_dir = os.path.join(self.job_working_directory, "logs")
        try:
            #os.makedirs(self.job_conf_dir)       
            os.makedirs(self.job_name_dir)
            os.makedirs(self.job_log_dir)
        except:
            pass
    
    
    def get_core_site_xml(self, hostname):
        module = "hadoop2.configs." + self.config_name
        print("Access config in module: " + module + " File: core-site.xml")
        my_data = pkg_resources.resource_string(module, "core-site.xml")
        my_data = my_data%(hostname)
        my_data = os.path.expandvars(my_data)
        return my_data

    #     return """<?xml version="1.0"?>
    # <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    # <configuration>
    #      <property>
    #          <name>fs.default.name</name>
    #          <value>hdfs://%s:9000</value>
    #      </property>
    #      <property>
    #          <name>hadoop.tmp.dir</name>
    #          <value>%s</value>
    #      </property>
    # </configuration>"""%(hostname,HADOOP_TMP_DIR)


    def get_hdfs_site_xml(self, hostname, name_dir):
        module = "hadoop2.configs." + self.config_name
        logging.debug("Access config in module: " + module + " File: hdfs-site.xml")
        my_data = pkg_resources.resource_string(module, "hdfs-site.xml")
        my_data = my_data%(name_dir)
        my_data = os.path.expandvars(my_data)
        return my_data
    #
    #     return """<?xml version="1.0"?>
    # <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    # <configuration>
    #      <property>
    #          <name>dfs.replication</name>
    #          <value>3</value>
    #      </property>
    #      <property>
    #          <name>dfs.name.dir</name>
    #          <value>%s</value>
    #      </property>
    #      <!--property>
    #          <name>dfs.datanode.dns.interface</name>
    #          <value>eth1</value>
    #      </property-->
    #      <property>
    #           <name>dfs.datanode.data.dir.perm</name>
    #           <value>700</value>
    #           <description>Permissions for the directories on on the local filesystem where
    #           the DFS data node store its blocks. The permissions can either be octal or
    #             symbolic.</description>
    #     </property>
    #      <property>
    #          <name>dfs.webhdfs.enabled</name>
    #          <value>true</value>
    #      </property>
    #      <property>
    #          <name>dfs.datanode.max.locked.memory</name>
    #          <value>25769803776</value>
    #      </property>
    # </configuration>"""%(name_dir)
    
    
    def get_mapred_site_xml(self, hostname):
        module = "hadoop2.configs." + self.config_name
        logging.debug("Access config in module: " + module + " File: mapred-site.xml")
        my_data = pkg_resources.resource_string(module, "mapred-site.xml")
        my_data = my_data%(hostname)
        my_data = os.path.expandvars(my_data)
        return my_data

    #     return """<?xml version="1.0"?>
    #  <configuration>
    #      <!--property>
    #          <name>mapred.job.tracker</name>
    #          <value>%s:9001</value>
    #      </property-->
    #      <property>
    #          <name>mapreduce.framework.name</name>
    #          <value>yarn</value>
    #      </property>
    #      <property>
    #        <name>mapreduce.map.java.opts</name>
    #        <value>-Xmx1792m</value>
    #     </property>
    #      <property>
    #        <name>mapreduce.reduce.java.opts</name>
    #        <value>-Xmx1792m</value>
    #     </property>
    # </configuration>"""%(hostname)
    
    
    def get_yarn_site_xml(self, hostname):
        module = "hadoop2.configs." + self.config_name
        logging.debug("Access config in module: " + module)
        my_data = pkg_resources.resource_string(module, "yarn-site.xml")
        my_data = my_data%(hostname)
        my_data = os.path.expandvars(my_data)
        return my_data

#         return """<?xml version="1.0"?>
# <configuration>
#   <property>
#     <name>yarn.resourcemanager.scheduler.class</name>
#     <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
#     <description>In case you do not want to use the default scheduler</description>
#   </property>
#   <property>
#     <name>yarn.resourcemanager.hostname</name>
#     <value>%s</value>
#   </property>
#   <property>
#     <name>yarn.nodemanager.local-dirs</name>
#     <value>/tmp</value>
#     <description>the local directories used by the nodemanager</description>
#   </property>
#   <property>
#     <name>yarn.nodemanager.aux-services</name>
#     <value>mapreduce_shuffle</value>
#     <description>shuffle service that needs to be set for Map Reduce to run </description>
#   </property>
#   <property>
#     <name>yarn.nodemanager.delete.debug-delay-sec</name>
#     <value>3600</value>
#     <description>delay deletion of user cache </description>
#   </property>
#   <property>
#     <name>yarn.resourcemanager.resource-tracker.address</name>
#     <value>${yarn.resourcemanager.hostname}:11031</value>
#   </property>
#   <property>
#     <name>yarn.nodemanager.resource.memory-mb</name>
#     <value>16384</value>
#   </property>
#   <property>
#     <name>yarn.scheduler.minimum-allocation-mb</name>
#     <value>2048</value>
#   </property>
#   <property>
#     <name>yarn.nodemanager.resource.cpu-vcores</name>
#     <value>16</value>
#   </property>
# </configuration>"""%(hostname)
    
    
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


    def configure_hadoop(self):
        logging.debug("Copy config from " + HADOOP_CONF_DIR + " to: " + self.job_conf_dir)
        shutil.copytree(HADOOP_CONF_DIR, self.job_conf_dir)
        if(os.environ.get("PBS_NODEFILE")!=None and os.environ.get("PBS_NODEFILE")!=""):
            nodes=self.get_pbs_allocated_nodes()
        elif (os.environ.get("SLURM_NODELIST")!=None):
            nodes=self.get_slurm_allocated_nodes()
        else:
            nodes=self.get_sge_allocated_nodes() 
        if nodes!=None:
            #master = nodes[0].strip()
            master = socket.gethostname().split(".")[0]
            master_file = open(os.path.join(self.job_conf_dir, "masters"), "w")
            master_file.write(master) 
            master_file.close()

            slave_file = open(os.path.join(self.job_conf_dir, "slaves"), "w")
            slave_file.writelines(nodes) 
            slave_file.close()
            logging.debug("Hadoop cluster nodes: " + str(nodes))
        
        core_site_file = open(os.path.join(self.job_conf_dir, "core-site.xml"), "w")
        core_site_file.write(self.get_core_site_xml(master))
        core_site_file.close() 
        
        hdfs_site_file = open(os.path.join(self.job_conf_dir, "hdfs-site.xml"), "w")
        hdfs_site_file.write(self.get_hdfs_site_xml(master, self.job_name_dir))
        hdfs_site_file.close() 
        
        mapred_site_file = open(os.path.join(self.job_conf_dir, "mapred-site.xml"), "w")
        mapred_site_file.write(self.get_mapred_site_xml(master))
        mapred_site_file.close() 
        
        yarn_site_file = open(os.path.join(self.job_conf_dir, "yarn-site.xml"), "w")
        yarn_site_file.write(self.get_yarn_site_xml(master))
        yarn_site_file.close() 
        

    def start_hadoop(self):
        logging.debug("Start Hadoop")    
        os.system("killall -s 9 java") 
        os.system("pkill -9 java") 
        time.sleep(5)
        if not os.environ.has_key("HADOOP_CONF_DIR") or os.path.exists(os.environ["HADOOP_CONF_DIR"])==False:
            self.set_env()    
            format_command = os.path.join(HADOOP_HOME, "bin/hadoop") + " --config " + self.job_conf_dir + " namenode -format"
            logging.debug("Execute: %s"%format_command)
            os.system("rm -rf /tmp/hadoop-*")           
            os.system(format_command)        

        else:
            logging.debug("Don't format namenode. Reconnect to existing namenode")

        self.set_env()    
        start_command = os.path.join(HADOOP_HOME, "sbin/start-all.sh")
        logging.debug("Execute: %s"%start_command)
        os.system(". ~/.bashrc & " + start_command)
        print("Hadoop started, please set HADOOP_CONF_DIR to:\nexport HADOOP_CONF_DIR=%s"%self.job_conf_dir)
        
        
    def stop_hadoop(self):
        logging.debug("Stop Hadoop")    
        self.set_env() 
        stop_command = os.path.join(HADOOP_HOME, "sbin/stop-all.sh")
        logging.debug("Execute: %s"%stop_command)
        os.system(stop_command)
    
    
    def start(self):
        if not os.environ.has_key("HADOOP_CONF_DIR") or os.path.exists(os.environ["HADOOP_CONF_DIR"])==False:
            self.configure_hadoop()
        else:
            logging.debug("Existing Hadoop Conf dir? %s"%os.environ["HADOOP_CONF_DIR"])
            self.job_conf_dir=os.environ["HADOOP_CONF_DIR"]
            self.job_log_dir=os.path.join(self.job_conf_dir, "../log")
            self.job_name_dir=os.path.join(self.job_conf_dir, "../name")
        self.start_hadoop()
        
    def stop(self):
        if os.environ.has_key("HADOOP_CONF_DIR") and os.path.exists(os.environ["HADOOP_CONF_DIR"])==True:
            self.job_conf_dir=os.environ["HADOOP_CONF_DIR"]
            self.job_log_dir=os.path.join(self.job_conf_dir, "../log")
        self.stop_hadoop()
    
    def set_env(self):
        logging.debug("Export HADOOP_CONF_DIR to %s"%self.job_conf_dir)
        os.environ["HADOOP_CONF_DIR"]=self.job_conf_dir  
        logging.debug("Export HADOOP_LOG_DIR to %s"%self.job_log_dir)
        os.environ["HADOOP_LOG_DIR"]=self.job_log_dir
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
                  help="start Hadoop", default=True)
    parser.add_option("-q", "--quit", action="store_false", dest="start",
                  help="terminate Hadoop")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                  help="clean HDFS datanodes after termination")

    parser.add_option("-n", "--config_name", action="store", type="string", dest="config_name")

    (options, args) = parser.parse_args()
    config_name=options.config_name
    logging.debug("Bootstrap Hadoop on " + socket.gethostname() + " Config: " + config_name)
    
    if not os.path.exists(HADOOP_HOME):
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass
        

        download_destination = os.path.join(WORKING_DIRECTORY,"hadoop.tar.gz")
        if os.path.exists(download_destination)==False:
            logging.debug("Download: %s to %s"%(HADOOP_DOWNLOAD_URL, download_destination))
            opener = urllib.FancyURLopener({})
            opener.retrieve(HADOOP_DOWNLOAD_URL, download_destination);
        else:
            logging.debug("Found existing Hadoop binaries at: " + download_destination)
        logging.debug("Install Hadoop 2")

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf hadoop.tar.gz")

    hadoop = Hadoop2Bootstrap(WORKING_DIRECTORY, config_name)
    if options.start:
        hadoop.start()
    else:
        hadoop.stop()
        if options.clean:
            directory = "/tmp/hadoop-"+os.getlogin()
            logging.debug("delete: " + directory)
            shutil.rmtree(directory)
        sys.exit(0)
    
    print "Finished launching of Hadoop Cluster - Sleeping now"
    f = open(os.path.join(WORKING_DIRECTORY, 'started'), 'w')
    f.close()

    while STOP==False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    hadoop.stop()    
    os.remove(os.path.join(WORKING_DIRECTORY, "started"))
        
        
    
    
    
