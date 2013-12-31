#!/usr/bin/env python
""" Hadoop Bootstrap Script (based on hadoop 0.20.203 release) """
import os, sys
import pdb
import urllib
import subprocess
import logging
import uuid
import shutil
import time
import signal
from optparse import OptionParser

logging.basicConfig(level=logging.DEBUG)

# For automatic Download and Installation
HADOOP_DOWNLOAD_URL = "http://psg.mtu.edu/pub/apache/hadoop/common/hadoop-1.2.1/hadoop-1.2.1-bin.tar.gz"
WORKING_DIRECTORY = os.path.join(os.getcwd(), "work")

# For using an existing installation
# HADOOP 0.22 - no webhdfs
# HADOOP 1.0.0 - has support for webhdfs
HADOOP_HOME=os.path.join(os.getcwd(), "work/hadoop-1.2.1")


STOP=False

def handler(signum, frame):
    logging.debug("Signal catched. Stop Hadoop")
    global STOP
    STOP=True

class HadoopBootstrap(object):

    def __init__(self, working_directory):
        self.working_directory=working_directory
        self.jobid = "hadoop-conf-"+str(uuid.uuid1())
        self.job_working_directory = os.path.join(WORKING_DIRECTORY, self.jobid)
        self.job_conf_dir = os.path.join(self.job_working_directory, "conf")
        self.job_name_dir = os.path.join(self.job_working_directory, "name")
        self.job_log_dir = os.path.join(self.job_working_directory, "logs")
        try:
            os.makedirs(job_dir)       
            os.makedirs(log_dir)
        except:
            pass
    
    def get_core_site_xml(self, hostname):
        return """<?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
         <property>
             <name>fs.default.name</name>
             <value>hdfs://%s:9000</value>
         </property>
    </configuration>"""%(hostname)
    
    def get_hdfs_site_xml(self, hostname, name_dir):
        return """<?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
         <property>
             <name>dfs.replication</name>
             <value>1</value>
         </property>
         <property>
             <name>dfs.name.dir</name>
             <value>%s</value>
         </property>
         <!--property>
             <name>dfs.datanode.dns.interface</name>
             <value>eth1</value>
         </property-->
         <property>
              <name>dfs.datanode.data.dir.perm</name>
              <value>700</value>
              <description>Permissions for the directories on on the local filesystem where
              the DFS data node store its blocks. The permissions can either be octal or
                symbolic.</description>
        </property>     
         <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
         </property>         
    </configuration>"""%(name_dir)
    
    
    def get_mapred_site_xml(self,hostname):
        return """<?xml version="1.0"?>
    <configuration>
         <property>
             <name>mapred.job.tracker</name>
             <value>%s:9001</value>
         </property>
    </configuration>"""%(hostname)
    
    
    def get_pbs_allocated_nodes(self):
        pbs_node_file = os.environ.get("PBS_NODEFILE")    
        if pbs_node_file == None:
            return
        f = open(pbs_node_file)
        nodes = f.readlines()
        for i in nodes:
            i.strip()
        f.close()    
        return list(set(nodes))


    def configure_hadoop(self):
        logging.debug("Configure Hadoop")
        shutil.copytree(os.path.join(HADOOP_HOME, "conf"), self.job_conf_dir)
        master="localhost"

        nodes = self.get_pbs_allocated_nodes()
        if nodes!=None:
            master = nodes[0].strip()
            master_file = open(os.path.join(self.job_working_directory, "conf/masters"), "w")
            master_file.write(master) 
            master_file.close()

            slave_file = open(os.path.join(self.job_working_directory, "conf/slaves"), "w")
            slave_file.writelines(nodes) 
            slave_file.close()
            logging.debug("Hadoop cluster nodes: " + str(nodes))
        
        core_site_file = open(os.path.join(self.job_working_directory, "conf/core-site.xml"), "w")
        core_site_file.write(self.get_core_site_xml(master))
        core_site_file.close() 
        
        hdfs_site_file = open(os.path.join(self.job_working_directory,"conf/hdfs-site.xml"), "w")
        hdfs_site_file.write(self.get_hdfs_site_xml(master, self.job_name_dir))
        hdfs_site_file.close() 
        
        mapred_site_file = open(os.path.join(self.job_working_directory,"conf/mapred-site.xml"), "w")
        mapred_site_file.write(self.get_mapred_site_xml(master))
        mapred_site_file.close() 
        

    def start_hadoop(self):
        logging.debug("Start Hadoop")    
        if not os.environ.has_key("HADOOP_CONF_DIR") or os.path.exists(os.environ["HADOOP_CONF_DIR"])==False:
            self.set_env()    
            format_command = os.path.join(HADOOP_HOME, "bin/hadoop") + " --config " + self.job_conf_dir + " namenode -format"
            logging.debug("Execute: %s"%format_command)
            os.system(format_command)        
        else:
            logging.debug("Don't format namenode. Reconnect to existing namenode")

        start_command = os.path.join(HADOOP_HOME, "bin/start-all.sh")
        logging.debug("Execute: %s"%start_command)
        os.system(start_command)
        print("Hadoop started, please set HADOOP_CONF_DIR to:\nexport HADOOP_CONF_DIR=%s"%self.job_conf_dir)
        
        
    def stop_hadoop(self):
        logging.debug("Stop Hadoop")    
        self.set_env() 
        stop_command = os.path.join(HADOOP_HOME, "bin/stop-all.sh")
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
    parser.add_option("-q", "--quite", action="store_false", dest="start",
                  help="terminate Hadoop")
    parser.add_option("-c", "--clean", action="store_true", dest="clean",
                  help="clean HDFS datanodes after termination")
   
    
    if not os.path.exists(HADOOP_HOME):
        logging.debug("Download Hadoop")
        try:
            os.makedirs(WORKING_DIRECTORY)
        except:
            pass
        opener = urllib.FancyURLopener({})
        opener.retrieve(HADOOP_DOWNLOAD_URL, os.path.join(WORKING_DIRECTORY,"hadoop.tar.gz"));
    
        logging.debug("Install Hadoop")

        os.chdir(WORKING_DIRECTORY)
        os.system("tar -xzf hadoop.tar.gz")
    
   
    (options, args) = parser.parse_args()
    
    hadoop = HadoopBootstrap(WORKING_DIRECTORY)
    if options.start:
        hadoop.start()
    else:
        hadoop.stop()
        if options.clean:
            dir = "/tmp/hadoop-"+os.getlogin()
            logging.debug("delete: " + dir)
            shutil.rmtree(dir)
        sys.exit(0)
    
    print "Finished launching of Hadoop Cluster - Sleeping now"
    f = open(os.path.join(WORKING_DIRECTORY, 'started'), 'w')
    f.close()

    while STOP==False:
        logging.debug("stop: " + str(STOP))
        time.sleep(10)
            
    hadoop.stop()    
    os.remove(os.path.join(WORKING_DIRECTORY, "started"))
        
        
    
    
    
