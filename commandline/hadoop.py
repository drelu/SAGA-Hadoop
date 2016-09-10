'''
Command Line Util for using BigJob (via the Pilot-API)
'''
import saga
import argparse
import sys
import os
import pdb
import pickle
import hadoop1, hadoop2
import logging
import time
import subprocess
import re
import spark.bootstrap_spark



logging.basicConfig(level=logging.DEBUG)
 
SAGA_HADOOP_DIRECTORY="~/.hadoop"  
  
class SAGAHadoopCLI(object):
    
    def __init__(self):
        self.pilots = []
        self.__restore()

    ####################################################################################################################
    # Kafka 0.10
    def submit_kafka_job(self,
                         resource_url="fork://localhost",
                         working_directory=os.getcwd(),
                         number_cores=1,
                         cores_per_node=1,
                         spmd_variation=None,
                         queue=None,
                         walltime=None,
                         project=None,
                         config_name="default"
                         ):

        try:
            # create a job service for Futuregrid's 'india' PBS cluster
            js = saga.job.Service(resource_url)
            # describe our job
            jd = saga.job.Description()
            # resource requirements
            jd.total_cpu_count = int(number_cores)
            # environment, executable & arguments
            executable = "python"
            arguments = ["-m", "kafka.bootstrap_kafka"]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            jd.executable  = executable
            jd.arguments   = arguments
            # output options
            jd.output =  os.path.join("kafka_job.stdout")
            jd.error  = os.path.join("kafka_job.stderr")
            jd.working_directory=working_directory
            jd.queue=queue
            if project!=None:
                jd.project=project
            #jd.environment =
            if spmd_variation!=None:
                jd.spmd_variation=spmd_variation
            if walltime!=None:
                jd.wall_time_limit=walltime

            # create the job (state: New)
            myjob = js.create_job(jd)

            #print "Starting Spark bootstrap job ..."
            # run the job (submit the job to PBS)
            myjob.run()
            id = myjob.get_id()
            #id = id[id.index("]-[")+3: len(id)-1]
            #print "**** Job: " + str(id) + " State : %s" % (myjob.get_state())
            #print "Wait for Spark Cluster to startup. File: %s" % (os.path.join(working_directory, "work/spark_started"))

            while True:
                state = myjob.get_state()
                if state=="Running":
                    if os.path.exists(os.path.join(working_directory, "work/kafka_started")):
                        self.get_kafka_config_data(id, working_directory)
                        break
                elif state == "Failed":
                    break
                time.sleep(3)
            return myjob

        except Exception as ex:
            print "An error occurred: %s" % (str(ex))


    def get_kafka_config_data(self, jobid, working_directory=None, all=False):
        base_work_dir = os.path.join(working_directory, "work")
        kafka_config_dirs = [i if os.path.isdir(os.path.join(base_work_dir,i)) and i.find("kafka-")>=0 else None for i in os.listdir(base_work_dir)]
        kafka_config_dirs = filter(lambda a: a != None, kafka_config_dirs)
        kafka_config_dirs.sort(key=lambda x: os.path.getmtime(os.path.join(base_work_dir, x)),  reverse=True)
        if all == False: kafka_config_dirs=kafka_config_dirs[:1]
        for kafka_config_dir in kafka_config_dirs:
            conf = os.path.join(base_work_dir, kafka_config_dir, "config")
            broker_config_dirs =[i if os.path.isdir(os.path.join(conf, i)) and i.find("broker-")>=0 else None for i in os.listdir(conf)]
            broker_config_dirs = filter(lambda a: a != None, broker_config_dirs)
            for broker in broker_config_dirs:
                with open(os.path.join(conf,broker, "server.properties"), "r") as config:
                    print "Kafka Config: %s (%s)"%(conf, time.ctime(os.path.getmtime(conf)))
                    lines = config.readlines()
                    for line in lines:
                        if line.startswith("broker.id") or line.startswith("listeners") or line.startswith("zookeeper.connect"):
                            print line.strip().replace("=", ": ")


    ####################################################################################################################
    # Spark 1.x
    def submit_spark_job(self,
                          resource_url="fork://localhost",
                          working_directory=os.getcwd(),
                          number_cores=1,
                          cores_per_node=1,
                          spmd_variation=None,
                          queue=None,
                          walltime=None,
                          project=None,
                          config_name="default"
    ):

        try:
            # create a job service for Futuregrid's 'india' PBS cluster
            js = saga.job.Service(resource_url)
            # describe our job
            jd = saga.job.Description()
            # resource requirements
            jd.total_cpu_count = int(number_cores)
            # environment, executable & arguments
            executable = "python"
            arguments = ["-m", "spark.bootstrap_spark"]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            jd.executable  = executable
            jd.arguments   = arguments
            # output options
            jd.output =  os.path.join("spark_job.stdout")
            jd.error  = os.path.join("spark_job.stderr")
            jd.working_directory=working_directory
            jd.queue=queue
            if project!=None:
                jd.project=project
            #jd.environment =
            if spmd_variation!=None:
                jd.spmd_variation=spmd_variation
            if walltime!=None:
                jd.wall_time_limit=walltime

            # create the job (state: New)
            myjob = js.create_job(jd)

            #print "Starting Spark bootstrap job ..."
            # run the job (submit the job to PBS)
            myjob.run()
            id = myjob.get_id()
            #id = id[id.index("]-[")+3: len(id)-1]
            #print "**** Job: " + str(id) + " State : %s" % (myjob.get_state())
            #print "Wait for Spark Cluster to startup. File: %s" % (os.path.join(working_directory, "work/spark_started"))

            while True:
                state = myjob.get_state()
                if state=="Running":
                    if os.path.exists(os.path.join(working_directory, "work/spark_started")):
                        self.get_spark_config_data(id, working_directory)
                        break
                elif state == "Failed":
                    break
                time.sleep(3)
            return myjob

        except Exception as ex:
            print "An error occurred: %s" % (str(ex))

    def scan_dir(dir):
        for name in os.listdir(dir):
            path = os.path.join(dir, name)
            if os.path.isfile(path):
                print path
            else:
                scan_dir(path)


    def get_spark_config_data(self, jobid, working_directory=None):
        spark_home_path=spark.bootstrap_spark.SPARK_HOME
        # search for spark_home:
        base_work_dir = os.path.join(working_directory, "work")
        spark_home=''.join([i.strip() if os.path.isdir("work/" + i) and i.find("spark")>=0 else '' for i in os.listdir("work")])
        spark_home_path=os.path.join(working_directory, "work", os.path.basename(spark_home_path))
        master_file=os.path.join(spark_home_path, "conf/masters")
        #print master_file
        counter = 0
        while os.path.exists(master_file)==False and counter <600:
            time.sleep(1) 
            counter = counter + 1

        with open(master_file, 'r') as f:
            master = f.read()
        f.closed
        print "SPARK installation directory: %s"%spark_home_path
        print "(please allow some time until the SPARK cluster is completely initialized)"
        print "export PATH=%s/bin:$PATH"%(spark_home_path)
        print "Spark Web URL: http://" + master + ":8080"
        print "Spark Submit endpoint: spark://" + master + ":7077"


    ####################################################################################################################
    # Hadoop 2.x Support
    def submit_hadoop_job(self,
                          resource_url="fork://localhost",
                          working_directory=os.getcwd(),
                          number_cores=1,
                          cores_per_node=1,
			              spmd_variation=None,
                          queue=None,
                          walltime=None,
                          project=None,
                          config_name="default"
                     ):
        
        try:
            # create a job service for Futuregrid's 'india' PBS cluster
            js = saga.job.Service(resource_url)
                    # describe our job
            jd = saga.job.Description()
            # resource requirements
            jd.total_cpu_count = int(number_cores)
            # environment, executable & arguments
            executable = "python"
            arguments = ["-m", "hadoop2.bootstrap_hadoop2", "-n", config_name]
            logging.debug("Run %s Args: %s"%(executable, str(arguments)))
            jd.executable  = executable
            jd.arguments   = arguments
            # output options
            jd.output =  os.path.join("hadoop_job.stdout")
            jd.error  = os.path.join("hadoop_job.stderr")
            jd.working_directory=working_directory
            jd.queue=queue
            if project!=None:
                jd.project=project
            #jd.environment =
            if spmd_variation!=None:
                jd.spmd_variation=spmd_variation
            if walltime!=None:
                jd.wall_time_limit=walltime

            # create the job (state: New)
            myjob = js.create_job(jd)
    
            print "Starting Hadoop bootstrap job...\n"
            # run the job (submit the job to PBS)
            myjob.run()
            id = myjob.get_id()
            #id = id[id.index("]-[")+3: len(id)-1]
            print "**** Job: " + str(id) + " State : %s" % (myjob.get_state())
    
            while True:
                state = myjob.get_state()
                if state=="Running":
                    if os.path.exists("work/started"):
                        self.get_hadoop_config_data(id)
                        break
                time.sleep(3)
        except Exception as ex:
            print "An error occurred: %s" % (str(ex))
        
        
    def get_hadoop_config_data(self, jobid):
        hosts = "localhost/"
        pbs_id = jobid[jobid.find("-")+2:len(jobid)-1]
        try:
            nodes = subprocess.check_output(["qstat", "-f", pbs_id])
            for i in nodes.split("\n"):
                if i.find("exec_host")>0:
                    hosts = i[i.find("=")+1:].strip()
        except:
            pass
        hadoop_home=os.path.join(os.getcwd(), "work/hadoop-2.7.1")
        print "HADOOP installation directory: %s"%hadoop_home
        #print "Allocated Resources for Hadoop cluster: " + hosts 
        #print "YARN Web Interface: http://%s:8088"% hosts[:hosts.find("/")]
        #print "HDFS Web Interface: http://%s:50070"% hosts[:hosts.find("/")]   
        print "(please allow some time until the Hadoop cluster is completely initialized)"
        print "\nTo use Hadoop set HADOOP_CONF_DIR: "
        print "export HADOOP_CONF_DIR=%s"%(os.path.join(os.getcwd(), "work", self.get_most_current_job(), "etc/hadoop")) 
        print "export HADOOP_HOME=%s"%(hadoop_home) 
        print "export PATH=%s/bin:$PATH"%(hadoop_home) 
        print "\nSmoke Test:"
        print "hadoop dfsadmin -report"
        print ""     
        print "Namenode Web URL: http://" + self.get_namenode_host() + ":50070"    
        print "YARN Web URL: http://" + self.get_namenode_host() + ":8088"    
        print ""     

    def get_namenode_host(self):
        core_site=open(os.path.join(os.getcwd(), "work", 
                                    self.get_most_current_job(), 
                                    "etc/hadoop/core-site.xml"), "r")
        core_site_content=core_site.read()
        m = re.search("(?<=<value>hdfs://)(.*):.*(?=</value>)", core_site_content)
        if m:
            return m.group(1)
        return None
    
    def get_most_current_job(self):
        dir = "work"
        files = os.listdir(dir)
        max = None
        for i in files:
            if i.startswith("hadoop-conf"):
                t = os.path.getctime(os.path.join(dir,i))
                if max == None or t>max[0]:
                    max = (t, i)
        return max[1]       

    def cancel(self, pilot_url):
        pass
    
    
    def list(self):
        print "\SAGA Hadoop Job\t\t\t\t\t\t\t\t\tState"
        print "-----------------------------------------------------------------------------------------------------"
        
    ###########################################################################
    # auxiliary methods

    def version(self):
        print "SAGA Hadoop Version: " + bigjob.version
    
    def clean(self):
        os.remove(self.__get_save_filename())
    
    ###########################################################################
    # private and protected methods
    
    def __persist(self):
        f = open(self.__get_save_filename(), 'wb')
        pickle.dump(self.pilots, f)
        f.close()
    
    def __restore(self):
        if os.path.exists(self.__get_save_filename()):
            try:
                f = open(self.__get_save_filename(), 'rb')
                self.pilots = pickle.load(f)
                f.close()
            except:
                pass

    def __get_save_filename(self):
        return os.path.join(os.path.expanduser(SAGA_HADOOP_DIRECTORY), 'pilot-cli.p')


def main():
    app = SAGAHadoopCLI()
    parser = argparse.ArgumentParser(add_help=True, description="""SAGA Hadoop Command Line Utility""")
    
    parser.add_argument('--clean', action="store_true", default=False)
    parser.add_argument('--version', action="store_true", default=False)    
    
    saga_hadoop_group = parser.add_argument_group('Manage SAGA Hadoop clusters')
    saga_hadoop_group.add_argument('--resource', action="store", nargs="?", metavar="RESOURCE_URL", 
                              help="submit a job to specified resource, e.g. fork://localhost",
                              default="fork://localhost")
    saga_hadoop_group.add_argument('--working_directory', action="store", nargs="?", metavar="WORKING_DIRECTORY", 
                              help="submit a job to specified resource, e.g. fork://localhost",
                              default=os.getcwd())    
        
    saga_hadoop_group.add_argument('--spmd_variation', action="store", nargs="?", metavar="SPMD_VARIATION", 
                              help="Parallel environment, e.g. openmpi",
                              default=None)
    saga_hadoop_group.add_argument('--queue', action="store", nargs="?", metavar="QUEUE", 
                              help="Queue Name",
                              default=None)    
    saga_hadoop_group.add_argument('--walltime', action="store", nargs="?", metavar="WALLTIME", 
                              help="Wall time limit",
                              default=None)    

    saga_hadoop_group.add_argument('--number_cores', default="1", nargs="?")
    saga_hadoop_group.add_argument('--cores_per_node',  default="1", nargs="?")    
    saga_hadoop_group.add_argument('--project', action="store", nargs="?", metavar="PROJECT", help="Allocation id for project", default=None)

    saga_hadoop_group.add_argument('--framework', action="store", nargs="?", metavar="FRAMEWORK", help="Framework to start: [hadoop, spark, kafka]", default="hadoop")
    saga_hadoop_group.add_argument("-n", "--config_name", action="store", nargs="?", metavar="CONFIG_NAME", help="Name of config for host", default="default")

    parsed_arguments = parser.parse_args()    
    
    
    
    if parsed_arguments.version==True:
        app.version()
    elif parsed_arguments.framework=="kafka":
        app.submit_kafka_job(resource_url=parsed_arguments.resource,
                             working_directory=parsed_arguments.working_directory,
                             number_cores=parsed_arguments.number_cores,
                             cores_per_node=parsed_arguments.cores_per_node,
                             spmd_variation=parsed_arguments.spmd_variation,
                             queue=parsed_arguments.queue,
                             walltime=parsed_arguments.walltime,
                             project=parsed_arguments.project,
                             config_name=parsed_arguments.config_name)
    elif parsed_arguments.framework=="spark":
        app.submit_spark_job(resource_url=parsed_arguments.resource,
                              working_directory=parsed_arguments.working_directory,
                              number_cores=parsed_arguments.number_cores,
                              cores_per_node=parsed_arguments.cores_per_node,
                              spmd_variation=parsed_arguments.spmd_variation,
                              queue=parsed_arguments.queue,
                              walltime=parsed_arguments.walltime,
                              project=parsed_arguments.project,
                              config_name=parsed_arguments.config_name)
    else:
        app.submit_hadoop_job(resource_url=parsed_arguments.resource, 
                              working_directory=parsed_arguments.working_directory, 
                              number_cores=parsed_arguments.number_cores, 
                              cores_per_node=parsed_arguments.cores_per_node,
                              spmd_variation=parsed_arguments.spmd_variation,
                              queue=parsed_arguments.queue,
                              walltime=parsed_arguments.walltime,
                              project=parsed_arguments.project,
                              config_name=parsed_arguments.config_name)
    
    
        
if __name__ == '__main__':
    main()
    
