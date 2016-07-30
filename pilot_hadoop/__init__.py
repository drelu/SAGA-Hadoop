#######################
# Mac OS:
# brew install apache-spark
# SPARK_HOME='/usr/local/Cellar/apache-spark/2.0.0/libexec/'
# Start Spark: /usr/local/Cellar/apache-spark/2.0.0/libexec/sbin/start-all.sh
# py4j needs be installed in your virtualenv

import spark.bootstrap_spark
import os, sys
import time

SPARK_HOME=os.environ["SPARK_HOME"]
print "SPARK Home: %s"%SPARK_HOME

try:
    sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
    sys.path.insert(0, os.path.join(SPARK_HOME, 'python/lib/py4j-0.8.2.1-src.zip'))
    sys.path.insert(0, os.path.join(SPARK_HOME, 'bin') )
    
    # import Spark Libraries
    from pyspark import SparkContext, SparkConf, Accumulator, AccumulatorParam
    from pyspark.sql import SQLContext
    from pyspark.sql.types import *
    from pyspark.mllib.linalg import Vector

except:
    print "Error  importing Spark"

class PilotComputeDescription(dict):
    """ B{PilotComputeDescription (PCD).}

        A PilotComputeDescription is a based on the attributes defined on
        the SAGA Job Description.

        The PilotComputeDescription is used by the application to specify
        what kind of PilotJobs it requires.

        Example::
             pilot_compute_description = {
                           resource_url="fork://localhost",
                           working_directory="/tmp",
                           number_cores=1,
                           cores_per_node=1
                           type="spark"
                           }

        B{Attention}: The PilotComputeDescription is mapped 1:1 to the underlying SAGA-Python
        job description, which is used for launching the pilot that manages the Hadoop cluster.

    """

    def __init__(self):
        pass


    def __setattr__(self, attr, value):
        self[attr]=value


    def __getattr__(self, attr):
        return self[attr]


    #############################################################################

class PilotCompute(object):
    """ B{PilotCompute (PC)}.

        This is the object that is returned by the PilotComputeService when a
        new PilotCompute (aka Pilot-Job) is created based on a PilotComputeDescription.

        A Pilot-Compute in Pilot-Hadoop represents an active Hadoop YARN or Spark cluster

        The PilotCompute object can be used by the application to keep track
        of PilotComputes that are active.

        A PilotCompute has state, can be queried, can be cancelled and be
        re-initialized.
    """


    def __init__(self, saga_job=None, details=None, spark_context=None, spark_sql_context=None):
        self.saga_job = saga_job
        self.details = details
        self.spark_context = spark_context
        self.spark_sql_context = spark_sql_context


    def cancel(self):
        """ Remove the PilotCompute from the PilotCompute Service.

            Keyword arguments:
            None
        """

        try:
            if self.details!=None and self.details.has_key("spark_home"):
                command = os.path.join(self.details["spark_home"], "sbin/stop-all.sh")
                os.system(command)
        except:
            pass


        if self.get_spark_context()!=None:
            self.get_spark_context().stop()
        if self.saga_job!=None:
            self.saga_job.cancel()


    def get_state(self):
        self.saga_job.get_state()


    def get_spark_context(self):
        if self.spark_context == None:
            self.spark_context = SparkContext(self.details["master_url"],
                                              "Pilot-Spark",
                                              sparkHome=SPARK_HOME)
        return self.spark_context


    def get_spark_sql_context(self):
        if self.spark_sql_context == None:
            self.spark_sql_context = SQLContext(self.get_spark_context())
        return self.spark_sql_context


    def get_details(self):
        return self.details



class PilotComputeService(object):
    """  B{PilotComputeService (PCS).}

        The PilotComputeService is responsible for creating and managing
        the PilotComputes.

        It is the application's interface to the Pilot-Manager in the
        P* Model.

    """

    def __init__(self, pjs_id=None):
        """ Create a PilotComputeService object.

            Keyword arguments:
            pjs_id -- Don't create a new, but connect to an existing (optional)
        """
        pass 

    @classmethod
    def create_pilot(cls, pilotcompute_description):
        """ Add a PilotCompute to the PilotComputeService

            Keyword arguments:
            pilotcompute_description -- PilotCompute Description

            Return value:
            A PilotCompute handle
        """

        if os.environ.has_key("SPARK_HOME"):
            #print "Cleanup old Spark Installation"
            try:
                os.remove(os.path.join(os.environ["SPARK_HOME"], "conf/masters"))
                os.remove(os.path.join(os.environ["SPARK_HOME"], "conf/slaves"))
            except:
                pass

        # {
        #     "resource_url":"slurm+ssh://localhost",
        #     "number_cores": 16,
        #     "cores_per_node":16,
        #     "project": "TG-CCR140028",
        #     "type":"spark"
        # }

        resource_url="fork://localhost"
        if pilotcompute_description.has_key("service_url"):
            resource_url=pilotcompute_description["service_url"]
        elif pilotcompute_description.has_key("resource_url"):
            resource_url=pilotcompute_description["resource_url"]
        if resource_url.startswith("yarn"):
            pilot = cls.__connected_yarn_spark_cluster(pilotcompute_description)
            return pilot
        elif resource_url.startswith("spark"):
            print "Connect to Spark cluster: "+ str(resource_url)
            pilot = cls.__connected_spark_cluster(resource_url, pilotcompute_description)
            return pilot
        else:
            pilot = cls.__start_spark_cluster(pilotcompute_description)
            return pilot


    def cancel(self):
        """ Cancel the PilotComputeService.

            This also cancels all the PilotJobs that were under control of this PJS.

            Keyword arguments:
            None

            Return value:
            Result of operation
        """
        pass

    @classmethod
    def __start_spark_cluster(self, pilotcompute_description):
        """
        Bootstraps Spark Cluster
        :param pilotcompute_description: dictionary containing detail about the spark cluster to launch
        :return: Pilot
        """
        import commandline.hadoop
        spark_cluster = commandline.hadoop.SAGAHadoopCLI()

        if pilotcompute_description.has_key("service_url"):
            resource_url=pilotcompute_description["service_url"]
        elif pilotcompute_description.has_key("resource_url"):
            resource_url=pilotcompute_description["resource_url"]

        working_directory="/tmp"
        if pilotcompute_description.has_key("working_directory"):
            working_directory=pilotcompute_description["working_directory"]

        project=None
        if pilotcompute_description.has_key("project"):
            project=pilotcompute_description["project"]

        queue=None
        if pilotcompute_description.has_key("queue"):
            queue=pilotcompute_description["queue"]

        walltime=10
        if pilotcompute_description.has_key("walltime"):
            walltime=pilotcompute_description["walltime"]

        number_cores=1
        if pilotcompute_description.has_key("number_cores"):
            number_cores=int(pilotcompute_description["number_cores"])


        cores_per_node=1
        if pilotcompute_description.has_key("cores_per_node"):
            cores_per_node=int(pilotcompute_description["cores_per_node"])


        saga_job = spark_cluster.submit_spark_job(
            resource_url=resource_url,
            working_directory=working_directory,
            number_cores=number_cores,
            cores_per_node=cores_per_node,
            queue=queue,
            walltime=walltime,
            project=project
        )

        details = PilotComputeService.get_spark_config_data(working_directory)
        pilot = PilotCompute(saga_job, details)
        return pilot


    @classmethod
    def __connected_yarn_spark_cluster(self, pilotcompute_description):

        number_cores=1
        if pilotcompute_description.has_key("number_cores"):
            number_cores=int(pilotcompute_description["number_cores"])
        
        number_of_processes = 1
        if pilotcompute_description.has_key("number_of_processes"):
            number_of_processes = int(pilotcompute_description["number_of_processes"])

        executor_memory="1g"
        if pilotcompute_description.has_key("number_of_processes"):
            executor_memory = pilotcompute_description["physical_memory_per_process"]

        conf = SparkConf()
        conf.set("spark.num.executors", str(number_of_processes))
        conf.set("spark.executor.instances", str(number_of_processes))
        conf.set("spark.executor.memory", executor_memory)
        conf.set("spark.executor.cores", number_cores)
        if pilotcompute_description!=None:
            for i in pilotcompute_description.keys():
                if i.startswith("spark"):
                    conf.set(i, pilotcompute_description[i])
        conf.setAppName("Pilot-Spark")
        conf.setMaster("yarn-client")
        sc = SparkContext(conf=conf)
        sqlCtx = SQLContext(sc)
        pilot = PilotCompute(spark_context=sc, spark_sql_context=sqlCtx)
        return pilot


    @classmethod
    def __connected_spark_cluster(self, resource_url, pilot_description=None):
        conf = SparkConf()
        conf.setAppName("Pilot-Spark")
        if pilot_description!=None:
            for i in pilot_description.keys():
                if i.startswith("spark"):
                    conf.set(i, pilot_description[i])
        conf.setMaster(resource_url)
        print(conf.toDebugString())
        sc = SparkContext(conf=conf)
        sqlCtx = SQLContext(sc)
        pilot = PilotCompute(spark_context=sc, spark_sql_context=sqlCtx)
        return pilot


    @classmethod
    def get_spark_config_data(cls, working_directory=None):
        spark_home_path=spark.bootstrap_spark.SPARK_HOME
        if working_directory!=None:
            spark_home_path=os.path.join(working_directory, "work", os.path.basename(spark_home_path))
        master_file=os.path.join(spark_home_path, "conf/masters")
        print master_file
        counter = 0
        while os.path.exists(master_file)==False and counter <600:
            print "Looking for %s"%master_file
            time.sleep(1)
            counter = counter + 1

        print "Open master file: %s"%master_file
        with open(master_file, 'r') as f:
            master = f.read().strip()
        f.closed
        print("Create Spark Context for URL: %s"%("spark://%s:7077"%master))
        details = {
            "spark_home": spark_home_path,
            "master_url": "spark://%s:7077"%master,
            "web_ui_url": "http://%s:8080"%master,
        }
        return details

