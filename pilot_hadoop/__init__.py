#######################
# Mac OS:
# brew install apache-spark
# SPARK_HOME='/usr/local/Cellar/apache-spark/1.3.0/libexec/'
# Start Spark: /usr/local/Cellar/apache-spark/1.3.0/libexec/sbin/start-all.sh
# py4j needs be installed in your virtualenv

import commandline.hadoop
import spark.bootstrap_spark
import os, sys

spark_home='/usr/local/Cellar/apache-spark/1.3.0/libexec/'
#if details!=None:
#    spark_home = details["spark_home"]
os.environ["SPARK_HOME"] = spark_home
sys.path.insert(0, os.path.join(spark_home, "python"))

# import Spark Libraries
from pyspark import SparkContext, SparkConf, Accumulator, AccumulatorParam
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vector


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



    def __init__(self, saga_job, details):
        self.saga_job = saga_job
        self.details = details
        self.spark_context = None

    def cancel(self):
        """ Remove the PilotCompute from the PilotCompute Service.

            Keyword arguments:
            None
        """
        self.saga_job.cancel()


    def get_state(self):
        self.saga_job.get_state()


    def get_spark_context(self):
        if self.spark_context == None:
            self.spark_context = SparkContext(self.details["master_url"],
                                              "Pilot-Spark",
                                              sparkHome=spark_home)
        return self.spark_context

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
        spark_cluster = commandline.hadoop.SAGAHadoopCLI()


        working_directory=os.getcwd()

        # {
        #     "resource_url":"slurm+ssh://localhost",
        #     "number_cores": 16,
        #     "cores_per_node":16,
        #     "project": "TG-CCR140028",
        #     "type":"spark"
        # }

        working_directory="/tmp"
        if pilotcompute_description.has_key("working_directory"):
            working_directory=pilotcompute_description["working_directory"]

        resource_url="fork://localhost"
        if pilotcompute_description.has_key("resource_url"):
            resource_url=pilotcompute_description["resource_url"]

        project=None
        if pilotcompute_description.has_key("project"):
            project=pilotcompute_description["project"]

        queue=None
        if pilotcompute_description.has_key("queue"):
            queue=pilotcompute_description["queue"]

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
                    project=project
        )

        details = PilotComputeService.get_spark_config_data(working_directory)
        pilot = PilotCompute(saga_job, details)
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
    def get_spark_config_data(cls, working_directory):

        with open(os.path.join(working_directory,
                               spark.bootstrap_spark.SPARK_HOME, "conf/masters"), 'r') as f:
            master = f.read()
        f.closed
        print("Create Spark Context for URL: %s"%("spark://%s:7077"%master))
        details = {
            "spark_home": spark.bootstrap_spark.SPARK_HOME,
            "master_url": "spark://%s:7077"%master,
            "web_ui_url": "http://%s:8080"%master,
        }
        return details

