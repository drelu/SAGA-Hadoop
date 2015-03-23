import commandline.hadoop
import spark.bootstrap_spark
import os


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


    def cancel(self):
        """ Remove the PilotCompute from the PilotCompute Service.

            Keyword arguments:
            None
        """
        self.saga_job.cancel()


    def get_state(self):
        self.saga_job.get_state()


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

        if pilotcompute_description.has_key("working_directory"):
            working_directory=pilotcompute_description["working_directory"]

        saga_job = spark_cluster.submit_spark_job(
                    resource_url="fork://localhost",
                    working_directory=working_directory,
                    number_cores=1,
                    cores_per_node=1
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
        details = {
            "spark_home": spark.bootstrap_spark.SPARK_HOME,
            "master_url": "spark://%s:7077"%master,
            "web_ui_url": "http://%s:8080"%master,
        }
        return details

