import commandline.hadoop



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
        spark_cluster.submit_spark_job(
                    resource_url="fork://localhost",
                    working_directory="/tmp",
                    number_cores=1,
                    cores_per_node=1
        )
        pilot = pilot_hadoop.PilotCompute()
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



    def __init__(self):
        return self


    def cancel(self):
        """ Remove the PilotCompute from the PilotCompute Service.

            Keyword arguments:
            None
        """
        pass


    def get_state(self):
        pass
