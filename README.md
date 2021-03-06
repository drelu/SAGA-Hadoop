# SAGA Hadoop

Last Updated: 12/09/2017

# Overview:

Use [SAGA](http://saga-project.github.io/saga-python/) to spawn an Hadoop Cluster within an HPC batch job.

Currently supported SAGA adaptors:

- Fork
- Torque

Requirements:

	* PBS/Torque cluster
	* Working directory should be on a shared filesystem
	* Setup password-less documentation
	* JAVA needs to be installed and in PATH

By default SAGA-Hadoop deploys an Hadoop 2.2.0 YARN cluster. The cluster can be customized by adjusting the templates for the Hadoop configuration files in `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml` and `yarn-site.xml` in the `hadoop2/bootstrap_hadoop2.py`.



# Usage

Requirement:
    
    pip install pykafka saga-python

Try to run a local Hadoop (e.g. for development and testing)
	
	
	
    pip install saga-hadoop
    saga-hadoop --resource fork://localhost
    
    
Try to run a Hadoop cluster inside a PBS/Torque job:

    saga-hadoop --resource pbs+ssh://india.futuregrid.org --number_cores 8

Some Blog Posts about SAGA-Hadoop:

    * <http://randomlydistributed.blogspot.com/2011/01/running-hadoop-10-on-distributed.html>


# Packages:

see `hadoop1` for setting up a Hadoop 1.x.x cluster

see `hadoop2` for setting up a Hadoop 2.7.x cluster
 
see `spark` for setting up a Spark 2.2.x cluster

see `kafka` for setting up a Kafka 1.0.x cluster

see `flink` for setting up a Flink 1.1.4 cluster

see `dask` for setting up a Dask Distributed 1.20.2 cluster


# Examples:


***Stampede:***

    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=239 --number_cores=256 --project=xxx


***Gordon:***

    saga-hadoop --resource=pbs://localhost --walltime=59 --number_cores=16 --project=TG-CCR140028 --framework=spark
    

***Wrangler***

    export JAVA_HOME=/usr/java/jdk1.8.0_45/
    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=59 --number_cores=24 --project=xxx


