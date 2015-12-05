# SAGA Hadoop

Last Updated: 12/05/2015

# Overview:

Use [SAGA](http://saga-project.github.io/saga-python/) to spawn an Hadoop Cluster within an HPC batch job.

Currently supported SAGA adaptors:

- Fork
- Torque

Requirements:

	* PBS/Torque cluster
	* Working directory should be on a shared filesystem

By default SAGA-Hadoop deploys an Hadoop 2.2.0 YARN cluster. The cluster can be customized by adjusting the templates for the Hadoop configuration files in `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml` and `yarn-site.xml` in the `hadoop2/bootstrap_hadoop2.py`.

# Usage


Try to run a local Hadoop (e.g. for development and testing)

    easy_install saga-hadoop
    saga-hadoop --resource fork://localhost
    
    
Try to run a Hadoop cluster inside a PBS/Torque job:

    saga-hadoop --resource pbs+ssh://india.futuregrid.org --number_cores 8

Some Blog Posts about SAGA-Hadoop:

    * <http://randomlydistributed.blogspot.com/2011/01/running-hadoop-10-on-distributed.html>


# Packages:

see `hadoop1` for setting up a Hadoop 1.x.x cluster

see `hadoop2` for setting up a Hadoop 2.7.x cluster
 
see `spark` for setting up a Spark 1.5.x cluster


# Examples:


***Stampede:***

    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=239 --number_cores=256 --project=TG-MCB090174


***Gordon:***

    saga-hadoop --resource=pbs://localhost --walltime=59 --number_cores=16 --project=TG-CCR140028 --framework=spark
    

***Wrangler***

    export JAVA_HOME=/usr/java/jdk1.8.0_45/
    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=59 --number_cores=24 --project=TG-MCB090174


