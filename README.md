# SAGA Hadoop

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

see `hadoop2` for setting up a Hadoop 2.2.x cluster 