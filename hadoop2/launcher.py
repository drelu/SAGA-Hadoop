#!/usr/bin/env python

import time
import saga
import os, sys
import subprocess
import pdb

import logging
logging.basicConfig(level=logging.DEBUG)

def main():
    
    try:
        # create a job service for Futuregrid's 'india' PBS cluster
        js = saga.job.Service("pbs+ssh://india")
        #js = saga.job.Service("fork://localhost")

        # describe our job
        jd = saga.job.Description()
        # resource requirements
        jd.total_cpu_count = 16   
        # environment, executable & arguments
        executable = os.path.join(os.getcwd(), "bootstrap_hadoop2.py")
        logging.debug("Run %s"%executable)
        jd.executable  = executable
        jd.arguments   = []
        # output options
        jd.output = "hadoop_job.stdout"
        jd.error  = "hadoop_job.stderr"
        jd.working_directory=os.getcwd()
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
                    get_hadoop_config_data(id)
                    break
            time.sleep(3)



    except Exception as ex:
        print "An error occured: %s" % (str(ex))


def get_hadoop_config_data(jobid):
    pbs_id = jobid[jobid.find("-")+2:len(jobid)-1]
    nodes = subprocess.check_output(["qstat", "-f", pbs_id])
    hosts = "empty"
    for i in nodes.split("\n"):
        if i.find("exec_host")>0:
            hosts = i[i.find("=")+1:].strip()

    hadoop_home=os.path.join(os.getcwd(), "work/hadoop-1.0.0")
    print "HADOOP installation directory: %s"%hadoop_home
    print "Allocated Resources for Hadoop cluster: " + hosts 
    print "HDFS Web Interface: http://%s:50070"% hosts[:hosts.find("/")]   
    print "\nTo use Hadoop set HADOOP_CONF_DIR: "
    print "export HADOOP_CONF_DIR=%s"%(os.path.join(os.getcwd(), "work", get_most_current_job(), "conf")) 
    print "%s/bin/hadoop dfsadmin -report"%hadoop_home
    print ""

def get_most_current_job():
    dir = "work"
    files = os.listdir(dir)
    max = None
    for i in files:
        if i.startswith("hadoop-conf"):
            t = os.path.getctime(os.path.join(dir,i))
            if max == None or t>max[0]:
                max = (t, i)
    return max[1]

if __name__ == "__main__":
    main()
