import os, sys
import socket
import logging
try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", 'VERSION')
    if not os.path.exists(fn):
        fn = os.path.join(sys.prefix, 'VERSION')
    #print "Open Version file: " + str(fn)
    version = open(fn).read().strip()
    logging.info("Loading SAGA-Hadoop version: " + version + " on " + socket.gethostname())
except IOError:
    pass