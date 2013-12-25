#!/usr/bin/env python

import os
import sys
from setuptools import setup
import subprocess

try:
    import saga
except:
    print "Installing BigJob and SAGA/Python."

if sys.version_info < (2, 6):
    sys.stderr.write("BigJob requires Python 2.6 and above. Installation unsuccessful!")
    sys.exit(1)

VERSION_FILE="VERSION"    
    

def update_version():
    if not os.path.isdir(".git"):
        print "This does not appear to be a Git repository."
        return
    try:
        p = subprocess.Popen(["git", "describe",
                              "--tags", "--always"],
                             stdout=subprocess.PIPE)
    except EnvironmentError:
        print "Unable to run git, not modifying VERSION"
        return
    stdout = p.communicate()[0]
    if p.returncode != 0:
        print "Unable to run git, not modifying VERSION"
        return
    
    ver = stdout.strip()
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    f = open(fn, "w")
    f.write(ver)
    f.close()
    print "BigJob VERSION: '%s'" % ver


def get_version():
    try:
        fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
        f = open(fn)
        version = f.read().strip()
        f.close()
    except EnvironmentError:
        return "-1"
    return version    

    
update_version()
    
setup(name='SAGA-Hadoop',
      version=get_version(),
      description='Using SAGA ',
      author='Andre Luckow, et al.',
      author_email='aluckow@cct.lsu.edu',
      url='https://github.com/drelu/saga-hadoop',
      classifiers = ['Development Status :: 5 - Production/Stable',                  
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['hadoop1', 'hadoop2'],
      include_package_data=True,
      # data files for easy_install
      
      install_requires=['uuid', 'radical.utils', 'saga-python' ],
      entry_points = {
        'console_scripts': [ ]
    }
)
