#!/usr/bin/env python
import os
from argparse import ArgumentParser
from socket import timeout
#from utilix.config import Config
from datetime import timezone, datetime, timedelta
import sys
import pprint 
import numpy as np
import time
import configparser
import shlex
import subprocess
from utilix import xent_collection
import datetime
import json

cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")
sys.path.append(cdir + "/utils/")
import constant
import dblib as dbl
import xomlib


try:
    import utils.utils as utils
except ModuleNotFoundError:
    import utils as utils
 
############### container test #################
#print(utils.get_container(53000))

############### straxen xom loading ###################
#print(utils.load_straxen())

#dict/json steps for xom results #################
data = {'a':1,'b':2}
test_dict = {'measurement_name':'meas_toto',
             'analysis_name': 'an_toto',
             'analysis_version': 'an_version_toto',
             'variable_name': 'var_name_toto',
             'variable_value': 45,
             'timestamp': datetime.datetime.now(),
             'container': 'toto.simg',
             'data': data,
             'runid':42}
xomr = xomlib.Xomresult(test_dict)
xomr.write_json()

# load the json files:
test_in = 'meas_toto_an_toto_an_version_toto_var_name_toto_42'
test_json = utils.load_json(test_in)
xomr_json = xomlib.Xomresult(test_json)
print(xomr_json.runid)


############# conversion of the df to dict ###################
# query something in the database:

#df_todo = xomdbtodo.df_query_analysis(self.analysis.analysis_name)


#
