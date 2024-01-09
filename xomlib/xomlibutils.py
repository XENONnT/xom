import configparser
import shlex
import subprocess
import os
import sys
import time
import pandas as pd
import glob
import ast 
import json

import straxen
import strax
import cutax


import xomlib.constant as constant
import xomlib.dblib as dbl



def connect_db(dbname):
    db = dbl.Xomdb(dbname)
    return db

def connect_dbs(prefix=""):
    list_of_db = constant.list_of_db
    dbs = []
    for dbname in list_of_db:
        dbfullname = prefix + dbname
        dbs.append(connect_db(dbfullname))
    return dbs
    
            
#def construct_filename(xomres):
def construct_filename(meas_name, an_name,an_version,an_var, runid):
    ''' 
    construct the filename out of the basic information of the xomresult
    '''
    return meas_name + "_" + an_name + "_" + an_version + '_' + an_var + '_' + str(runid)  + '.json'  
#  return xomres.measurement_name + "_" + xomres.analysis_name + "_" + xomres.analysis_version + '_' + xomres.variable_name + '_' + str(xomres.runid) + '.json'  

def load_straxen():
    '''
    defines the context to be shared by all the analyses and the data checks

    the runs accessible are listed here:
    https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenonnt:analysis:analysis_tools_team:midway3_rucio_guide#which_container_and_context_should_i_use
    '''
    st = cutax.contexts.xenonnt_online(_rucio_local_path='/project/lgrandi/rucio', include_rucio_local = True, output_folder='/scratch/midway2/gaior/strax_data/')
    st.storage.append(strax.DataDirectory("/project2/lgrandi/xenonnt/processed/", readonly=True))
    st.storage.append(strax.DataDirectory("/project/lgrandi/xenonnt/processed/", readonly=True))

    return st
