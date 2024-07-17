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
from datetime import datetime
import pytz

import straxen
import strax
import cutax

from utilix import xent_collection
coll = xent_collection()

import xomlib.dblib as dbl


import functools
import time

def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer

@timer
def timer_test():
    time.sleep(3)


################## LOGGER ########################
import logging
def get_logger(name, log_folder):
    '''
    set the common logger for all the code with the common properties.
    
    '''
    now = datetime.now() # current date and time
    date_time = now.strftime("%Y_%m_%d")
    logger = logging.getLogger(name)
    log_format = "%(asctime)s  - %(name)s - %(levelname)s - %(message)s"
    log_level = 10
    logger.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    fh = logging.FileHandler(log_folder + 'xom_'+date_time+ '.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def connect_db(dbname):
    db = dbl.Xomdb(dbname)
    return db

            
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


def get_time_stamp(timezone='UTC'):
    ''' return the current time timestamp in the time zone given in argument
    timezone: pytz timezone (for a list of them: import pytz; for tz in pytz.all_timezones; print tz)
    '''
    return datetime.now(pytz.timezone(timezone))




############## DAQ RUN DB Function ###############
from utilix import xent_collection
coll = xent_collection()

#@timer
def get_rundb_collection(query, itemstokeep={'number': 1, 'rate': 1, 'start': 1, 'end':1}):
    '''
    return a list with the collection item matching the query on the rundb mongo db 
    query: dictionnary, ex: {}
    itemstokeep: dictionnary with the name of the items to keep with 1 as value. Important for query performance issues. 
    '''
    c = coll.find(query,itemstokeep)
    return c


def get_last_rundb(query={}):    
    '''
    return the sorted result of the query i.e. latest ended run with the query 'query' 

    ---- 
    query: dictionnary with query to be added to the end run requirement
           ex: query={'status': 'transferred'}
    '''

    last_query = {'end': {'$ne': None}}
    last_query.update(query)
    c = get_rundb_collection(last_query)
    return c.sort("number",-1).limit(1)[0]["number"]


 
################### MISC ######################
def get_container_from_env():
    conda_env = os.popen("printenv CONDA_DEFAULT_ENV").read()
    # lower case:
    conda_env= conda_env.lower()
    conda_env=conda_env.strip()
    conda_env=conda_env.replace('_','-')
    conda_env += '.simg'
    return conda_env

def sleep(delay, message=""):
    for remaining in range(delay, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("waiting " + message + ": {:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r waiting " + message + " complete!            \n")

def mysleep(delay, message=""):
    for remaining in range(delay, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("waiting " + message + ": {:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r waiting " + message + " complete!            \n")

def get_container(run_id, tool_config):
    '''retrieve the container corresponding to the run_id

    list of container is given in the wiki:
    https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenonnt:analysis:analysis_tools_team:midway3_rucio_guide#which_container_and_context_should_i_use
    '''
    for cont, run_id_lim in tool_config['container_dict'].items():
        if run_id >= run_id_lim['from']:
            if run_id_lim['to'] == 0:
                return cont
        if run_id_lim['from'] <= run_id <=run_id_lim['to'] :
            return cont



################# JOBS ####################
def wait_for_slot(job_name=None, jobs_limit=10):
    """ 
    checks if the maximum number of jobs running (as defined in constant.py) at the same time is reached
    Waits for 2minutes if reached, proceed otherwise


    """
    nr_of_jobs = check_jobs(job_name)
    limit = jobs_limit
    if nr_of_jobs > limit:
        mysleep(120,"for jobs to finish, now " + str(nr_of_jobs) + ' jobs are running')
        wait_for_slot(job_name)
    else:
        print("job limit not reached. will proceed")
        pass

def check_jobs(job_name=None):
    """ 
    check the number of jobs currently running with the job_name
    the check is done by executing the squeue command with the user name from env. 
    
    """
    username = os.environ.get("USER")
    if job_name:
        command =  "squeue -u " + username + " -n " + job_name + " | wc --lines"
    else:
        command =  "squeue -u " + username + " | wc --lines"

    execcommand = shlex.split(command)
    process = subprocess.run(command,
                             stdout=subprocess.PIPE,
                             universal_newlines=True, shell=True)
    nr_of_lines = int(process.stdout) - 1
    return nr_of_lines

############## FILE/FOLDER ################
def remove_file(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)

def empty_directory(path, prefix=None):
    if prefix:
        to_be_removed = path + "/"+  prefix + '*'
    else:
        to_be_removed = path + '*'
    print("to_be_removed = ", to_be_removed)
    for i in glob.glob(to_be_removed):
        remove_file(i)


def search_in_file(filename, to_search):
    """ assess if the string 'to_search' is in 'filename'
    """
    with open(filename, "r", encoding='utf-8') as f:
        a= f.read()
    if to_search in a:
        return True
    else:
        return False



##################### JSON FILES RELATED #######################

def load_result_json(fullname):
    '''
    loads a json file with the xomresults content from the result folder
    '''
    # load the json files:
    with open(fullname) as res_file:
        res_contents = res_file.read()
    parsed_json = json.loads(res_contents)
    return parsed_json


def read_json(filename):
    # Opening JSON file
    with open(filename) as json_file:
        data = json.load(json_file)
    return data



####################  CONFIG RELATED #################
def get_analysis_config(filename):
    analysis_config = configparser.ConfigParser()
    analysis_config.sections()
    print(filename)
    fname = config_dir + filename

    try:
        if os.path.isfile(fname):
            analysis_config.read(fname)
            return analysis_config
        else:
            raise Exception(f"Config File {fname} do not exist")
    except Exception as error:
        # handle the exception
        print(f"An exception occurred in xom config reading: {error}")


def get_from_config(xomconfig, analysis_name, item_to_return):
    analysis_names = xomconfig.sections()
    if analysis_name not in analysis_names:
        print("error in analyis name")
    else:
        if xomconfig.has_option(analysis_name,item_to_return):
            item_returned  = xomconfig.get(analysis_name,item_to_return)
            if item_to_return in ['exclude_tags', 'include_tags', 'container', 'available_type','run_mode']:
                item_returned = item_returned.split(',')

            return item_returned
            if item_to_return in ['container']:
                # returns the dictionnary composed of the container name and a array with extremum accepted runs
                cont_dict = ast.literal_eval(item_to_return)

        else:
            return ""

def get_daq_run_ts(runid):
    query = {'number': runid}
    cursor = coll.find(query, {'number': 1, 'rate': 1, 'start': 1, 'end':1})                      
    date = cursor[0].get('end')
    date = pytz.timezone("UTC").localize(date)
    return date

def get_delta_days(date, computing_tz):
    now = datetime.now(pytz.timezone(computing_tz))
    return (date - now).days

#################### STRAXEN #######################
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

