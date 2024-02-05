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
import xomlib.xomlibutils as xomlibutils

def get_xom_config(configname='xomconfig.cfg'):
    xomconfig = configparser.ConfigParser()
    xomconfig.sections()
    configfilename = configname
    fname = constant.xomfolder + '/config/' + configfilename
    if os.path.isfile(fname):
        xomconfig.read(fname)

    return xomconfig

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

def read_json(filename):
    # Opening JSON file
    with open(filename) as json_file:
        data = json.load(json_file)
    return data

config_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config/")
tool_config = read_json(config_dir + 'tool_config.json')
 
def sleep(delay, message=""):
    for remaining in range(delay, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("waiting " + message + ": {:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r waiting " + message + " complete!            \n")


    
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


def get_container(run_id):
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
    
def load_result_json(fullname):
    '''
    loads a json file with the xomresults content from the result folder
    '''
    # load the json files:
    with open(fullname) as res_file:
        res_contents = res_file.read()
    parsed_json = json.loads(res_contents)
    return parsed_json


def df_to_dict(dfrow):
    '''
    translates one row from the df query to a dictionnary 
    '''
    dfrow.to_dict()
    
    
    #                            '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_value", "variable_error","analysis_name","analysis_version","runid","container"])')
    
def search_in_file(filename, to_search):
    """ assess if the string 'to_search' is in 'filename'
    """
    with open(filename, "r", encoding='utf-8') as f:
        a= f.read()
    if to_search in a:
        return True
    else:
        return False

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

def wait_for_slot(job_name=None, jobs_limit=10):
    """ 
    checks if the maximum number of jobs running (as defined in constant.py) at the same time is reached
    Waits for 2minutes if reached, proceed otherwise


    """
    nr_of_jobs = check_jobs(job_name)
    limit = jobs_limit
    if nr_of_jobs > limit:
        utils.sleep(120,"for jobs to finish, now " + str(nr_of_jobs) + ' jobs are running')
        wait_for_slot(job_name)
    else:
        print("job limit not reached. will proceed")
        pass



def print_db_entries(dbname, n_entries):
    '''
    dbname: full name with prefix
    n_entries: number of entries to show, if n_entries = 0 shows the length of the db
    '''
    db = xomlibutils.connect_db(dbname)
    df = db.df_query_all()
    if df.empty:
        print(f">>> database :{dbname} is empty")
        return
    df_s = df.sort_values(by='_time', ascending=False)     
    print(f">>> database : {dbname} <<<")
    print(df_s[['analysis_name','runid','variable_name','_time' ]].head(n_entries))
    

def get_container_from_env():
    conda_env = os.popen("printenv CONDA_DEFAULT_ENV").read()
    # lower case:
    conda_env= conda_env.lower()
    conda_env=conda_env.strip()
    conda_env=conda_env.replace('_','-')
    conda_env += '.simg'
    return conda_env
    
