#!/usr/bin/env python

import os
import numpy as np
import time
import subprocess
import shlex
import sys
import pytz
cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")
import xomlib.xomlibutils as xomlibutils
import xomlib.constant as constant
import xomlib.dblib as dbl
import xomlib
import xomutils as xomutils

import analysis as analysis


import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from argparse import ArgumentParser
import glob
import time
import datetime

from utilix.batchq import submit_job
import logging
sys.path.append('/home/gaior/codes/utilix/')
from utilix import xent_collection
coll = xent_collection()
import straxen
st = xomlibutils.load_straxen()            
 
from logging.handlers import TimedRotatingFileHandler
logger = logging.getLogger(__name__)
log_format = "%(asctime)s  - %(name)s - %(levelname)s - %(message)s"
log_level = 10
logger.setLevel(log_level)
formatter = logging.Formatter(log_format)
fh = TimedRotatingFileHandler(cdir+ '/logs/xommanager.log', when="midnight", interval=1)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
# add a suffix which you want
fh.suffix = "%Y%m%d"

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


class Runner:
    def __init__(self, analysis, tool_config):
        self.analysis = analysis
        self.prefix = tool_config['prefix']
        self.tool_config = tool_config
        self.base_file_name = self.tool_config['job_folder'] + self.prefix + self.analysis.analysis_name 
        self.analysis.print_config()

        self.logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        self.logger.debug(f"creating instance of {self.__class__}")

    def test_log(self):
        self.logger.debug("debug level")
        self.logger.info("info level")
        self.logger.warning("warning level")


    def make_todo_list(self, dbs):
        '''
        takes the list of database []
        '''
        # check if xom is up to date
        
        last_run_daq = coll.find({'end': {'$ne': None}}).sort("number",-1).limit(1)[0]["number"]
        
        # check the list of run already treated:
        tot_list = np.array([])
        for db in dbs:
            tot_list = np.append(tot_list, db.get_runid_list(self.analysis.analysis_name, self.analysis.analysis_version))
        unique_list = np.unique(tot_list)
        max_run = self.analysis.max_run if self.analysis.max_run else last_run_daq

        # check the daq runs within the limits given in the config file (min_run, max_run)                
        daq_runs = self.analysis.get_valid_runs(self.analysis.min_run, max_run)
        treated_runs = unique_list[ (unique_list >= self.analysis.min_run) & (unique_list <= max_run) ]
        
        # the run to be treated are the ones in the DAQ but not treated yet 
        todo_runs = np.setxor1d(daq_runs, treated_runs) 
        
        for r in todo_runs:
            r = int(r)
            self.logger.info(f"producing the todo entry for ananlys {self.analysis.analysis_name} and run: {r}")
            self.analysis.produce_todo_entry(r, self.prefix)

    def is_available(self, runid):
        """ 
        check data storage for the runid, the container and the datatype required

        ----- parameters-----:
        runid:     runid int
        
        """
        required_type = self.analysis.available_type_list
        if not required_type:
            return True
        runid_str = str(runid).zfill(6)
        container = xomutils.get_container(runid)
        # >> if the environment matches the container the data should be treated in, then we can just test with st.is_stored
        # >> otherwise raise an exception, the xom should be restarted with the correct container for these runs.
        if xomutils.get_container_from_env() == container:
            is_stored = True
            print("same container and environ,enmt", container)
            # XOM standard way to load context for data availability consistency
            for data_type in required_type:
                is_stored = is_stored & st.is_stored(runid_str,data_type)
                return is_stored
        else:
            is_stored = False
            raise Exception("the container XOM is run into DOES NOT MATCH the expected one from the data")
        
    def get_list(self, dbtolist):        
        '''
        return a list from a data base for a specific analysis
        '''
        df = dbtolist.df_query_analysis(self.analysis.analysis_name)
        return df

    def submit_job(self, runid):
        '''
        wrapper around the utilix submit_job method. 
        create the 
        '''
        sbatch_filename = self.base_file_name + "_" + str(runid)
        code_folder = "cd " + self.analysis.folder + " \n"            
        command = "python " +  self.analysis.command 
        analysis_command = code_folder + command.replace('[run]',str(runid).zfill(6)) 
        result_folder = self.tool_config['result_folder']
        analysis_command += " " + result_folder
        analysis_command += (" --prefix "+ self.prefix)
        
        if self.analysis.mem_per_cpu:
            mem_per_cpu = int(self.analysis.mem_per_cpu)
        else:
            mem_per_cpu = 4000
        log_filename = self.tool_config['job_folder'] + self.prefix + self.analysis.analysis_name + "_" + str(runid) +'.log'
                
        print(f'data for run {runid} is available, will submit the job {sbatch_filename}')
        # utilix job submission:
        submit_job(jobstring= analysis_command,
                   log= self.get_logfile_name(runid),
                   partition = self.tool_config['job_partition'],
                   qos = self.tool_config['job_partition'],
                   jobname = 'xom_job',
                   sbatch_file = sbatch_filename,
                   dry_run=False,
                   mem_per_cpu=mem_per_cpu,
                   container=xomutils.get_container(runid),
                   cpus_per_task=1,
                   hours=None,
                   node=None,
                   exclude_nodes=None,
               )
        
        self.logger.info(f"submitted JOB {sbatch_filename}")

    def get_logfile_name(self, runid):
        logfile_name = self.base_file_name + "_" + str(runid) + '.log'
        return logfile_name

    def is_success(self, runid):        
        '''
        test of log file: look for success message
        '''
        success = False
        try: 
            filename = self.get_logfile_name(runid)
            if xomutils.search_in_file(filename,"SUCCESSWITHXOM"):
                success = True
            return success
        except Exception as error: 
            print("An exception occurred in is_success:", error) 
            return success

    def is_failure(self, runid):        
        '''
        test of log file: look for error message
        '''
        failure = False
        try: 
            filename = self.get_logfile_name(runid)
            if xomutils.search_in_file(filename,"ERRORWITHXOM"):
                failure = True
                return failure
        except Exception as error: 
            print("An exception occurred in is_failure:", error) 
            return failure


    def save_data_in_db(self, runid):
        ''' 
        add the DAQ comments, DAQ tags and run mode in the result dictionnary before saving it into the database
        
        ''' 
        query = {'number':runid}
        cursor = coll.find(query, {'number': 1, 'mode': 1, 'tags.name':1, 'comments.comment':1})
        name_of_variable = 'number'
        daq_info = {}

        # add the additional informatio: comment, tags, runmode
        try:
            comments = []
            tags = []
            if cursor[0].get('comments'):
                comments = [cursor[0]['comments'][i]['comment'] for i in range( len(cursor[0]['comments']) )]
            if cursor[0].get('tags'):
                tags = [ cursor[0]['tags'][i]['name'] for i in range( len(cursor[0]['tags']) ) ]
            if cursor[0].get('mode'):
                run_mode = cursor[0]['mode']
        except Exception as error:
            print('an execption occured in save data in db', error)

        # look for specific Science Run tag, load the result from the JSON file and save the result in the database
        try:
            for variable_name in self.analysis.variable_list:
                sr_tag = "no_sr_tag" 
                for tag in tags:
                    if '_sr' in tag:
                        sr_tag = tag 
                fname = xomlibutils.construct_filename(self.prefix + 'xomdata',self.analysis.analysis_name, self.analysis.analysis_version, variable_name,runid)
                print("JSON file name: ", fname)
                    
                res_dict = xomutils.load_result_json(self.tool_config['result_folder'] + fname)
                res_dict.update({'daq_comment': '_'.join(comments), 'daq_tags': tags,'container': xomutils.get_container(runid), 'run_mode':run_mode, 'sr_tag': sr_tag})
                xom_res = xomlib.Xomresult(res_dict)                
                xom_res.save()
        except Exception as error:
            # handle the exception
            print("An exception occurred in save in db:", error) 
            



    def get_delta_days(self, runid):
        query = {'number': runid}
        cursor = coll.find(query, {'number': 1, 'rate': 1, 'start': 1, 'end':1})                      
        date = cursor[0].get('end')
        
        now = datetime.datetime.now(pytz.timezone(self.tool_config['computing_timezone']))

        return (date - now).days
        
