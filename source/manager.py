#!/usr/bin/env python

import os
import time
import subprocess
import shlex
import sys
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from argparse import ArgumentParser
import glob
import time
import datetime
import pytz
from utilix.batchq import submit_job
import logging


from utilix import xent_collection
coll = xent_collection()

cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")
import xomlib.xomlibutils as xomlibutils
import xomlib.constant as constant
import xomlib.dblib as dbl
import xomlib
import xomutils as xomutils
import analysis as analysis
import runner as rn


from logging.handlers import TimedRotatingFileHandler
logger = logging.getLogger('manager')
log_format = "%(asctime)s  - %(name)s - %(levelname)s - %(message)s"
log_level = 10
logger.setLevel(log_level)
formatter = logging.Formatter(log_format)

# create file handler which logs even debug messages
#fh = logging.FileHandler(os.path.join(logdir, 'acm.log'))
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

#############################
### sets up the xomconfig ###
#############################

#tool_config = xomutils.read_json(constant.tool_config_folder + 'tool_config.json')
config_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config/")
tool_config = xomutils.read_json(config_dir + 'tool_config.json')
ana_config = xomutils.get_xom_config(tool_config['configname'])
analysis_names = ana_config.sections()


        

stop_condition = True
def main():
    print()
    print("--------------------------------------")
    print("XOM BACKEND PROCESS RUNNER module     ")
    print("--------------------------------------")
    print()

    parser = ArgumentParser("proc_runner")
    parser.add_argument("--loglevel", type=str, help="Logging level", default='INFO')
    parser.add_argument("--prefix",type=str, help="define the database name you want to write in, for instance test", default='test_')
    parser.add_argument("--skipdatacheck", help="debug purpose, skip the test of the data availability", action = 'store_true')
    args = parser.parse_args()
    loglevel = args.loglevel
    prefix = args.prefix
    skipdatacheck = args.skipdatacheck
    # ch = logging.StreamHandler(sys.stdout)
    # ch.setLevel(loglevel.upper())
    # # create formatter and add it to the handlers
    # formatterch = logging.Formatter('%(name)-20s - %(levelname)-5s - %(message)s')
    # ch.setFormatter(formatterch)
    # # add the handlers to the logger
    # logger.addHandler(ch)

    [xomdb,xomdbtodo,xomdbdone,xomdbsubmitted,xomdbtocheck] = xomlibutils.connect_dbs(prefix)
    logger.info("##################################################")
    logger.info("########### starting XOM MANAGER #################") 
    logger.info("##################################################")
    logger.info(">>> will work with XOM measurement names:")
    logger.info(f"     for jobs to do :{xomdbtodo.measurement_name}")
    logger.info(f"     for submitted jobs :{xomdbsubmitted.measurement_name}")
    logger.info(f"     for done jobs :{xomdbdone.measurement_name}")
    logger.info(f"     for jobs to check:{xomdbtocheck.measurement_name}")
    logger.info(f"     for data :{xomdb.measurement_name}")

    analysis_list = []
    runner_list = []
    for analysis_name in analysis_names:
        an = analysis.Analysis(analysis_name, loglevel)
        logger.info(f"analysis setup: {an.analysis_name}")
        an.fill_from_config(ana_config)
        analysis_list.append(an)
        run = rn.Runner(an, prefix)
        runner_list.append(run)

    count = 0
    todo_b = True
    submitted_b = True

    #    while(todo_b | submitted_b):
    todo_b = False
    submitted_b = False
    ###################################
    ### looping over the todo lists ###
    ###################################
    
    # query the todo list:
    for an, runner in zip(analysis_list, runner_list):
        # dictionnary for the xom results
        base_dict = {'analysis_name': an.analysis_name,
                     'analysis_version': an.analysis_version, 
                 }

        # produce the todo list in the todo data base
        runner.make_todo_list([xomdb,xomdbtodo,xomdbdone,xomdbsubmitted,xomdbtocheck])
            
        todo = runner.get_list(xomdbtodo)
        logger.info(f"size of todo for analysis {an.analysis_name}: {len(todo)} entries")
        try:
            for index, row in todo[:100].iterrows(): # treating the first 5 entries then going to the next analysis (important in case of production mostly)
                base_dict.update({
                    'runid':row['runid'], 
                    'variable_name': row['variable_name'],
                    'container': row['container'],
                    'timestamp':datetime.datetime.now(pytz.timezone(tool_config['computing_timezone']))
                })
                xomutils.wait_for_slot("xom_job", jobs_limit=tool_config['jobslimit']) 
                runid = row['runid']
                logger.info(f"in TODO list, treating run ID: {runid} for analysis {an.analysis_name}" )
                    

                ############# data availability check ######################
                if runner.is_available(runid): # >> data is available
                    # construct the job submission:
                    runner.submit_job(runid)                 
                    # built the dictionnary for the submitted entry
                    submitted_dict = base_dict                    
                    submitted_dict['measurement_name'] = xomdbsubmitted.measurement_name
                    submitted_dict['tag'] ='submitted'
                    submitted_result = xomlib.Xomresult(submitted_dict)
                    submitted_result.save()
                    # remove the todo entry
                    xomdbtodo.delete_record(row)
                    logger.info(f">>> data is available, jobs was submitted, todo entry removed for run ID: {runid} for analysis {an.analysis_name}" )
                        

                else: # >>  data is not available
                    logger.info(f">>> data is NOT available for run ID: {runid} for analysis {an.analysis_name}" )
                    if runner.get_delta_days(runid) < -tool_config['ndays_before_removal']: # if still no data after 2 days the entry is removed from the to do list and put in the tocheck list. 
                        unavail_dict = base_dict
                        unavail_dict['tag'] ='data_unavailable'
                        unavail_dict['measurement_name'] = xomdbtocheck.measurement_name
                        unavail_result = xomlib.Xomresult(unavail_dict)
                        unavail_result.save()
                        xomdbtodo.delete_record(row)
                        logger.info(f">>> run ID: {runid} is older than {ndays_before_removal} days, todo entry was removed for analysis {an.analysis_name}" )
                            
                            
        except Exception as error:
                # handle the exception
                logger.warning(f"An exception occurred in the TODO part: {error}")
        


    #######################################
    ### looping over the submitted list ###
    #######################################

    # query the submitted list:
    for an, runner in zip(analysis_list, runner_list):
        base_dict = {'analysis_name': an.analysis_name,
                     'analysis_version': an.analysis_version, 
                 }
        df_submitted = runner.get_list(xomdbsubmitted)
        logger.info(f"size of submitted for analysis {an.analysis_name}: {len(df_submitted)} entries")
        now = datetime.datetime.now(pytz.timezone(tool_config['computing_timezone']))
        try:
            # check the status of the submitted entries:
            for index, row in df_submitted.iterrows():
                logger.info(f"in SUBMITTED list, treating run ID: {row['runid']} for analysis {an.analysis_name}" )
                base_dict.update({
                    'runid':row['runid'], 
                    'variable_name': row['variable_name'],
                    'container': row['container'],
                    'timestamp': now
                })
                
                done_dict = base_dict
                done_dict['measurement_name'] = xomdbdone.measurement_name
                if runner.is_success(row['runid']):
                    logger.info(f"SUCCESS of JOB for Analysis {row['analysis_name']} and RUNID {row['runid']}")
                    done_dict['tag'] = 'done'
                    done_result = xomlib.Xomresult(done_dict) 
                    done_result.save()
                    runner.save_data_in_db(row['runid'])
                    xomdbsubmitted.delete_record(row)                    
                elif runner.is_failure(row['runid']):
                    logger.info(f"FAILURE of JOB for Analysis {row['analysis_name']} and RUNID {row['runid']}")                    
                    done_dict['tag'] = 'done_failed'
                    done_result = xomlib.Xomresult(done_dict) 
                    done_result.save()
                    xomdbsubmitted.delete_record(row)
                else:
                    print("Neither success or failure, job is maybe not finished")
                    if (row['_time'] - now).days < -tool_config['ndays_before_removal']:
                        unfinished_dict = base_dict 
                        unfinished_dict.update({'measurement_name': xomdbtocheck.measurement_name, 'tag':'unfinished_job'})
                        unfinished_result = xomlib.Xomresult(unfinished_dict)
                        unfinished_result.save()
                        xomdbsubmitted.delete_record(row)
                        logger.info(f"Job for Analysis {row['analysis_name']} and RUNID {row['runid']} is submitted longer than {tool_config['ndays_before_removal']} days, will be moved to tocheck data base")
                        
        except Exception as error:
            # handle the exception
            logger.warning(f"An exception occurred in the SUBMITTED part: {error}") 

if __name__ == "__main__":
    main()
                
