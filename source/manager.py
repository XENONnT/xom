#!/usr/bin/env python
import os
import sys
from argparse import ArgumentParser
import datetime
import pytz
import logging


import xomlib.xomlibutils as xomlibutils
import xomlib.analysis as analysis
import xomlib

st = xomlibutils.load_straxen()            

def main():
    print()
    print("--------------------------------------")
    print("XOM BACKEND PROCESS RUNNER module     ")
    print("--------------------------------------")
    print()

    parser = ArgumentParser("manager")
    parser.add_argument("--config",type=str, help="path of the config file")
    args = parser.parse_args()
    config_file = args.config

    tool_config = xomlibutils.read_json(config_file)

    # connect to databases:
    dbs = {}
#    for dbsuffix in ['todo','done','submitted','tocheck','data',]:
    for dbsuffix in ['todo','done','submitted','tocheck']:
        dbname = tool_config['prefix'] + "xom" + dbsuffix        
        dbs[dbsuffix] = xomlibutils.connect_db(dbname)
     
    logger = xomlibutils.get_logger('manager', tool_config['log_folder'])
    logger.info("##################################################")
    logger.info("########### starting XOM MANAGER #################") 
    logger.info("##################################################")
    logger.info(">>> will work with XOM measurement names:")
    logger.info(f"     for jobs to do :{dbs['todo'].measurement_name}")
    logger.info(f"     for submitted jobs :{dbs['submitted'].measurement_name}")
    logger.info(f"     for done jobs :{dbs['done'].measurement_name}")
    logger.info(f"     for jobs to check:{dbs['tocheck'].measurement_name}")



    ana_config = xomlibutils.read_json(tool_config['configname'])
    

    analysis_list = []
    # configuration of the analysis:
    for ana in ana_config:
        an = analysis.Analysis(ana, tool_config)
        logger.info(f"analysis setup: {an.config.get('analysis_name')}")
        analysis_list.append(an)


    ###################################
    ### looping over the todo lists ###
    ###################################
    
    # query the todo list:
    for an in analysis_list:
        # produce the TODO list in the TODO data base
        logger.info("          ##################################")
        logger.info(f"######### ANALYSIS: {an.config.get('analysis_name')} #########################")
        logger.info("          ##################################")
        an.make_todo_list(dbs.values())
        
        # collect to do items from the database:
        todo_list = an.get_list(dbs['todo'])
        

        # treat the TODO entries
        # if data is available submit the job if not wait or put in TOCHECK db
        entries_to_treat = 10
        for index, row in todo_list[:entries_to_treat].iterrows(): # treating the first 5 entries then going to the next analysis (important in case of production mostly)
            runid = row['runid']
            try:
                # verify that the limit of nr of job is not reached 
                xomlibutils.wait_for_slot("xom_job", jobs_limit=tool_config['jobslimit']) 
                logger.info(f"In TODO list, Analysis {row['analysis_name']} -- RUNID {row['runid']}: treating this rund")
                    
 
                ############# data availability check ######################
                if an.is_datatype_available(int(runid), st): # >> data is available
                    print('data is availbale')
                    # construct the job submission:
                    an.submit_job(int(runid), dry_run=False)
                    dbs['todo'].delete_record(row)
                    logger.info(f"In TODO list, Analysis {row['analysis_name']} -- RUNID {row['runid']}: data is available, jobs was submitted")
                    an.fill_db([runid], 'submitted')

                else: # >>  data is not available
    #                 logger.info(f">>> data is NOT available for run ID: {runid} for analysis {an.config.get('analysis_name')}" )
                    date_daq = xomlibutils.get_daq_run_ts(runid)
                    delta_t = xomlibutils.get_delta_days(date_daq, tool_config['computing_timezone']) 
                    # if still no data after 2 days the entry is removed from the to do list and put in the tocheck list. 
                    if delta_t < -tool_config['ndays_before_removal']:
                        an.fill_db([runid], 'tocheck',tag='unavailable')
                        logger.info(f"In TODO list, Analysis {row['analysis_name']} -- RUNID {row['runid']}: is older than {tool_config['ndays_before_removal']} days, todo entry was removed")
                    else:
                        pass

            except Exception as error:
                # handle the exception
                logger.warning(f"An exception occurred in the TODO part: {error}")
        
        
    #######################################
    ### looping over the submitted list ###
    #######################################

    #    query the submitted list:
    for an in analysis_list:
        logger.info("          ##################################")
        logger.info(f"######### ANALYSIS: {an.config.get('analysis_name')} #########################")
        logger.info("          ##################################")
        df_submitted = an.get_list(dbs['submitted'])
        logger.info(f"Analysis {an.config.get('analysis_name')} -- size of SUBMITTED DB: {len(df_submitted)} entries")
        # check the status of the submitted entries:
        entries_to_treat = 100
        for index, row in df_submitted[:entries_to_treat].iterrows():
            runid = row['runid']
            try:
                log_filename = an.get_log_filename(runid)
                if xomlibutils.search_in_file(log_filename, "SUCCESSWITHXOM"): # if "SUCCESSWITHXOM" is found in the log
                    logger.info(f"Analysis {row['analysis_name']} -- RUNID {row['runid']}: SUCCESS of JOB")
                    # saves the done entry in the DONE DB
                    an.fill_db([runid],'done',tag='done')
                    # adds the DAQ comments to the result file written by the analysis and saves in the DATA DB
                    an.save_data_in_db(runid) 
                    # delete the entry in the SUBMITTED DB
                    dbs['submitted'].delete_record(row)
                    
                elif xomlibutils.search_in_file(log_filename, "ERRORWITHXOM"): # if "ERRORWITHXOM" is found in the log
                    logger.info(f"Analysis {row['analysis_name']} -- RUNID {row['runid']}: FAILURE of JOB")
                    # saves the done entry in the DONE DB with the tag done_failed
                    an.fill_db([runid],'done',tag='done_failed')
                    # delete the entry in the SUBMITTED DB
                    dbs['submitted'].delete_record(row)
                    # neither success of failure, job might be stuck somewhere for no know reason --> to be checked 
                else: 
                    date_sub = row['_time']
                    delta_t = xomlibutils.get_delta_days(date_sub, tool_config['computing_timezone']) 
                    if delta_t  <  -tool_config['ndays_before_removal']:
                        an.fill_db([runid],'done',tag='unfinished_job')
                        dbs['submitted'].delete_record(row)
                        logger.info(f"Analysis {row['analysis_name']} -- RUNID {row['runid']}: Job not finished longer than {tool_config['ndays_before_removal']} days, will be moved to tocheck data base")
                    else:
                        logger.info(f"Analysis {row['analysis_name']} -- RUNID {row['runid']}: JOB not finished, will check next time")
                        
            except Exception as error:
            # handle the exception
                logger.warning(f"An exception occurred in the SUBMITTED part: {error}") 


    logger.info("#########------- STATUS OF DB --------- ############" )
    for db in dbs.values():
        logger.info(f">> Entries in DB {db.measurement_name}")
        for an in analysis_list:            
            entries = len(db.df_query_analysis(an.config.get('analysis_name'), an.config.get('analysis_version'), xomlib.__version__)) 
            logger.info(f'{an.config.get("analysis_name")}: {entries}')
    logger.info("############## end of manager #########################" )
    

if __name__ == "__main__":
    main()
