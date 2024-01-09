from utilix.config import Config
import json
import logging
import sys
import os
import pytz
import json

from utilix import xent_collection
coll = xent_collection()


cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")


import xomlib
import xomlib.dblib as dbl
import xomlib.constant as constant

import xomutils as xomutils

from datetime import datetime
import time


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

config_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config/")
tool_config = xomutils.read_json(config_dir + 'tool_config.json')
 
class Analysis:
    def __init__(self, name, loglevel="INFO"):
        self.analysis_name = name
        self.analysis_version = ""
        self.variable_list = []
        self.container_list = []
        self.exclude_tags_list = []
        self.include_tags_list = []
        self.available_type_list = []
        self.runwise = False
        self.analysis_path = ""
        self.detectors_list = []
        self.run_mode_list = []
        self.command = ""
        self.result = None
        self.logger = logging.getLogger('manager.analysis.'+self.analysis_name)
        self.min_run = None
        self.max_run = None

        self.logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        self.logger.debug(f"creating instance of {self.__class__}")

         
    def fill_from_config(self, ana_config):
        self.analysis_version = ana_config.get(self.analysis_name,'analysis_version')
#        containerlist = ana_config.get(self.analysis_name,'container')
#        self.container_list = containerlist.split(',')
        if ana_config.has_option(self.analysis_name,'exclude_tags'):
            exclude_tags_list  = ana_config.get(self.analysis_name,'exclude_tags')
            self.exclude_tags_list = exclude_tags_list.split(',')

        if ana_config.has_option(self.analysis_name,'include_tags'):
            include_tags_list  = ana_config.get(self.analysis_name,'include_tags')
            self.include_tags_list = include_tags_list.split(',')

        if ana_config.has_option(self.analysis_name,'available_type'):
            available_type_list  = ana_config.get(self.analysis_name,'available_type')
            self.available_type_list = available_type_list.split(',')

        if ana_config.has_option(self.analysis_name,'run_mode'):
            run_mode_list  = ana_config.get(self.analysis_name,'run_mode')
            self.run_mode_list = run_mode_list.split(',')
        if ana_config.has_option(self.analysis_name,'detector'):
            detectors_list  = ana_config.get(self.analysis_name,'detector')
            self.detectors_list = detectors_list.split(',')
        else:
            self.detector_list = ['tpc']

        variablelist = ana_config.get(self.analysis_name,'variable_name')
        self.variable_list = variablelist.split(',')
        self.runwise = ana_config.getboolean(self.analysis_name,'runwise')
        self.folder = ana_config.get(self.analysis_name,'folder')
        self.command = ana_config.get(self.analysis_name,'command')
        if ana_config.has_option(self.analysis_name,'min_run'):
            self.min_run = int(ana_config.get(self.analysis_name,'min_run'))
        if ana_config.has_option(self.analysis_name,'max_run'):
            self.max_run = int(ana_config.get(self.analysis_name,'max_run'))

    def print_config(self):
        self.logger.info(f"##### Analysis: {self.analysis_name} version {self.analysis_version} ##########")
        self.logger.info(f"variable list = {self.variable_list}")
        self.logger.info(f"exclude_tags list = {self.exclude_tags_list}")
        self.logger.info(f"include_tags list = {self.include_tags_list}")
        self.logger.info(f"runwise = {self.runwise}")
        self.logger.info(f"command = {self.command}")
        self.logger.info("###################################################")



    def get_valid_runs(self, last_xom, last_daq): 
        and_query = []
        if self.detectors_list:
            detector_query =  [{"detectors": i} for i in self.detectors_list]
        else:
            detector_query = [{}]

        # list of unused run modes:
        excluded_run_modes = tool_config['excluded_run_modes']
        if self.run_mode_list:
            run_mode_query = [{"$or":[{"mode": i} for i in self.run_mode_list]}]
        else: 
            run_mode_query = [{"mode":{"$ne": e}} for  e in excluded_run_modes]
        if not self.exclude_tags_list:
            exclude_tags_query = [{}]
        else:
            exclude_tags_query = [{"tags.name":{"$ne": e}} for  e in self.exclude_tags_list]
 
        # included in or query
        if not self.include_tags_list:
            include_tags_query = [{}]
        else:
            include_tags_query =  [{"tags.name": i} for i in self.include_tags_list]
        
         
        if not self.min_run:
            min_run_query = {"number":{"$gt": last_xom}}
        else:            
            min_run = max(last_xom,self.min_run)            
            min_run_query = {"number":{"$gt": min_run}}

        if not self.max_run:
            max_run_query = {"number": {"$lte": last_daq}}
        else: 
            max_run = min(last_daq,self.max_run)            
            max_run_query = {"number": {"$lte": max_run}}
 
        finished_run_query =  {'end': {'$ne': None}}

        and_query = exclude_tags_query + detector_query + run_mode_query + [min_run_query, max_run_query] + [finished_run_query]
        or_query = include_tags_query
#        self.logger.debug(f"AND query for the DAQ DB {and_query}, OR query :{or_query}")
        coll_list = list(coll.find({"$and" : and_query,"$or": or_query } ))
        
        valid_runs = []

        [valid_runs.append(x['number']) for x in coll_list]
        return valid_runs


    def produce_todo_entry(self, r, prefix=""):
        measurement_name = prefix + 'xomtodo'
        container = xomutils.get_container(r)
        self.logger.info(f'adding entry in TODO list for runid {r} and container {container}')
        todo_dict = {
            'measurement_name':measurement_name,
            'analysis_name': self.analysis_name,
            'analysis_version': self.analysis_version,
            'variable_name': '_'.join(self.variable_list),
            'variable_value': 0,
            'timestamp': datetime.now(pytz.timezone(tool_config['computing_timezone'])),
            'runid': r,
            'container': container,
            'tag': 'todo'          
        }
        todo_result = xomlib.Xomresult(todo_dict)
        todo_result.save()


    def test_log(self):
        self.logger.info("just testing level INFO" )
        self.logger.debug("just testing level DEBUG" )
        self.logger.error("just testing level ERROR" )
     

