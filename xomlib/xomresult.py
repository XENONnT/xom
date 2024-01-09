#needed for xom_saver:
import os
from doctest import run_docstring_examples
import sys
import json
#from bson.json_util import dumps
import xomlib.constant as constant
import xomlib
import datetime
## figure folder on the machine that host the database and display (grafana)
#display_fig_folder = "/home/xom/data/v1.1/test/." 
import logging


import xomlib.dblib as dbl
import xomlib.xomlibutils as xomlibutils

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatterch = logging.Formatter('%(name)-20s - %(levelname)-5s - %(message)s')
ch.setFormatter(formatterch)



success_message = "SUCCESSWITHXOM"
error_message = "ERRORWITHXOM"
required_key = ['measurement_name', 'runid','analysis_name', 'analysis_version', 'variable_name']

#required_key = ['measurement_name', 'runid',  'data_type']
 
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    else:
        return obj
            
class Xomresult:
    def __init__(self, result_dict):
        
        '''
        analysis_name: str
        variable_name: str
        analysis_version: str
        variable_value: float
        runid: int
        container : str (name of the container)
        runids : list of runids
        timestamp : int
        data format is a dictionnary {"key":value, }
        figure_path : str
        tag:analysis specific tag
        daq_tags: array of str
        daq_comments: array of str
        '''
        self.result_dict = result_dict
        self.xom_version = xomlib.__version__
        for key in required_key:
            try:
                self.result_dict[key]
            except KeyError as e:
                if e.args[0] in required_key:
                    print(f'key {key} is NOT present in the result dictionnary')
                    return            
            # required:
        self.measurement_name = self.result_dict.get('measurement_name')
        self.runid = self.result_dict.get('runid')
        self.analysis_name = self.result_dict.get('analysis_name')
        self.analysis_version = self.result_dict.get('analysis_version')
        self.variable_name = self.result_dict.get('variable_name')
        self.variable_value = self.result_dict.get('variable_value')
        if self.variable_value: self.result_dict['variable_value'] = float(self.variable_value)
        self.container = self.result_dict.get('container')
        datetime_iso = json_serial(self.result_dict.get('timestamp'))
        self.result_dict['timestamp'] = datetime_iso
        self.timestamp = datetime_iso        
 
        # optional:
        self.data = self.result_dict.get('data')
        self.figure_path = self.result_dict.get('figure_path')
        self.tag = self.result_dict.get('tag')
        self.lineage = self.result_dict.get('lineage')
        self.daq_comment = self.result_dict.get('daq_comment') # string
        self.daq_tags = self.result_dict.get('daq_tags') # list of tags: ['tag0, tag1 ...']}
        self.sr_tag = self.result_dict.get('sr_tag')
        self.run_mode = self.result_dict.get('run_mode')
        #            self.runids = self.result_dict.get('runids')

        

    def get_result_record(self):
        '''
        organise the xom result data into a influxdb record 
        
        '''
        tags = {"analysis_name":self.analysis_name, 
                "analysis_version": self.analysis_version, 
                "variable_name": self.variable_name, 
                "container": self.container,
                "xom_version": self.xom_version}
        if self.tag: tags.update({'tag':self.tag})
        if self.daq_tags:
            for i, t in enumerate(self.daq_tags) :
                tags.update({'tag'+str(i):t})
        if self.sr_tag:
            tags.update({'sr_tag':self.sr_tag})
        if self.run_mode:
            tags.update({'run_mode':self.run_mode})
        fields = {}
        fields["variable_value"] = self.variable_value
        fields["runid"] = self.runid
        if self.daq_comment: fields['daq_comment'] = self.daq_comment
#        if self.result_dict['data']: fields = self.data
        if self.data: fields.update(self.data)
        if self.figure_path: fields.update({'figure_path':self.figure_path})

        ###  warning ###
        # there is no check on the presence of the lineage 
        # in case the object is used for the data it is required to have the lineage present
        ################
        if self.lineage: fields.update(self.lineage)
        record =[
            {
                "measurement":self.measurement_name,
                "tags": tags,
                "fields":fields,
                "time": self.timestamp
            }
        ]
        return record
                            
    def write_json(self, folder):
        ''' 
        write the result in the result folder 
        This step is used for the data.
        '''
        fname = xomlibutils.construct_filename(self.measurement_name, self.analysis_name, self.analysis_version, self.variable_name, self.runid)
        print("folder + fname = ", folder + fname )
        with open(folder + fname, "w") as outfile: 
            json.dump(self.result_dict, outfile)

            
    # def get_json(self):
    #     folder = constant.result_folder
    #     fname = utils.construct_filename(self)
    #     json_dict = utils.load_json(fname)
        


    def save_in_db(self, record):
        ## connect to the database:
        client = dbl.Xomdb(self.measurement_name)
        client.insert_record(record)

    def save(self):
        record = self.get_result_record()
        self.save_in_db(record)
                
                 
    def xom_message(self,success):
        if success == True: print(success_message)
        if success == False: print(error_message)



