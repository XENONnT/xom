#!/usr/bin/env python
import os
import sys
import pprint 
import numpy as np
import time
import xomlib.constant as constant
import xomlib.info as info
import datetime
from dateutil.tz import tzutc
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatterch = logging.Formatter('%(name)-20s - %(levelname)-5s - %(message)s')
ch.setFormatter(formatterch)

########################
### connection to DB ###
########################


class Xomdb:
    def __init__(self, measurement_name):
        self.client  = None
        self.measurement_name = measurement_name
        self.connect()
        self.logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        self.logger.debug(f"creating instance of {self.__class__}")
        # add the handlers to the logger
        self.logger.addHandler(ch)


    def connect(self):
        try:
            client = influxdb_client.InfluxDBClient(
                url=info.url,
                token=info.token,
                org=info.org
            )   
        except:
            self.logger.error("could not connect to the DB")
            return 0
        self.client = client

    
    def get_last_runid(self):
        query_api = self.client.query_api()
        fluxquery = 'from(bucket:"xom")  |> range(start: '+ str(-constant.query_period) + 'd) |> filter(fn: (r) => r._measurement == \"' +self.measurement_name + '\")|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") |>group()|> max(column:"runid")'
        try:
            tables = query_api.query(fluxquery)
            self.last_run_id = tables[0].records[0]['runid']
        except:
            self.last_run_id = -1
        return self.last_run_id
 
    def get_last_runid_from_analysis(self, analysis_name,analysis_version="v0.0"):
        '''will query the latest runid'''
        query_api = self.client.query_api()

        df = query_api.query_data_frame('from(bucket:"xom") '
                                '|> range(start: -100d) '
                                '|> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
                                '|> filter(fn: (r) => r.analysis_name == \"'+ analysis_name + '\")'
                                '|> filter(fn: (r) => r.analysis_version == \"'+ analysis_version + '\")'
                                '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
                                '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_value", "variable_error","analysis_name","analysis_version","runid","container"])')
        if len(df) > 0:
            last_run_id = int(np.max(df['runid']))
        else:
            last_run_id = -1
        
        return last_run_id

    def get_runid_list(self, analysis_name, analysis_version="v0.0", xom_version=None):
        '''will query the latest runid'''
        query_api = self.client.query_api()
        df = self.df_query_analysis(analysis_name, analysis_version, xom_version)
    
        if len(df) > 0:
            run_id_list = np.unique(df['runid'])
        else:
            run_id_list = []
        
        return run_id_list

            

    def insert_record(self, record):
        '''
        record or list of record
        '''
        write_api = self.client.write_api(write_options=SYNCHRONOUS)    
        if isinstance(record,list):
            for r in record:
                write_api.write(bucket="xom", org=self.client.org, record=r)
        else:
            write_api.write(bucket="xom", org=self.client.org, record=record)

    def df_insert_record(self, df, tag_columns):
        '''
        record or list of record
        '''
        write_api = self.client.write_api(write_options=SYNCHRONOUS)    
        measurement = df['_measurement'].values[0]
        write_api.write(bucket="xom", org=self.client.org, record=df, data_frame_measurement_name=measurement,
                        data_frame_tag_columns=tag_columns)
 

    def df_query_all(self):
        query_api = self.client.query_api()
        df = query_api.query_data_frame('from(bucket:"xom") '
                                '|> range(start: -100d) '
                                '|> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
                                '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
                                '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_value","variable_name", "variable_error","analysis_name","analysis_version","runid","container"])')

        return df

    def df_query_analysis(self, analysis_name, analysis_version=None, xom_version=None):
        '''
        returns the data frame associated to the measurement and the analysis 
        (for all version)
        
        '''
        query = 'from(bucket:"xom") |> range(start: -100d) |> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
        if xom_version == None: xom_version = self.get_latest_xom_version()
        if xom_version: query+= '|> filter(fn: (r) => r.xom_version == \"' + xom_version + '\")'
        query+= '|> filter(fn: (r) => r.analysis_name == \"'+ analysis_name + '\")'
        if analysis_version: query+= '|> filter(fn: (r) => r.analysis_version == \"'+ analysis_version + '\")'
        query += '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
        query += '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_name","variable_value", "variable_error","analysis_name","analysis_version","runid","container"])'
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)

        return df
        

    def delete(self):
        start = "1970-01-01T00:00:00Z"
        stop = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        delete_api = self.client.delete_api()
        print("deleting measurement ",  self.measurement_name)
        delete_api.delete(start, stop,'_measurement=\"' + self.measurement_name + '\"', bucket='xom')

    def delete_record(self, p):
        d1 = datetime.timedelta(microseconds=-1)
        d2 = datetime.timedelta(microseconds=+1)
        start = p['_time'] + d1
        stop = p['_time'] + d2
        delete_api = self.client.delete_api()
        delete_api.delete(start, stop,'_measurement=\"' + self.measurement_name + '\"', bucket='xom')

    
    def delete_runid(self, analysis_name, analysis_version,  runid,  xom_version=None):
        ''' 
        delete the records of one specific analysis for a given runid
        '''
        query_api = self.client.query_api()

        df = self.df_query_analysis(analysis_name, analysis_version, xom_version)

        df = df.query("runid ==" + str(runid))
        for index, row  in df.iterrows():
            print('>>>>>>>>> deleting :')
            print(row)
            self.delete_record(row)
            
    def get_latest_xom_version(self):
        query_api = self.client.query_api()
        print("self.measurement_name = ",  self.measurement_name)
        df = query_api.query_data_frame('from(bucket:"xom") '
                                        '|> range(start: -100d) '
                                        '|> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
                                        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
                                        '|> keep(columns: ["_start","_stop","_time","_measurement","_value","xom_version"])')
        if df.size:
            latest_xom_version = df['xom_version'].max()
            return latest_xom_version
        else: 
            pass
        
