#!/usr/bin/env python
import os
import sys
import pprint 
import numpy as np
import time
import datetime
from dateutil.tz import tzutc
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

########################
### connection to DB ###
########################

import xomlib.xomlibutils as xomlibutils

class Xomdb:
    def __init__(self, measurement_name, influxdb_info=None):
        '''
        name of the influx db measurement
        dictionnary with the credential for influx db connection, if None will fetch the file ~/.xom_influxdb.json
        
        
        '''
        self.client  = None
        self.measurement_name = measurement_name
        if influxdb_info==None:
            self.influxdb_info = self.get_info()
        else:
            self.influxdb_info = influxdb_info
        self.connect()


    def get_info(self):
        try:         
            info_path = os.path.join(os.path.expanduser('~'), ".xom_influxdb.json")            
            info = xomlibutils.read_json(info_path)
        except FileNotFoundError:
            print('The file does not exist')
        return info

    def connect(self):
        try:
            client = influxdb_client.InfluxDBClient(
                url=self.influxdb_info.get('url'),
                token=self.influxdb_info.get('token'),
                org=self.influxdb_info.get('org')
            )   
        except:
            return 0
        self.client = client
        self.query_api = self.client.query_api()



    def df_query_all(self):
        df = self.query_api.query_data_frame('from(bucket:"xom") '
                                '|> range(start: -10000d) '
                                '|> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
                                '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
                                '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_value","variable_name", "variable_error","analysis_name","analysis_version","runid","container","xom_version"])')
 
        return df

    def df_query_analysis(self, analysis_name, analysis_version=None, xom_version=None):
        '''
        returns the data frame associated to the measurement and the analysis 
        (for all version)
        
        '''
        query = 'from(bucket:"xom") |> range(start: -10000d) |> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
        if xom_version == None: xom_version = self.get_latest_xom_version()
        if xom_version: query+= '|> filter(fn: (r) => r.xom_version == \"' + xom_version + '\")'
        query+= '|> filter(fn: (r) => r.analysis_name == \"'+ analysis_name + '\")'
        if analysis_version: query+= '|> filter(fn: (r) => r.analysis_version == \"'+ analysis_version + '\")'
        query += '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
        query += '|> keep(columns: ["_start","_stop","_time","_measurement","_value","variable_name","variable_value", "variable_error","analysis_name","analysis_version","runid","container","xom_version"])'
        df = self.query_api.query_data_frame(query)

        return df
        
        
 
    def get_runid_list(self, analysis_name, analysis_version="v0.0", xom_version=None):
        '''will query the latest runid'''
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
        df = self.df_query_analysis(analysis_name, analysis_version, xom_version)

        df = df.query("runid ==" + str(runid))
        for index, row  in df.iterrows():
            print('>>>>>>>>> deleting :')
            print(row)
            self.delete_record(row)
            
    def get_latest_xom_version(self):
        df = self.query_api.query_data_frame('from(bucket:"xom") '
                                '|> range(start: -10000d) '
                                '|> filter(fn: (r) => r._measurement == \"' + self.measurement_name + '\")'
                                '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") '
                                '|> keep(columns: ["_start","_stop","_time","xom_version"])')
 
        if df.size:
            latest_xom_version = df['xom_version'].max()
            return latest_xom_version
        else: 
            pass
        

