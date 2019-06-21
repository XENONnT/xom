import os
import sys
import numpy as np 
import pandas as pd
from tqdm import tqdm
from glob import glob
import straxen 
import warnings


class GetDataFrame(object):
    
    """this class uses straxen to read a give data sets (like: calibration Kr) 
    and returns back a panda data frame for a given run_number or a run_name
    - you need to give the proper plugins in order to get the variables needed
    - you need to specify which data_type you have.
    - In the future, it will be wise to add in the run data base, data_stream which will decide if we are dealing with DM or calibration
    
    """

    def __init__(self, run_number = None , run_name = None, plugins =['event_info'], data_stream = "calibration", data_type = "kr"):
        

        
        self.run_number = run_number
        self.run_name   = run_name
        self.plugins = plugins
        self.data_stream = data_stream
        self.data_type = data_type
        
    def get_df(self):
        
        """ 
        Get the data frame corresponding to the run_number/run_name and processed it with straxen        
        """
        if (self.run_number != None) and (self.run_name != None) :
            
            # This solution is temporary: once we start taking data with XENONnT this needs to be changed
            warnings.warn("THIS NEEDS TO BE LOOKED AT ONCE XENONnT STARTS TAKING DATA")
            try:
                # load all contexts from straxen, this is also temporary
                st = straxen.contexts.strax_workshop_dali()

                # get the data sets, here we are interested in Kr data and only one run
                dsets = st.select_runs(run_mode=self.data_type + "*")
                mask_name = dsets["name"].values == self.run_name
                df = st.get_df(dsets["name"][mask_name], self.plugins)
                
                # here also it is another temporary addition: s2_bottom
                # It does not exist yet in this testing mode
                df["s2_bottom"] = pd.Series((1-df.s2_area_fraction_top)*df.s2_area, index=df.index)

                return df
            
            except Exception as err:
                print(" the errro {}".format(err) )
                 
                return None
            
        else:
            warnings.warn(" you did not Specify the run number and run name")
            print("the class GetDataFrame is going to quit here")
            sys.exit(0)
        
