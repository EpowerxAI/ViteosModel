# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 14:39:33 2020

@author: consultant138
"""

import pandas as pd

class Write_Class:
    def __init__(self):
        pass
    
    def write_pd_df_to_json(self, param_path_to_csv, param_path_of_writing_df_to_json):
        df = pd.read_csv(param_path_to_csv, header = None)
        df.to_json(param_path_of_writing_df_to_json)
    
