# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 14:39:33 2020

@author: consultant138
"""

import pandas as pd
import json

class Write_Class:
    def __init__(self):
        pass
    
    def write_pd_df_to_json(self, param_path_to_csv, param_path_of_writing_df_to_json):
        df = pd.read_csv(param_path_to_csv, header = None)
        df.to_json(param_path_of_writing_df_to_json)
    
    def write_dict_at_top(param_filename, param_dict_to_add):
        with open(param_filename, 'r+') as f:
            fun_existing_content = f.read()
            f.seek(0, 0)
            f.write(json.dumps(param_dict_to_add, indent = 4))
            f.write('\n')
            f.write(fun_existing_content)
