# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 18:52:21 2020

@author: consultant138
"""

import os
os.chdir('D:\\ViteosModel\\ML2_Phase2')

import json

class ML2:
    def __init__(self, param_setup_code : str = 'None'):
        self.setup_code = param_setup_code.strip()
        self.columns_json_path = 'Setup_' + self.setup_code + '\\columns_json.json'
        self.add_columns_json_path = 'Setup_' + self.setup_code + '\\add_columns_json.json'
        
    class Read_Class(object):
        def __init__(self):
            
            self.ML2_outer_class = ML2() 
            
            with open(self.ML2_outer_class.columns_json_path) as infile:
                self.columns_from_json = json.load(infile)
		
            with open(self.ML2_outer_class.add_columns_json_path) as infile:
                self.add_columns_from_json = json.load(infile)
            
        def print_columns_from_json(self):
            print(self.columns_from_json)