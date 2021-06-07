# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 16:31:32 2020

@author: consultant138
"""
import sshtunnel
import pymongo
import logging
import sys
sys.path.append('..')
import pandas as pd
from Read import Read_Class as rd_cl
from pandas.io.json import json_normalize
from ViteosDecorator import logging_decorator
from RabbitMQ import RabbitMQ_Class as rbmq_cl

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class ViteosModel_changes_Class :

    
    @logging_decorator
    def __init__(self, param_meo_df, param_comparison_df, param_aua_df):

        self.meo_df = param_meo_df
        self.comparison_df = param_comparison_df
        self.aua_df = param_aua_df
    
    @logging_decorator
    def 