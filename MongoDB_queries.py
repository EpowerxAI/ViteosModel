# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 16:38:02 2020

@author: consultant138
"""

class MongoQueries_Class:
    def __init__(self):
        pass
    
    def get_unique_status_numbers(self, param_collection):
        query_result = param_collection.aggregate([
                                                  { '$group': 
                                                             {'_id': 
                                                                    {
                                                                            'Status': '$ViewData.Status',
                                                                            'BreakID': '$ViewData.BreakID'
                                                                    },
                                                              'count': { '$sum': 1 }
                                                 }}
                                                  ],  allowDiskUse =  True  )
        return query_result
    
    
    