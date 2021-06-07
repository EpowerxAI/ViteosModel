#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 12:09:45 2018

@author: shweta
"""

#!/usr/bin/env python

dbconnection = {
        'meo_connection_host': '192.168.170.158:27017',
        'aua_connection_host': '192.168.170.158:27017',
        'pred_connection_host': '192.168.170.158:27017'
        #'aua_connection_host': 'localhost:27017',
}


dbname = { 
        'meo_db':'ReconDB_14_02',
        'aua_db':'ReconDB_14_02',
        'pred_db':'ReconDB_14_02'
}

collection_names = { 
        'collection_meo':'MEODataForPredictionCash_AMIA',
        'collection_aua_Cash':'AfterUserActionDataForTrainingCash',
        'collection_aua_Position':'BreakManagementOutput_Position',
        'collection_aua_Trade':'BreakManagementOutput_Trade',

	'collection_to_save_predictions_cash': 'predictions_cash'
}

model_save_path = "./"
