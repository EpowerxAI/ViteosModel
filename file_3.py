# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 19:57:30 2021

@author: consultant138
"""

smallerlist = [l.split(',') for l in ','.join(stout_list).split('\n')]
        
ReconDF = DataFrame.from_records(smallerlist)
ReconDF = ReconDF.dropna(how='any',axis=0)
#        ReconDF.columns = ['TaskID', 'csc','ReconPurpose','collection_meo','RequestId']
ReconDF.columns = ['TaskID', 'csc','ReconPurpose','collection_meo','ProcessID','Completed_Status','Setup_Code','MongoDB_TaskID']
        
ReconDF['TaskID'] = ReconDF['TaskID'].str.lstrip("b'")
ReconDF['ProcessID'] = ReconDF['ProcessID'].str.replace(r"[^0-9]"," ")
ReconDF['Setup_Code'] = ReconDF['Setup_Code'].str.rstrip("'\r")
ReconDF['MongoDB_TaskID'] = ReconDF['MongoDB_TaskID'].str.rstrip("'\r")
        
    
print('ReconDF')
print(ReconDF)
ReconDF_filepath = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(while_loop_iterator) + '.csv'
ReconDF.to_csv(ReconDF_filepath)
