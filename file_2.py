# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 19:28:55 2021

@author: consultant138
"""
try:
    s2_out = subprocess.check_output([sys.executable, 'D:/ViteosModel/Setup_' + setup_code + '_ML2_RMQ_Receive.py'])
except Exception:
    data = None
    
#Note that message from .Net code is as follows:
#string message = string.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}", task.InstanceID, task.ReconSetupForTask.Client.ClientShortCode, task.ReconSetupForTask.ReconPurpose.ReconPurpose, ReconciliationDataRepository.GetReconDataCollection(task.ReconSetupCode), processID, "Recon Run Completed", task.ReconSetupCode) 
       
    # Decoding the output of rabbit MQ message
s2_stout=str(s2_out, 'utf-8')
stout_list = s2_stout.split("|")

print (stout_list)
