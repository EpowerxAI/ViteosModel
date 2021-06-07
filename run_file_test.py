# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 19:11:10 2021

@author: consultant138
"""

import os
os.chdir('D:\\ViteosModel')
#from subprocess import Popen

#Popen('file_1.py')
exec(open('D:\\ViteosModel\\file_1.py').read())

filepath_to_read_ReconDF_from = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(1) + '.csv'
ReconDF = pd.read_csv(filepath_to_read_ReconDF_from)

while_loop_iterator = 0

while True:
	exec(open('D:\\ViteosModel\\file_2.py').read())
    if len(stout_list) > 1:
        while_loop_iterator = while_loop_iterator + 1
		exec(open('D:\\ViteosModel\\file_3.py').read())
        for z in range(ReconDF.shape[0]):
		
	