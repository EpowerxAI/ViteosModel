# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 18:48:49 2020

@author: consultant138
"""

#import difflib
#import os
#import sys
#os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')
#with open('Final Testing on Unseen data -Soros - Phase 2 (OTM, MTM) (1).py') as f1:
#    f1_text = f1.read()
#os.chdir('D:\\ViteosModel')
#with open('OakTree_uat_pipeline_AuditTrail.py') as f2:
#    f2_text = f2.read()
## Find and print the diff:
#orig_stdout = sys.stdout
#f = open('Difference_between_soros_and_oaktree_model_2_files_output.txt', 'w')
#sys.stdout = f
#
#for line in difflib.unified_diff(f1_text, \
#                                 f2_text, \
#                                 fromfile='D:\\ViteosModel\\OakTree - Pratik Code\\Final Testing on Unseen data -Soros - Phase 2 (OTM, MTM) (1).py',\
#                                 tofile='D:\\ViteosModel\\OakTree_uat_pipeline_AuditTrail.py', lineterm=''):
#    print(line)
#sys.stdout = orig_stdout
#f.close()


with open('D:\\ViteosModel\\OakTree - Pratik Code\\Final Testing on Unseen data -Soros - Phase 2 (OTM, MTM) (1).py', 'r') as file1:
    with open('D:\\ViteosModel\\OakTree_uat_pipeline_AuditTrail.py', 'r') as file2:
        same = set(file1).difference(file2)

same.discard('\n')

with open('Difference_between_soros_and_oaktree_model_2_files_output.py', 'w') as file_out:
    for line in same:
        file_out.write(line)