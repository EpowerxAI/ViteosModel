# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 06:34:54 2021

@author: riteshkumar.patra
"""

class ViteosLogger_Class:
    def __init__(self):
        return 0
    
#    def log_to_file(param_filename, param_log_str, param_except_str = 'Logging failed'):
#        try:
#            f = open(param_filename, "w")
#            try:
#                f.write(param_log_str)
#            finally:
#                f.close()
#        except IOError:
#            print(param_except_str)
        
#    def log_to_file(param_filename, param_log_str, param_except_str = 'Logging failed'):
#        try:
#            with open(param_filename, 'r+') as f:
#                fun_existing_content = f.read()
#                f.seek(0, 0)
#                f.write(param_log_str)
#                f.write('\n')
#                f.write(fun_existing_content)
#
##            f = open(param_filename, "w")
##            f.write(param_log_str)
##            f.close()
#        except IOError:
#            print(param_except_str)

    def log_to_file(param_filename, param_log_str, param_except_str = 'Logging failed'):
        with open(param_filename, 'a+') as f:
            f.seek(0)
            data = f.read(100)
            if len(data) > 0 :
                f.write("\n")
            # Append text at the end of file
            f.write(param_log_str)

