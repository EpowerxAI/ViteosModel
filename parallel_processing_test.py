# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 12:48:44 2021

@author: consultant138
"""

import multiprocessing 
import time 
  
  
class Process(multiprocessing.Process): 
    def __init__(self, id): 
        super(Process, self).__init__() 
        self.id = id
                 
    def run(self): 
        time.sleep(1) 
        print("I'm the process with id: {}".format(self.id)) 
  
if __name__ == '__main__': 
    p = Process(0) 
    p.start() 
    p.join() 
    p = Process(1) 
    p.start() 
    p.join() 