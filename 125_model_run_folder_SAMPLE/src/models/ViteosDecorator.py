# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 20:27:09 2020

@author: consultant138
"""

import inspect
import logging
import time

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

def logging_decorator(fn):
    def wrapper(*args, **kwargs):
        try :
            is_method   = inspect.getargspec(fn)[0][0] == 'self'
        except :
            is_method   = False

        if is_method :
            name    = '\tModule : {} \n\tFunction Class : {} \n\tFunction Name : {}'.format(fn.__module__, args[0].__class__.__name__, fn.__name__)
        else :
            name    = '\tModule : {} \n\tFunction Name{}'.format(fn.__module__, fn.__name__)
            
            
        ts = time.time()
        LOGGER.info('START \n' + name)
        print('START \n' + name)
            
        result = fn(*args,**kwargs)
            
        te = time.time()
            
        LOGGER.info('STOP \n' + name)
        print('STOP \n' + name)
            
        LOGGER.info('Time Taken : %2.2f sec' % (te - ts))
        print('Time Taken : %2.2f sec' % (te - ts))
            
        return result
    return wrapper
