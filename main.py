# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 09:01:10 2020

@author: consultant138
"""

from RabbitMQ import RabbitMQ_Class as rb_mq

#For Testing with reading messages from a dataframe instead of receiving messages from RabbitMQ directly
# Execute in a separate console - Console 2
rb_mq_obj = rb_mq()
#print(rb_mq_obj.df_test_message_to_RabbitMQ.shape[0])
