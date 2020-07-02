# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 09:07:22 2020

@author: consultant138
"""

from RabbitMQ import RabbitMQ_Class as rb_mq

#For Testing with reading messages from a dataframe instead of receiving messages from RabbitMQ directly
# Execute in a separate console - Console 1
rb_mq_obj = rb_mq(param_test_message_publishing = True)
rb_mq_obj.fun_publish_muliple_messages()
