import pika
import os
import subprocess

connection = pika.BlockingConnection(pika.connection.URLParameters('amqp://recon2:recon2@192.168.172.4/viteos'))
channel = connection.channel()


#channel.queue_declare(queue='hello')

def add_callback_threadsafe(ch, method, properties, body):
   print("%r" % body)


timeout = 10

def on_timeout():
  global connection
  connection.close()

connection.add_timeout(timeout, on_timeout)
#connection.add_callback_threadsafe()
	
channel.basic_consume(add_callback_threadsafe,
                      queue='VNFRecon_WriteToML_DEV',
                      no_ack=True)
                     # exchange='ReconDEVExchange_ML',
                     # queue='VNFRecon_WriteToML_DEV',
                      #no_ack=True)
#connection.add_callback_threadsafe()
#print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
