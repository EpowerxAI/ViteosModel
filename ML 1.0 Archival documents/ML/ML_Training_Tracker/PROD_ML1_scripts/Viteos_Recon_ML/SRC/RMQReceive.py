import pika
import os
import subprocess

connection = pika.BlockingConnection(pika.connection.URLParameters('amqps://recon2:recon2@vitblrauth02.viteos.com/viteos'))
channel = connection.channel()


#channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
   print("%r" % body)


timeout = 10

def on_timeout():
  global connection
  connection.close()

connection.add_timeout(timeout, on_timeout)
	
channel.basic_consume(callback,
                     # exchange='ReconDEVExchange_ML',
                      queue='VNFRecon_WriteToML_PROD',
                      no_ack=True)

#print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
