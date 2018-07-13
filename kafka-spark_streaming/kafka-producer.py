from kafka import KafkaProducer
import random
from time import sleep
import sys, os


def kafka_producer(servers, topic,file_abs_path, timesleep):

    producer = KafkaProducer(bootstrap_servers=servers)
    file_object = open(file_abs_path ,'r')

    try:
        try:
            for line in file_object:
                print(line)
                producer.send(topic, bytes(line, 'utf8'))
                sleep(timesleep)
        finally:
            file_object.close()
        producer.flush()

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__=="__main__":
    kafka_producer(servers='localhost:9092',topic='motogp',
                   file_abs_path='data/DATASET-Twitter-23-26-Mar-2014-MotoGP-Qatar.csv',
                   timesleep=0.02)