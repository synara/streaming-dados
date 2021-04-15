import json
import random
import threading
import time

from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType
from kafka import KafkaConsumer, KafkaProducer
# python3 producer_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class Producer(threading.Thread):

    def run(self):

        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            data = {}
            id_ = random.randint(0, 1000)

            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'idade': random.randint(10, 50), 'altura': random.randint(100, 200),
                            'peso': random.randint(30, 100)}
                message = [id_, streaming]
                data[id_] = message

            producer.send('topic', streaming)
            time.sleep(random.randint(0, 5))

class Consumer(threading.Thread):

    def run(self):
        # stream = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest')
        # stream.subscribe(['topic'])
        #spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 producer_consumer.py
    
        spark = SparkSession \
            .builder \
            .appName("wordCountStructured") \
            .getOrCreate()

        schema = StructType([
                    StructField('idade', StringType()),
                    StructField('altura', StringType()),
                    StructField('peso', StringType())
                ])

        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "topic") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        #teste = kafka_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")

        #kafka_df.select(from_json(col("value"), schema))

        #words_df = kafka_df.select(expr("explode(split(value,',')) as word"))

        #counts_df = words_df.groupBy("word").count()

        # teste.withColumn("value", from_json("value", schema))\
        #             .select(col('value.*'))
        
        kafka_df = kafka_df.withColumn("value", from_json("value", schema)) #\
                    #.select(col("value.*"))\

        kafka_df.writeStream \
            .format("console") \
            .start()
        
        #kafka_df.awaitTermination()

if __name__ == '__main__':
    threads = [
        Producer(),
        # Producer(),
        # Producer(),
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()
