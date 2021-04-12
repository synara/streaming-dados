from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep
import sys


# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties

#!!! COMANDO PARA RODAR !!!
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 sparkStreamingKafka.py

#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic twitter
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic facebook
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic instagram
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic other

#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic twitter
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic facebook
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic instagram
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic other

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("wordCountStructured") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "facebook, twitter, instagram, other") \
        .load()\
        .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")

    query = kafka_df\
                        .where("topic <> 'other'")\
                        .writeStream\
                        .queryName("networks")\
                        .format("memory").start()

    words_df = kafka_df.select(expr("explode(split(value,' ')) as word")).where("topic = 'other'")
    counts_df = words_df.where("word not in ('streaming', 'uni7', 'data', 'science')")

    word_count_query = counts_df.writeStream \
        .queryName("naoentendo")\
        .format("memory") \
        .start()
            
    for x in range(50):
        # spark.sql("SELECT (CASE WHEN TOPIC = 'twitter' THEN '<T>'" \
        #                        "WHEN TOPIC = 'facebook' THEN '<F>'" \
        #                        "WHEN TOPIC = 'instagram' THEN '<I>'END) AS ORIGEM, "
        #                        "value as MENSAGEM " 
        #                         "FROM networks"
        # ).show()
        spark.sql("SELECT * from naoentendo")
        sleep(5)

    
    query.awaitTermination()
    word_count_query.awaitTermination()