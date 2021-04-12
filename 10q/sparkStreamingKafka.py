from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties

#!!! COMANDO PARA RODAR !!!
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 sparkStreamingKafka.py

#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic twitter
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic facebook
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic instagram

#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic twitter
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic facebook
#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic instagram


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("wordCountStructured") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("subscribe", "facebook") \
        .option("subscribe", "instagram") \
        .load()

    words_df = kafka_df.select(expr("explode(split(value,' ')) as word"))

    
    word_count_query.awaitTermination()
