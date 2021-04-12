from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import expr
from time import sleep
 
 #/opt/spark/bin/spark-submit activity.py

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("activityExample") \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    static = spark.read.json("activity-data/")
    dataSchema = static.schema

    # leitura dos dados
    streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json("activity-data")

    simpleTransformQuery = streaming.select("User", "gt", "model", "Arrival_Time")\
                                    .where("gt in ('walk','stand')")\
                                    .where("User = 'a'")\
                                    .withColumnRenamed("gt", "activity")\
                                    .writeStream\
                                    .queryName("simple_transform")\
                                    .format("memory")\
                                    .outputMode("append")\
                                    .start()

    for x in range(50):
        spark.sql("SELECT * FROM simple_transform").show()
        sleep(5)

    
    simpleTransform.awaitTermination() # usar só via script