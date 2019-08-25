
#importing a pyspark package without which one gets this error: "'JavaPackage' object is not callable"
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 pyspark-shell'


import pykafka
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


#Create Spark Context to Connect Spark Cluster
conf =
SparkConf().setAppName("PythonStreamingKafkaTweetCount").setMaster("local[2]").
set("spark.cassandra.connection.host","127.0.0.1")

sc = SparkContext(conf=conf) 
sqlContext=SQLContext(sc)


#A function which saves the data in Cassandra

def saveToCassandra(user_counts):
    if not user_counts.isEmpty(): 
        sqlContext.createDataFrame(user_counts).write.format("org.apache.spark.sql.cassandra").mode('append').
        options(table="Tw", keyspace="user").save()


#Set the Batch Interval is 20 sec of Streaming Context
ssc = StreamingContext(sc, 20)

#Create Kafka Stream to Consume Data Comes From Twitter Topic

#localhost:2181 = Default Zookeeper Consumer Address
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181',"spark-streaming-consumer" , {'twitter':1})
#It is very important to notice that the port from the producer has to be from Kafka (9092) and from the consumer has to be from Zookeeper (2181).

#Parse Twitter Data as json
parsed = kafkaStream.map(lambda v: json.loads(v[1]))

#Count the number of tweets per User
user_counts = parsed.map(lambda tweet: (tweet['user']["screen_name"], 1)).reduceByKey(lambda x,y: x + y)
user_counts.foreachRDD(saveToCassandra)

#Print the User tweet counts
user_counts.pprint()

#Start Execution of Streams
ssc.start()
#ssc.awaitTermination()
ssc.stop(stopSparkContext=False,stopGraceFully=True)


data =sqlContext.read.format("org.apache.spark.sql.cassandra").options(table= "Tw", keyspace="user").load()

data.show()



