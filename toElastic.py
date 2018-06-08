# Spark 2.0.0
#Elasticsearch 6.2.3
#ElasticsearchHadoop
#python 2.7
#elasticsearch-spark-20_2.11
#scala 2.11
#> pip install elasticsearch
#> pyspark --jars /usr/local/spark/jars/elasticsearch-spark-20_2.11.jars
#******* Pay attention to the reqirements in the elasticsearch ES-Hadoop Documentation *******

from pyspark import SparkContext
from pyspark.sql import SQLContext
from elasticsearch import Elasticsearch, helpers
import pandas as pd


#Initialising Spark Context & sqlContext
sc.stop()
sc = SparkContext("local[2]", "appName")
sqlContext = SQLContext(sc)
#Connection to elastic (I have no authentification needed)+index creation
es = Elasticsearch()
es.indices.create(index='test', ignore=400)
#Import csv File
inputData= sc.textFile("../path_to_data/data.csv")
ES_ready_df = inputData.map(lambda x: x.split(",")).toDF(["id_1","field1" , "field2" , "field3"])
ES_ready_df.show(3)
#send To Elasticsearch
ES_ready_df.write.format("org.elasticsearch.spark.sql").option('es.mapping.id', 'id_1').mode(append).save("detection/log")
#If you want to delete the index
es.indices.delete(index='test', ignore=400)
