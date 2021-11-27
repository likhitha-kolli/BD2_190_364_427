# import pyspark
# import pandas
# import tqdm
# import numpy
import json

import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


sc = SparkContext("local[2]", appName="spam")
ssc = StreamingContext(sc, 1)
sql_context = SQLContext(sc)
schema = StructType([
    StructField("feature0", StringType(), True),
    StructField("feature1", StringType(), True),
    StructField("feature2", StringType(), True),
])
#
def RDDtoDf(x):
    spark = SparkSession(x.context)
    if not x.isEmpty():
        # y = json.loads(x)
        # print(y)
        y = x.collect()[0]
        z = json.loads(y)
        k = z.values()
        # print(list(k))
        for i in k:
            datas = list(i.values())
        df = spark.createDataFrame(k, schema=['feature0', 'feature1', 'feature2'])
        df.show()

        # for i in k:


# print(x)
# df.show()


records = ssc.socketTextStream("localhost", 6100)
# records.flatMap(lambda x: print(x))
if records:
    records.foreachRDD(lambda x: RDDtoDf(x))
ssc.start()
ssc.awaitTermination()
ssc.stop()
