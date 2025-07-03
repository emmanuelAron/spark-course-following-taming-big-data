from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField,IntegerType,DoubleType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

#Schema 
schema = StructType(
    [
     StructField("cust_id",IntegerType(),True),
     StructField("item_id",IntegerType(),True),
     StructField("amount",DoubleType(),True),
     ])

people = spark.read.option("header", "true").schema(schema)\
    .csv("customer-orders.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Group by cust_id, and sum by amount,rounded with 2 decimals")
people.groupBy("cust_id").agg(F.round(F.sum("amount"),2).alias("total_spent")).show()

#print("Make everyone 10 years older:")
#people.select(people.name, people.age + 10).show()

spark.stop()

