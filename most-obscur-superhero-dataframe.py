from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel_Names.txt")

lines = spark.read.text("Marvel_Graph.txt")

print("First 10 names:")
names.show(10)

print("First 10 lines:")
lines.show(10)

# Small tweak : we trim each line of whitespace as that could
# throw off the counts.

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
     .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
     .groupBy("id").agg(func.sum("connections").alias("connections"))
     
print("Connections 10 first rows")
connections.show(10)

# filter connections with only one connection
oneConnection = connections.filter(func.col("connections") == 1)
joinedDf = oneConnection.join(names, on="id").orderBy("id")
print(" Joined dataframe:")
joinedDf.show()
    
# mostPopular = connections.sort(func.col("connections").desc()).first()

# mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

