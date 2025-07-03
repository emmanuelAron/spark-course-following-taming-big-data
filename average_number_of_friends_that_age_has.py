from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the age and number of friends column:")
friendsByAge = people.select("age","friends")
friendsByAge.show()


print("Group by age and get the number")
friendsByAge.groupBy("age").agg(F.avg("friends").alias("average_friends")).show()

#print("Make everyone 10 years older:")
#people.select(people.name, people.age + 10).show()

spark.stop()

