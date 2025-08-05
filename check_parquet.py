from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("data-check").getOrCreate()
df=spark.read.parquet("output/clean_customers/part-00000-ea42b042-f3c4-45fe-a7f1-1d2cefb45862-c000.snappy.parquet")
df.printSchema()
df.show()