from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim,lower,year,to_date,when
spark= SparkSession.builder.appName("data-cleaning-partitioning").getOrCreate()
df=spark.read.json("sample_data/Customers.json")
df_clean= df\
    .withColumn("name",trim(col("name")))\
    .fillna({"email":"Unknown"})\
    .fillna({"country":"Unknown"})\
    .withColumn("signup_date",to_date("signup_date"))\
    .withColumn("signup_year",year("signup_date"))\
    .dropna(subset=["name","email"])
df_clean.repartition(1).write\
    .mode("overwrite")\
    .parquet("output/clean_customers")