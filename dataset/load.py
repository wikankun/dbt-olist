import os, glob
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("etl").getOrCreate()

properties_target = {
    "user": "postgresuser",
    "password": "postgrespw",
    "driver": "org.postgresql.Driver"
}

for file in glob.glob('./*.csv'):
    df = spark.read \
            .options(header='True', inferSchema='True',delimiter=',') \
            .csv(file)
    
    df_name = file.split("/")[-1].replace(".csv", "")
    print(df_name)
    
    if df_name == "geolocation":
        df = df.orderBy('geolocation_lat', 'geolocation_lng') \
               .coalesce(1) \
               .dropDuplicates(subset = ['geolocation_zip_code_prefix'])

    if df_name == "order_payments":
        new_row = spark.createDataFrame(
            [("bfbd0f9bdef84302105ad712db648a6c",1,"credit_card",3,134.97)],
            ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
        )
        df = df.union(new_row)
        
    if df_name == "orders":
        df = df.withColumn("order_purchase_timestamp",df.order_purchase_timestamp.cast('timestamp'))
        df = df.withColumn("order_approved_at",df.order_approved_at.cast('timestamp'))
        df = df.withColumn("order_delivered_carrier_date",df.order_delivered_carrier_date.cast('timestamp'))
        df = df.withColumn("order_delivered_customer_date",df.order_delivered_customer_date.cast('timestamp'))
        df = df.withColumn("order_estimated_delivery_date",df.order_estimated_delivery_date.cast('timestamp'))
    
    df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", properties_target["driver"]).option("dbtable", f"raw.{df_name}") \
        .option("user", properties_target["user"]).option("password", properties_target["password"]) \
        .mode('overwrite').save()

    print(df.printSchema())