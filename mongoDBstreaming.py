import findspark
findspark.init("/home/cipl1120/spark/spark-3.2.1-bin-hadoop3.2")

from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import *

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
#prerequestie
#you need to create a confluent kafka producer using mongoDb connector to stream data from mongoDb database
if __name__ == "__main__":

    print("application started")

    spark = SparkSession\
        .builder\
        .appName("mongo streaming app")\
        .master("local[*]")\
        .getOrCreate()


    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'localhost:9092') \
        .option("subscribe", "dbtopic") \
        .option("startingOffsets", "latest") \
        .load()
    print(stream_df.isStreaming)


    stream_df.printSchema()

    order_df1=stream_df.selectExpr("CAST(value AS STRING)")

    order_schema = StructType() \
        .add("status", StringType()) \
        .add("email", StringType())

    order_df2 = order_df1\
        .select(from_json(col("value"), order_schema)\
        .alias("orders"))

    order_df3 = order_df2.select("orders.*")
    df_filter = order_df3.filter("status == 'rejected'")


    def process_row(row):
        # Write row to storage
        print(row)
        emailp=row.email
        statusp=row.status

        receiver = "reciever@gmail.com"
        sender = "sender@gmail.com"
        senders_password = "password"

        body = "user " + emailp + " is " + statusp
        subject = "User rejected"

        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = receiver
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))
        text = message.as_string()

        mail = smtplib.SMTP('smtp.gmail.com', 587)
        mail.ehlo()
        mail.starttls()
        mail.login(sender, senders_password)
        mail.sendmail(sender, receiver, text)
        mail.close()


    write_query = df_filter\
        .writeStream\
        .outputMode("update")\
        .foreach(process_row)\
        .start()


    write_query.awaitTermination()


    print("application completed")


