import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import PipelineModel
import re

def preprocess_text(text):
    cleaned_text = re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', text)
    return cleaned_text

if __name__ == "__main__":
    findspark.init()

    # Path to the pre-trained pipeline model
    path_to_model = r'C:\Users\samad\Downloads\TelecomChurn\pre_trained_model'

    # Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("ChurnPrediction") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Define schema for the JSON data
    schema = StructType([
        StructField("Account length", IntegerType(), True),
        StructField("International plan", StringType(), True),
        StructField("Voice mail plan", StringType(), True),
        StructField("Number vmail messages", IntegerType(), True),
        StructField("Total day minutes", FloatType(), True),
        StructField("Total day calls", IntegerType(), True),
        StructField("Total eve minutes", FloatType(), True),
        StructField("Total eve calls", IntegerType(), True),
        StructField("Total night minutes", FloatType(), True),
        StructField("Total night calls", IntegerType(), True),
        StructField("Total intl minutes", FloatType(), True),
        StructField("Total intl calls", IntegerType(), True),
        StructField("Customer service calls", IntegerType(), True),
        StructField("Churn", StringType(), True)
    ])

    # Read the data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "churn") \
        .option("startingOffsets", "latest") \
        .load()

    # Convert the JSON value to string and parse the JSON
    df = df.selectExpr("CAST(value AS STRING) as json_str")
    df = df.withColumn("data", from_json(col("json_str"), schema)).select("data.*")

    # Preprocessing to match training data schema
    df = df.withColumn("Churn", when(col("Churn") == "True", 1).when(col("Churn") == "False", 0).otherwise(-1))
    df = df.withColumn("Voice mail plan", when(col("Voice mail plan") == "Yes", 1).otherwise(0))
    df = df.withColumn("International plan", when(col("International plan") == "Yes", 1).otherwise(0))

    # Drop columns that were not used in training
    columns_to_drop = ['State', 'Area code', 'Total day charge', 'Total eve charge', 'Total night charge', 'Total intl charge']
    for column in columns_to_drop:
        if column in df.columns:
            df = df.drop(column)

    # Load the pre-trained pipeline model
    pipeline_model = PipelineModel.load(path_to_model)

    # Prepare features for prediction
    assembler = VectorAssembler(
        inputCols=[col for col in df.columns if col != "Churn"],
        outputCol="features"
    )

    df = assembler.transform(df)

    # Apply the entire pipeline, including the model
    prediction = pipeline_model.transform(df)

    # Select the columns of interest
    prediction = prediction.select("features", "prediction")

    # Print prediction in console
    prediction \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()
