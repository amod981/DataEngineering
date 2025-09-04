
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
import json
from pyspark.sql.functions import col, explode, when, array_contains
import boto3
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
df = spark.read.option("multiLine", True).json("s3://bucket-amod/data/2025-01-03.json")
df.select(col("preferences.privacy")).show()
sample =   {
    "user_id": "u_003",
    "email": "carol@example.com",
    "is_active": True,
    "score": 92,
    "preferences": {
      "theme": {"mode": "dark", "contrast": "medium"},
        "privacy": True
    }
  }
rdd = spark.sparkContext.parallelize([json.dumps(sample)])
df = spark.read.json(rdd)

schema_json = df.schema.jsonValue()            # Python dict
print(json.dumps(schema_json, indent=2)) 
# new code with schema used
from pyspark.sql.types import StructType
s3 = boto3.resource("s3")
obj = s3.Object("bucket-amod", "schema.json")
schema_str = obj.get()["Body"].read().decode("utf-8")
schema = StructType.fromJson(json.loads(schema_str))
df = spark.read.option("multiLine", True).json("s3://bucket-amod/data/2025-01-03.json", schema=schema)
df.select(col("preferences.privacy")).show()
df.printSchema()
job.commit()