Schema Issues in Spark (and How I Stay Sane)

If you’ve worked with Spark long enough, you must have run into schema inconsistencies. When you let Spark infer schemas, it feels magical at first… until it isn’t.

One day everything works. The next day, Spark throws a tantrum because a field decided to disappear from the data. Can’t really blame Spark, right? I mean, it’s not a mind reader (and honestly, if it were, I’d be worried about other things than missing fields).

Let’s see this in action.

Setup
import sys
from awsglue.transforms import \*
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
from pyspark.sql.functions import col
import boto3

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

The Problem

Every day, we receive user data. Inside preferences, there’s a subfield called privacy (boolean). Our code tries to flatten it.

df = spark.read.option("multiLine", True).json("s3://bucket-amod/data/2025-01-03.json")
df.select(col("preferences.privacy")).show()

Boom 💥

AnalysisException: No such struct field privacy in theme

Why? Let’s check the schema.

When the Field Exists (2025-01-02.json)
root
|-- email: string (nullable = true)
|-- is_active: boolean (nullable = true)
|-- preferences: struct (nullable = true)
| |-- privacy: boolean (nullable = true)
| |-- theme: struct (nullable = true)
| | |-- contrast: string (nullable = true)
| | |-- mode: string (nullable = true)
|-- score: long (nullable = true)
|-- user_id: string (nullable = true)

Everything looks great, the code works fine.

When the Field Goes Missing (2025-01-03.json)
root
|-- email: string (nullable = true)
|-- is_active: boolean (nullable = true)
|-- preferences: struct (nullable = true)
| |-- theme: struct (nullable = true)
| | |-- contrast: string (nullable = true)
| | |-- mode: string (nullable = true)
|-- score: long (nullable = true)
|-- user_id: string (nullable = true)

Notice something? Yup — privacy packed its bags and left. Spark didn’t see it in the data, so it just shrugged and said, “Not my problem.” Meanwhile, our code crashes.

Why Spark Does This

Spark only knows what it sees. If a field doesn’t appear in the sample, Spark doesn’t magically add it. And honestly, that’s fair. Imagine Spark trying to read our intentions — that’d be creepier than helpful.

The Fix: Explicit Schema

So the “proper” fix is: we pass a schema to Spark. Spark will happily accept it, and for fields missing in the data, it just assigns null.

But here’s the kicker: writing schemas for nested JSON is painful. And I don’t mean “stubbed-my-toe” painful. I mean “nested 20 levels deep, why-did-I-choose-this-career” painful. If you’ve worked with MongoDB-style documents, you know the suffering.

My Shortcut: Let Spark Do the Hard Work

Instead of handcrafting schemas, I cheat. I write a sample JSON record that has all the fields I expect, and let Spark generate the schema for me.

sample = {
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

schema_json = df.schema.jsonValue()
print(json.dumps(schema_json, indent=2))

This gives me a neat JSON schema with privacy included, without me crying over StructType definitions at midnight.

Schema Registry (The DIY Version)

You can save this schema JSON to S3 (say, schema.json). That’s your own little schema registry. No Glue Schema Registry overhead, just plain JSON and full control.

Loading Schema Back

Now, when reading new files, just load the schema and pass it:

from pyspark.sql.types import StructType

s3 = boto3.resource("s3")
obj = s3.Object("bucket-amod", "schema.json")
schema_str = obj.get()["Body"].read().decode("utf-8")
schema = StructType.fromJson(json.loads(schema_str))

df = spark.read.option("multiLine", True).json(
"s3://bucket-amod/data/2025-01-03.json",
schema=schema
)

df.select(col("preferences.privacy")).show()
df.printSchema()

Resulting schema:

root
|-- email: string (nullable = true)
|-- is_active: boolean (nullable = true)
|-- preferences: struct (nullable = true)
| |-- privacy: boolean (nullable = true)
| |-- theme: struct (nullable = true)
| | |-- contrast: string (nullable = true)
| | |-- mode: string (nullable = true)
|-- score: long (nullable = true)
|-- user_id: string (nullable = true)

Now even when privacy is missing from the data, Spark won’t complain. It just shows null. Problem solved.

Wrap Up

So that’s how I keep my sanity:

Spark isn’t wrong — it just infers from what it sees.

Writing schemas manually for nested documents is a nightmare.

Instead, generate a schema from a sample record, save it, and reuse it.

Your future self will thank you for not having to debug “No such struct field” errors at 2 AM.

What do you guys think? For me, this trick has saved a ton of headaches — and possibly my hairline.
