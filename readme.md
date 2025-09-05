# Schema Issues in Spark (and How I Stay Sane)

If you’ve worked with Spark long enough, you’ve probably run into schema inconsistencies. When you let Spark infer schemas, it feels magical at first… until it isn’t.

One day everything works. The next day, Spark throws a tantrum because a field decided to disappear from the data. Can’t really blame Spark, right? It’s not a mind reader (and honestly, if it were, I’d be worried about other things than missing fields).

Let’s see this in action.

---

## Setup

```python
import sys
from awsglue.transforms import *
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
```

---

## The Problem

Every day, we receive user data. Inside `preferences`, there’s a subfield called `privacy` (boolean). Our code tries to flatten it.

```python
df = spark.read.option("multiLine", True).json("s3://bucket-amod/data/2025-01-03.json")
df.select(col("preferences.privacy")).show()
```
Error:
```
AnalysisException: No such struct field privacy in theme
```

Why? Let’s check the schema.

---

### When the Field Exists (`2025-01-02.json`)

```text
root
 |-- email: string (nullable = true)
 |-- is_active: boolean (nullable = true)
 |-- preferences: struct (nullable = true)
 |    |-- privacy: boolean (nullable = true)
 |    |-- theme: struct (nullable = true)
 |    |    |-- contrast: string (nullable = true)
 |    |    |-- mode: string (nullable = true)
 |-- score: long (nullable = true)
 |-- user_id: string (nullable = true)
```

Everything looks great; the code works fine.

---

### When the Field Goes Missing (`2025-01-03.json`)

```text
root
 |-- email: string (nullable = true)
 |-- is_active: boolean (nullable = true)
 |-- preferences: struct (nullable = true)
 |    |-- theme: struct (nullable = true)
 |    |    |-- contrast: string (nullable = true)
 |    |    |-- mode: string (nullable = true)
 |-- score: long (nullable = true)
 |-- user_id: string (nullable = true)
```

Notice something? Yup — `privacy` packed its bags and left. Spark didn’t see it in the data, so it left it out. Meanwhile, our code crashes.

---

## Why Spark Does This

Spark only knows what it sees. If a field is absent in the sample, Spark doesn’t magically add it. And honestly, that’s fair. Imagine Spark trying to read our intentions — that’d be creepier than helpful.

Our code, however, is written assuming `privacy` will always exist. Hence the mismatch.

---

## The Fix: Explicit Schema

The right move is to **pass a schema to Spark**. Spark will keep the `privacy` field in the schema and set it to `null` when it’s missing in the data.

But here’s the kicker: **writing nested schemas manually is painful**. And I don’t mean “stubbed-my-toe” painful. I mean “nested 20 levels deep, why-did-I-choose-this-career” painful. If you’ve worked with MongoDB-style documents, you know the suffering.

---

## Shortcut: Generate Schema From a Sample

Instead of handcrafting schemas, create a **sample JSON record** with the fields you expect and let Spark generate the schema for you.

```python
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

schema_json = df.schema.jsonValue()   # Python dict
print(json.dumps(schema_json, indent=2))
```

<details>
<summary><strong>Click to expand: Generated Schema JSON</strong></summary>

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "email",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "is_active",
      "type": "boolean",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "preferences",
      "type": {
        "type": "struct",
        "fields": [
          {
            "name": "privacy",
            "type": "boolean",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "theme",
            "type": {
              "type": "struct",
              "fields": [
                {
                  "name": "contrast",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "mode",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                }
              ]
            },
            "nullable": true,
            "metadata": {}
          }
        ]
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "score",
      "type": "long",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "user_id",
      "type": "string",
      "nullable": true,
      "metadata": {}
    }
  ]
}
```
</details>

---

## DIY Schema Registry (S3)

Store the schema JSON in a central place (e.g., S3 as `schema.json`). That’s your own lightweight “schema registry” — simple JSON, full control.

---

## Load Schema and Use It

```python
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
```

Resulting schema (consistent, with `privacy` retained):

```text
root
 |-- email: string (nullable = true)
 |-- is_active: boolean (nullable = true)
 |-- preferences: struct (nullable = true)
 |    |-- privacy: boolean (nullable = true)
 |    |-- theme: struct (nullable = true)
 |    |    |-- contrast: string (nullable = true)
 |    |    |-- mode: string (nullable = true)
 |-- score: long (nullable = true)
 |-- user_id: string (nullable = true)
```

Now even when `privacy` is missing from the data, Spark won’t complain — it’ll just be `null`.

---

## Wrap Up

How I keep my sanity:

- Spark isn’t “wrong”; it infers from what it sees.
- Writing nested schemas by hand is a nightmare.
- Generate a schema from a sample, store it (S3), and reuse it.

Your future self will thank you for not debugging “No such struct field” at 2 AM.
