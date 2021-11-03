# hive-schema-from-df

The function hive_schema(df.schema) returns a string that can be used for table creation in Apache Hive or Databricks SQL. Example:

``` python
from pyspark.sql.types import *

df = spark.table("some_database.some_table")
# Or
df = spark.read.csv("some/path/to/file.csv")
# Or
df = spark.createDataFrame([{"my": "data"}], schema=StructType(StructField("my", StringType(), True)))


# Let's supose that you have to create a table, and for that you need the dataframe schema in the hive format, you the hive_schema:
schema = hive_schema(df.schema)

# Now you can create the table:
spark.sql(f"CREATE TABLE some_database.some_table ({schema})")

```

Credits: LH
