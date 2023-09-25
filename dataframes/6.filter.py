from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import col, array_contains

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()


data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M"),
]

schema = StructType(
    [
        StructField(
            "name",
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                ]
            ),
        ),
        StructField("languages", ArrayType(StringType()), True),
        StructField("state", StringType(), True),
        StructField("gender", StringType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
"""
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- languages: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- state: string (nullable = true)
 |-- gender: string (nullable = true)
"""

df.show(truncate=False)
"""
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Anna, Rose, }        |[Spark, Java, C++]|NY   |F     |
|{Julia, , Williams}   |[CSharp, VB]      |OH   |F     |
|{Maria, Anne, Jones}  |[CSharp, VB]      |NY   |M     |
|{Jen, Mary, Brown}    |[CSharp, VB]      |NY   |M     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
"""
# Using equals condition
# df.filter(df.state == "OH").show(truncate=False)
# df.where(col("state") == "OH").show(truncate=False)
df.filter(col("state") == "OH").show(truncate=False)
"""
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Julia, , Williams}   |[CSharp, VB]      |OH   |F     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
"""
## Multiple Conditions
df.where((df.state == "OH") & (df.gender == "M")).show(truncate=False)
"""
+----------------------+------------------+-----+------+
|name                  |languages         |state|gender|
+----------------------+------------------+-----+------+
|{James, , Smith}      |[Java, Scala, C++]|OH   |M     |
|{Mike, Mary, Williams}|[Python, VB]      |OH   |M     |
+----------------------+------------------+-----+------+
"""


## Using SQL Expression
df.filter("gender == 'F' AND state == 'NY'").show()
"""
+--------------+------------------+-----+------+
|          name|         languages|state|gender|
+--------------+------------------+-----+------+
|{Anna, Rose, }|[Spark, Java, C++]|   NY|     F|
+--------------+------------------+-----+------+
"""
## Filter IS IN List values

# df.filter(
#     (array_contains(df.languages, "CSharp") | array_contains(df.languages, "Java"))
# ).show(truncate=False)
df.filter(array_contains(df.languages, "Python")).show(truncate=False)
"""
+----------------------+------------+-----+------+
|name                  |languages   |state|gender|
+----------------------+------------+-----+------+
|{Mike, Mary, Williams}|[Python, VB]|OH   |M     |
+----------------------+------------+-----+------+
"""

## Close the Object
close_spark_session_object(spark_obj=spark)
