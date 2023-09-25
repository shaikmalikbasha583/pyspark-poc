from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

dataDF = [
    (("James", "", "Smith"), "1991-04-01", "M", 3000),
    (("Michael", "Rose", ""), "2000-05-19", "M", 4000),
    (("Robert", "", "Williams"), "1978-09-05", "M", 4000),
    (("Maria", "Anne", "Jones"), "1967-12-01", "F", 4000),
    (("Jen", "Mary", "Brown"), "1980-02-17", "F", -1),
]

schema = StructType(
    [
        StructField(
            "name",
            StructType(
                [
                    StructField("firstName", StringType(), True),
                    StructField("middleName", StringType(), True),
                    StructField("lastName", StringType(), True),
                ]
            ),
        ),
        StructField("dob", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()
"""
root
 |-- name: struct (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- middleName: string (nullable = true)
 |    |-- lastName: string (nullable = true)
 |-- dob: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
"""

## Example 1
df.withColumnRenamed("dob", "dateOfBirth").show()
"""
+--------------------+-----------+------+------+
|                name|dateOfBirth|gender|salary|
+--------------------+-----------+------+------+
|    {James, , Smith}| 1991-04-01|     M|  3000|
|   {Michael, Rose, }| 2000-05-19|     M|  4000|
|{Robert, , Williams}| 1978-09-05|     M|  4000|
|{Maria, Anne, Jones}| 1967-12-01|     F|  4000|
|  {Jen, Mary, Brown}| 1980-02-17|     F|    -1|
+--------------------+-----------+------+------+
"""

## Example 2
df.withColumnRenamed("dob", "dateOfBirth").withColumnRenamed(
    "salary", "salaryAmount"
).show()
"""
+--------------------+-----------+------+------------+
|                name|dateOfBirth|gender|salaryAmount|
+--------------------+-----------+------+------------+
|    {James, , Smith}| 1991-04-01|     M|        3000|
|   {Michael, Rose, }| 2000-05-19|     M|        4000|
|{Robert, , Williams}| 1978-09-05|     M|        4000|
|{Maria, Anne, Jones}| 1967-12-01|     F|        4000|
|  {Jen, Mary, Brown}| 1980-02-17|     F|          -1|
+--------------------+-----------+------+------------+
"""

## Example 3
schema2 = StructType(
    [
        StructField("fname", StringType()),
        StructField("middleName", StringType()),
        StructField("lname", StringType()),
    ]
)
df.select(
    col("name").cast(schema2), col("dob"), col("gender"), col("salary")
).printSchema()
"""
root
 |-- name: struct (nullable = true)
 |    |-- fname: string (nullable = true)
 |    |-- middleName: string (nullable = true)
 |    |-- lname: string (nullable = true)
 |-- dob: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
"""

## Example 4
df.select(
    col("name.firstName").alias("fname"),
    col("name.middleName").alias("mname"),
    col("name.lastName").alias("lname"),
    col("dob"),
    col("gender"),
    col("salary"),
).show(truncate=False)
"""
+-------+-----+--------+----------+------+------+
|fname  |mname|lname   |dob       |gender|salary|
+-------+-----+--------+----------+------+------+
|James  |     |Smith   |1991-04-01|M     |3000  |
|Michael|Rose |        |2000-05-19|M     |4000  |
|Robert |     |Williams|1978-09-05|M     |4000  |
|Maria  |Anne |Jones   |1967-12-01|F     |4000  |
|Jen    |Mary |Brown   |1980-02-17|F     |-1    |
+-------+-----+--------+----------+------+------+
"""

## Example 5
df.withColumn("fname", col("name.firstName")).withColumn(
    "mname", col("name.middleName")
).withColumn("lname", col("name.lastName")).drop("name").show()
"""
+----------+------+------+-------+-----+--------+
|       dob|gender|salary|  fname|mname|   lname|
+----------+------+------+-------+-----+--------+
|1991-04-01|     M|  3000|  James|     |   Smith|
|2000-05-19|     M|  4000|Michael| Rose|        |
|1978-09-05|     M|  4000| Robert|     |Williams|
|1967-12-01|     F|  4000|  Maria| Anne|   Jones|
|1980-02-17|     F|    -1|    Jen| Mary|   Brown|
+----------+------+------+-------+-----+--------+
"""

close_spark_session_object(spark_obj=spark)
