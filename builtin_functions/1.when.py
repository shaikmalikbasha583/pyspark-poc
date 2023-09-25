import pyspark.sql.functions as F

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()
data = [
    ("James", "M", 60000),
    ("Michael", "M", 70000),
    ("Robert", None, 400000),
    ("Maria", "F", 500000),
    ("Jen", "", None),
]

columns = ["name", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()
"""
+-------+------+------+
|   name|gender|salary|
+-------+------+------+
|  James|     M| 60000|
|Michael|     M| 70000|
| Robert|  null|400000|
|  Maria|     F|500000|
|    Jen|      |  null|
+-------+------+------+
"""

# Using When otherwise
df2 = df.withColumn(
    "NEW_GENDER",
    F.when(df.gender == "M", "Male")
    .when(df.gender == "F", "Female")
    .when(df.gender.isNull(), "")
    .otherwise(df.gender),
)
df2.show()
"""
+-------+------+------+----------+
|   name|gender|salary|NEW_GENDER|
+-------+------+------+----------+
|  James|     M| 60000|      Male|
|Michael|     M| 70000|      Male|
| Robert|  null|400000|          |
|  Maria|     F|500000|    Female|
|    Jen|      |  null|          |
+-------+------+------+----------+
"""


df2 = df.select(
    F.col("*"),
    F.when(df.gender == "M", "Male")
    .when(df.gender == "F", "Female")
    .when(df.gender.isNull(), "")
    .otherwise(df.gender)
    .alias("NEW_GENDER"),
)
df2.show()
"""
+-------+------+------+----------+
|   name|gender|salary|NEW_GENDER|
+-------+------+------+----------+
|  James|     M| 60000|      Male|
|Michael|     M| 70000|      Male|
| Robert|  null|400000|          |
|  Maria|     F|500000|    Female|
|    Jen|      |  null|          |
+-------+------+------+----------+
"""


# Using SQL Case When
df3 = df.withColumn(
    "NEW_GENDER",
    F.expr(
        """
        CASE WHEN gender = 'M' THEN 'Male' 
        WHEN gender = 'F' THEN 'Female' 
        WHEN gender IS NULL THEN '' 
        ELSE gender END"""
    ),
)
df3.show()
"""
+-------+------+------+----------+
|   name|gender|salary|NEW_GENDER|
+-------+------+------+----------+
|  James|     M| 60000|      Male|
|Michael|     M| 70000|      Male|
| Robert|  null|400000|          |
|  Maria|     F|500000|    Female|
|    Jen|      |  null|          |
+-------+------+------+----------+
"""

df4 = df.select(
    F.col("*"),
    F.expr(
        """
        CASE WHEN gender = 'M' THEN 'Male' 
        WHEN gender = 'F' THEN 'Female' 
        WHEN gender IS NULL THEN '' 
        ELSE gender END"""
    ),
)
"""
+-------+------+------+----------+
|   name|gender|salary|NEW_GENDER|
+-------+------+------+----------+
|  James|     M| 60000|      Male|
|Michael|     M| 70000|      Male|
| Robert|  null|400000|          |
|  Maria|     F|500000|    Female|
|    Jen|      |  null|          |
+-------+------+------+----------+
"""

df.createOrReplaceTempView("EMP")
spark.sql(
    """
    SELECT name, 
        CASE WHEN gender = 'M' THEN 'Male' 
        WHEN gender = 'F' THEN 'Female' 
        WHEN gender IS NULL THEN ''
        ELSE gender END AS NEW_GENDER
    FROM EMP
"""
).show()
"""
+-------+----------+
|   name|NEW_GENDER|
+-------+----------+
|  James|      Male|
|Michael|      Male|
| Robert|          |
|  Maria|    Female|
|    Jen|          |
+-------+----------+
"""


close_spark_session_object(spark_obj=spark)
