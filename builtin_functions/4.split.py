import pyspark.sql.functions as F


from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

data = data = [
    ("James", "", "Smith", "1991-04-01"),
    ("Michael", "Rose", "", "2000-05-19"),
    ("Robert", "", "Williams", "1978-09-05"),
    ("Maria", "Anne", "Jones", "1967-12-01"),
    ("Jen", "Mary", "Brown", "1980-02-17"),
]

columns = ["firstname", "middlename", "lastname", "dob"]
df = spark.createDataFrame(data, columns)
df.printSchema()
df.show(truncate=False)
df1 = (
    df.withColumn("year", F.split(df["dob"], "-").getItem(0))
    .withColumn("month", F.split(df["dob"], "-").getItem(1))
    .withColumn("day", F.split(df["dob"], "-").getItem(2))
)
df1.printSchema()
df1.show(truncate=False)

# Alternatively we can do like below
split_col = F.split(df["dob"], "-")
df2 = (
    df.withColumn("year", split_col.getItem(0))
    .withColumn("month", split_col.getItem(1))
    .withColumn("day", split_col.getItem(2))
)
df2.show(truncate=False)

# Using split() function of Column class
split_col = F.split(df["dob"], "-")
df3 = df.select(
    "firstname",
    "middlename",
    "lastname",
    "dob",
    split_col.getItem(0).alias("year"),
    split_col.getItem(1).alias("month"),
    split_col.getItem(2).alias("day"),
)
df3.show(truncate=False)
df4 = spark.createDataFrame(
    [("oneAtwoBthree",)],
    [
        "str",
    ],
)
df4.select(F.split(df4.str, "[AB]").alias("str")).show()

df4.select(F.split(df4.str, "[AB]", 2).alias("str")).show()

close_spark_session_object(spark)
