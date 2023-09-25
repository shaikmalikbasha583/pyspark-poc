import os

from pyspark.sql.functions import col

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "Movies.csv")

df = spark.read.format("csv").option("header", True).load(source_file_path)
df.show(n=10, truncate=False)
"""
+-------+----------------------------------+-------------------------------------------+
|movieId|title                             |genres                                     |
+-------+----------------------------------+-------------------------------------------+
|1      |Toy Story (1995)                  |Adventure|Animation|Children|Comedy|Fantasy|
|2      |Jumanji (1995)                    |Adventure|Children|Fantasy                 |
|3      |Grumpier Old Men (1995)           |Comedy|Romance                             |
|4      |Waiting to Exhale (1995)          |Comedy|Drama|Romance                       |
|5      |Father of the Bride Part II (1995)|Comedy                                     |
|6      |Heat (1995)                       |Action|Crime|Thriller                      |
|7      |Sabrina (1995)                    |Comedy|Romance                             |
|8      |Tom and Huck (1995)               |Adventure|Children                         |
|9      |Sudden Death (1995)               |Action                                     |
|10     |GoldenEye (1995)                  |Action|Adventure|Thriller                  |
+-------+----------------------------------+-------------------------------------------+
only showing top 10 rows
"""

n = 5
df.select("movieId", "title").show(n=n)
"""
+-------+--------------------+
|movieId|               title|
+-------+--------------------+
|      1|    Toy Story (1995)|
|      2|      Jumanji (1995)|
|      3|Grumpier Old Men ...|
|      4|Waiting to Exhale...|
|      5|Father of the Bri...|
+-------+--------------------+
only showing top 5 rows
"""


df.select(df.movieId, df.title).show(n=n)
"""
+-------+--------------------+
|movieId|               title|
+-------+--------------------+
|      1|    Toy Story (1995)|
|      2|      Jumanji (1995)|
|      3|Grumpier Old Men ...|
|      4|Waiting to Exhale...|
|      5|Father of the Bri...|
+-------+--------------------+
only showing top 5 rows
"""


df.select(df["movieId"], df["genres"]).show(n=n, truncate=False)
"""
+-------+-------------------------------------------+
|movieId|genres                                     |
+-------+-------------------------------------------+
|1      |Adventure|Animation|Children|Comedy|Fantasy|
|2      |Adventure|Children|Fantasy                 |
|3      |Comedy|Romance                             |
|4      |Comedy|Drama|Romance                       |
|5      |Comedy                                     |
+-------+-------------------------------------------+
only showing top 5 rows
"""


# Select All columns from List
print(f'df.select(col("movieId")).show()')
df.select(col("movieId")).show(n=n)
"""
df.select(col("movieId")).show()
+-------+
|movieId|
+-------+
|      1|
|      2|
|      3|
|      4|
|      5|
+-------+
only showing top 5 rows
"""

# Select All columns
df.select([col for col in df.columns]).show(n=n)
"""
+-------+--------------------+--------------------+
|movieId|               title|              genres|
+-------+--------------------+--------------------+
|      1|    Toy Story (1995)|Adventure|Animati...|
|      2|      Jumanji (1995)|Adventure|Childre...|
|      3|Grumpier Old Men ...|      Comedy|Romance|
|      4|Waiting to Exhale...|Comedy|Drama|Romance|
|      5|Father of the Bri...|              Comedy|
+-------+--------------------+--------------------+
only showing top 5 rows
"""

df.select("*").show(n=n)
"""
+-------+--------------------+--------------------+
|movieId|               title|              genres|
+-------+--------------------+--------------------+
|      1|    Toy Story (1995)|Adventure|Animati...|
|      2|      Jumanji (1995)|Adventure|Childre...|
|      3|Grumpier Old Men ...|      Comedy|Romance|
|      4|Waiting to Exhale...|Comedy|Drama|Romance|
|      5|Father of the Bri...|              Comedy|
+-------+--------------------+--------------------+
only showing top 5 rows
"""


close_spark_session_object(spark_obj=spark)
