import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

tmp_path = os.path.join(os.getcwd(), "tmp")
tmp_path = "D:\spark-tmp"


def get_spark_session_object(app_name: str = "PySpark Session Object") -> SparkSession:
    print(f"Creating spark session object...{tmp_path}")
    return (
        SparkSession.builder.master("local[3]")
        .config("spark.local.dir", tmp_path)
        .config("spark.driver.memory", "2G")
        .appName(app_name)
        .getOrCreate()
    )


def close_spark_session_object(spark_obj: SparkSession) -> None:
    print("Closing the created spark session object...")
    spark_obj.stop()
    print("Closed!!!")
