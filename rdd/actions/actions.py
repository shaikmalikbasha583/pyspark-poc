import os

from helper.spark_object import close_spark_session_object, get_spark_session_object

spark = get_spark_session_object()

source_file_path = os.path.join(os.getcwd(), "data", "Employee.csv")

df = spark.read.format("csv").option("header", True).load(source_file_path)
df.printSchema()
"""
root
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- hireDate: string (nullable = true)
 |-- lastLoginTime: string (nullable = true)
 |-- salary: string (nullable = true)
 |-- bonusPercentage: string (nullable = true)
 |-- seniorManagement: string (nullable = true)
 |-- team: string (nullable = true)
"""

## 1. count()
print(f"count(): {df.count()}")
"""
count(): 1000
"""

## 2. first()
print(f"first(): {df.first()}")
"""
first(): 
Row(
    firstName='Douglas', 
    gender='Male', 
    hireDate='8/6/1993', 
    lastLoginTime='12:42 PM', 
    salary='97308', 
    bonusPercentage='6.945', 
    seniorManagement='true', 
    team='Marketing')
"""

## 3. take()
print(f"take(): {df.take(5)}")
"""
take(): [
    Row(firstName='Douglas', gender='Male', hireDate='8/6/1993', lastLoginTime='12:42 PM', salary='97308', bonusPercentage='6.945', seniorManagement='true', team='Marketing'), 
    Row(firstName='Thomas', gender='Male', hireDate='3/31/1996', lastLoginTime='6:53 AM', salary='61933', bonusPercentage='4.17', seniorManagement='true', team=None), 
    Row(firstName='Maria', gender='Female', hireDate='4/23/1993', lastLoginTime='11:17 AM', salary='130590', bonusPercentage='11.858', seniorManagement='false', team='Finance'), 
    Row(firstName='Jerry', gender='Male', hireDate='3/4/2005', lastLoginTime='1:00 PM', salary='138705', bonusPercentage='9.34', seniorManagement='true', team='Finance'), 
    Row(firstName='Larry', gender='Male', hireDate='1/24/1998', lastLoginTime='4:47 PM', salary='101004', bonusPercentage='1.389', seniorManagement='true', team='Client Services')
]
"""

## 4. show()
df.show(n=5, truncate=False)
"""
+---------+------+---------+-------------+------+---------------+----------------+---------------+
|firstName|gender|hireDate |lastLoginTime|salary|bonusPercentage|seniorManagement|team           |
+---------+------+---------+-------------+------+---------------+----------------+---------------+
|Douglas  |Male  |8/6/1993 |12:42 PM     |97308 |6.945          |true            |Marketing      |
|Thomas   |Male  |3/31/1996|6:53 AM      |61933 |4.17           |true            |null           |
|Maria    |Female|4/23/1993|11:17 AM     |130590|11.858         |false           |Finance        |
|Jerry    |Male  |3/4/2005 |1:00 PM      |138705|9.34           |true            |Finance        |
|Larry    |Male  |1/24/1998|4:47 PM      |101004|1.389          |true            |Client Services|
+---------+------+---------+-------------+------+---------------+----------------+---------------+
"""

close_spark_session_object(spark_obj=spark)
