# python3 --version  
# python3.10 -m venv env
# source env/bin/activate
# pip install pyspark 
# pip install pandas pyarrow
# spark-submit Q3-C.py

import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType, coalesce, mean, pandas_udf, split, udf
from pyspark.sql.types import FloatType

spark = (
    SparkSession.builder.appName("HW1-Q3-C")
    .master("local[*]")
    .getOrCreate()
)

output_folder = "PARTC-output-py"
if os.path.exists(output_folder):
    os.system(f"rm -rf {output_folder}")

@pandas_udf("float")
def get_age_average(ages: pd.Series) -> float:
    avg = ages.fillna(23).mean()
    return avg

_ = spark.udf.register("get_age_average", get_age_average)
df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
df = df.createOrReplaceTempView("titanic")

df = spark.sql(
    "SELECT Name, Age, split(Name, ',')[0] as LastName "
    "FROM titanic "
    "WHERE Age > (SELECT get_age_average(Age) FROM titanic)"
)

df.write.csv(output_folder, header=True)
spark.stop()