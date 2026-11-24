"""Step 12 smoke test: verify PySpark + Python 3.11 + Java 8."""
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("step12-smoke").getOrCreate()
print("PYTHON_VERSION", sys.version.split()[0])
print("SPARK_VERSION", spark.version)
df = spark.range(0, 100000, 1, 8).selectExpr("id", "id % 7 as g")
print("ROWS", df.count())
print("GROUPS", df.groupBy("g").count().count())
spark.stop()
