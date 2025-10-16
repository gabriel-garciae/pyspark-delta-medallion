# Databricks notebook source
# MAGIC %md
# MAGIC Libraries imports

# COMMAND ----------

import pyspark.sql.functions as F
import re

# COMMAND ----------

# MAGIC %md
# MAGIC Read and light transformations

# COMMAND ----------

raw_dir = "dbfs:/FileStore/tables"
file_sales = "abi_bus_case1_beverage_sales_20210726.csv"

df_sales = (
    spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "UTF-16")
        .option("inferSchema", "true")
        .option("sep", "\t")
        .load(f"{raw_dir}/{file_sales}")
)

#clean column names (normalize to snake_case)
def clean_col(col_name: str) -> str:
    col_name = col_name.strip()
    col_name = re.sub(r'[^0-9a-zA-Z_]', '_', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    return col_name.lower()

df_sales = df_sales.toDF(*[clean_col(c) for c in df_sales.columns])

print("Columns after cleaning:")
print(df_sales.columns)
display(df_sales)
print(f"Total rows in sales dataset: {df_sales.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC Save as bronze delta table

# COMMAND ----------

df_sales.write.mode("overwrite").format("delta").saveAsTable(
    "abinbev_case_bronze.beverage_sales")
