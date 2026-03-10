# Databricks notebook source


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# Sales Data
# Step 1: TextInput
df_1 = spark.createDataFrame([['Widget A', 'East', '250.00', '2024-01-15', 'Active'], ['Widget B', 'West', '75.00', '2024-01-20', 'Active'], ['Widget A', 'East', '500.00', '2024-02-10', 'Active'], ['Widget C', 'Central', '150.00', '2024-02-15', 'Inactive'], ['Widget B', 'West', '300.00', '2024-03-01', 'Active'], ['Widget A', 'Central', '50.00', '2024-03-10', 'Active']], schema=['Product', 'Region', 'Amount', 'Date', 'Status'])

# COMMAND ----------

# Calculate Tax
# Step 2: Formula
df_2 = df_1
df_2 = df_2.withColumn("TaxAmount", (F.col("Amount") * 0.13))

# COMMAND ----------

# High Value Only
# Step 3: Filter
_filter_cond_3 = (F.col("Amount") > 100)
df_3_true = df_2.filter(_filter_cond_3)
df_3_false = df_2.filter(~(_filter_cond_3))

# COMMAND ----------

# Rename Columns
# Step 4: Select
df_4 = df_3_true
df_4 = df_4.withColumnRenamed("Region", "SalesRegion")
df_4 = df_4.withColumnRenamed("Amount", "SalesAmount")
df_4 = df_4.withColumnRenamed("Date", "SaleDate")
df_4 = df_4.drop("Status")

# COMMAND ----------

# Sort by Region and Amount
# Step 5: Sort
df_5 = df_4.orderBy(F.col("SalesRegion").asc(), F.col("SalesAmount").desc())

# COMMAND ----------

# Summary by Region
# Step 6: Summarize
df_6 = df_5.groupBy("SalesRegion").agg(F.sum("SalesAmount").alias("TotalSales"), F.count("SalesAmount").alias("TransactionCount"))

# COMMAND ----------

# Save Results
# Step 7: Output
df_6.write.format("csv").mode("overwrite").save("UNKNOWN_PATH")
