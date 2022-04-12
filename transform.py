# Databricks notebook source
import os
from pyspark.sql import functions as F

df = spark.read.format("csv").load(f"file:{os.getcwd()}/data/games_data_greece.csv", header = True)
df.show()

# COMMAND ----------

def split_points_columns(df, col_name, new_col_1, new_col_2):
    return (df.withColumn(new_col_1, F.split(F.col(col_name), pattern = "-").getItem(0))
                   .withColumn(new_col_2, F.split(F.col(col_name), pattern = "-").getItem(1))
                   .drop(col_name))

def overwrite_points_columns(df):
    for col_name in ['1M-1A', '2M-2A', '3M-3A']:
        col1, col2 = col_name.strip().split('-')
        df = split_points_columns(df, col_name, col1, col2)
    return df

def overwrite_score_column(df):
    return split_points_columns(df, 'score', 'home_score', 'away_score')

df = overwrite_points_columns(df)
df = overwrite_score_column(df)
df.show()

# COMMAND ----------

df = df.drop('Pts', 'Reb', 'Ast', 'FG%', '1%', 'Reb.1', 'Ast.1', 'Pts.1', 'Eff', 'url')
df.show()

# COMMAND ----------

splitted_date = F.split(F.col('date'), pattern = ",")
df.select(splitted_date.getItem(1), splitted_date.getItem(2)).show()

# COMMAND ----------

expr = F.expr('locate(",", date) - 1')
col_len = F.length('date')

df.withColumn('date2', F.substring('date', 2,3)).show()

# COMMAND ----------



# COMMAND ----------

display(df.select('date'))

# COMMAND ----------

df = df.drop(F.col('Pts'), F.col('FG%'), F.col('1%'), F.col('Reb.1'))
