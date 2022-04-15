# Databricks notebook source
import os
from pyspark.sql import functions as F

df = spark.read.format("csv").load(f"file:{os.getcwd()}/data/games_data_greece.csv", header = True).drop_duplicates()

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
df = df.drop('Pts', 'Reb', 'Ast', 'FG%', '1%', 'Reb.1', 'Ast.1', 'Pts.1', 'Eff')

# COMMAND ----------

dict_for_map = {"January" : "1", "February" : "2", 
     "March" : "3", "April" : "4", 
     "May" : "5", "June" : "6", 
     "July" : "7", "August" : "8", 
     "September" : "9", "October" : "10", 
     "November" : "11", "December" : "12", 
    }

df = (
    df
        .withColumn('splitt', F.split(F.col('date'), pattern = ","))
        .withColumn('month_day', F.expr('splitt[1]'))
        .withColumn('month_day', F.split(F.col('month_day'), pattern = " "))
        .withColumn('day', F.expr('month_day[2]'))
        .withColumn('month', F.expr('month_day[1]'))
        .withColumn('year', F.expr('splitt[2]'))
        .replace(to_replace=dict_for_map, subset=['month'])
        .withColumn('month', F.col('month').cast('int'))
        .withColumn('date', F.expr('day||"-"||month||"-"||substring(year,2,5)||" "||time'))
        .withColumn('date', F.to_timestamp('date', format='d-M-y h:m a'))
        .drop('splitt', 'month_day', 'time')
 )

# COMMAND ----------

from pyspark.sql.types import StringType, DateType, IntegerType
df = (df
    .withColumn('MIN', F.col('MIN').cast(IntegerType()))
    .withColumn('Or', F.col('Or').cast(IntegerType()))
    .withColumn('Dr', F.col('Dr').cast(IntegerType()))
    .withColumn('To', F.col('To').cast(IntegerType()))
    .withColumn('Stl', F.col('Stl').cast(IntegerType()))
    .withColumn('Blk', F.col('Blk').cast(IntegerType()))
    .withColumn('Fo', F.col('Fo').cast(IntegerType()))
    .withColumn('home_away', F.col('home_away').cast(IntegerType()))
    .withColumn('season', F.col('season').cast(IntegerType()))
    .withColumn('1M', F.col('1M').cast(IntegerType()))
    .withColumn('1A', F.col('1A').cast(IntegerType()))
    .withColumn('2M', F.col('2M').cast(IntegerType()))
    .withColumn('2A', F.col('2A').cast(IntegerType()))
    .withColumn('3M', F.col('3M').cast(IntegerType()))
    .withColumn('3A', F.col('3A').cast(IntegerType()))
    .withColumn('home_score', F.col('home_score').cast(IntegerType()))
    .withColumn('away_score', F.col('away_score').cast(IntegerType()))
    .withColumn('day', F.col('day').cast(IntegerType()))
    .withColumn('month', F.col('month').cast(IntegerType()))
    .withColumn('year', F.col('year').cast(IntegerType()))
)

# COMMAND ----------

df = df.withColumn('y', F.when((F.col('home_score') > F.col('away_score')), 1).otherwise(0))
df.show(5)

# COMMAND ----------

from pyspark.sql import Window as W
window = W.partitionBy(F.col('season')).orderBy(F.col('url'))
df = (df
    .withColumn('game_no', F.dense_rank().over(window))
    .orderBy(F.col('season'), F.col('game_no'), F.col('home_away'), ascending =[1,1,0])
)
df.show()

# COMMAND ----------

window = W.partitionBy(F.col(''))

# COMMAND ----------

df.filter(F.col('game_no') == 1).select('year').drop_duplicates().show()
