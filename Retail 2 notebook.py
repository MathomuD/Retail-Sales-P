# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings 
warnings.filterwarnings('ignore')



# COMMAND ----------

df=spark.table('workspace.default.details')
rs=df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Understanding data

# COMMAND ----------

rs.describe()

# COMMAND ----------

rs.info()

# COMMAND ----------

#no brackets
rs.columns 

# COMMAND ----------

#Displaying the entire table
display(rs)

# COMMAND ----------

#Summarise the data
print(rs)

# COMMAND ----------

#First 5 rows
rs.head()

# COMMAND ----------

#Last 5 rows
rs.tail()

# COMMAND ----------

#removes rows that have missing values
rs=rs.dropna()

# COMMAND ----------

#replaces missing values with 0
rs=rs.fillna(0)

# COMMAND ----------

rs.dtypes

# COMMAND ----------

#Checking for duplicates
rs.duplicated().sum()

# COMMAND ----------

#Checking for missing values
rs.isnull().sum()

# COMMAND ----------

#number of rows and columns
#no brackets
rs.shape

# COMMAND ----------

pd.DatetimeIndex(rs['Date'])

# COMMAND ----------

#Splitting the date column into year, month and day
date_column = pd.to_datetime(rs['Date'])
rs['year'] = date_column.dt.year
rs['month'] = date_column.dt.month
rs['day'] = date_column.dt.day

# COMMAND ----------

display(rs)

# COMMAND ----------

rs.groupby('month')['Total Amount'].sum()


# COMMAND ----------

# Sales by Category (Matplotlib)
sales_by_category = rs.groupby('Product Category')['Total Amount'].sum()
plt.figure()##Creating the canvas
sales_by_category.plot(kind='bar')
plt.title('Total Sales by Category')
plt.xlabel('Category')
plt.ylabel('Sales')
plt.show()

# COMMAND ----------

# Sales Trend Over Time
rs['Date'] = pd.to_datetime(rs['Date'])
sales_trend = rs.groupby('Date')['Total Amount'].sum()
plt.figure()
sales_trend.plot()
plt.title('Sales Trend Over Time')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.show()

# COMMAND ----------

# Seaborn - Sales by Month
plt.figure()
sns.barplot(data=rs, x='month', y='Total Amount')
plt.title('Sales by Month')
plt.show()
