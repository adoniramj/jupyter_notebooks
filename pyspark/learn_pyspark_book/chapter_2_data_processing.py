#!/usr/bin/env python
# coding: utf-8

# # Chapter 2 Learn Pyspark
# 
# [Reference](https://www.amazon.com/Learn-PySpark-Python-based-Machine-Learning/dp/1484249607/ref=sr_1_2?keywords=Learn+pyspark&qid=1575896237&sr=8-2)

# ### Creating a SparkSession Object

# In[1]:


from pyspark.sql import SparkSession


# [Apache Spark reference](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
# 
# - SparkSession is the entry point to Spark SQL.
# - SparkSession is required to work with Dataset and DataFrame API.
# 

# In[2]:


spark=SparkSession.builder.appName('data_processing').getOrCreate()


# To create an instance of the SparkSession class attribute `builder`. Think of `builder` as a constructor method.
# 
# `appName` sets the name of the application that will be shown in the Spark UI.
# 
# `getOrCreate` similar to `new` in JavaScript.

# In[3]:


import pyspark.sql.functions as F


# `pyspark.sql.functions` is a collection of built-in functions.
# [Link to functions.](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# In[4]:


from pyspark.sql.types import *


# `pyspark.sql.types` are needed to create the data types in the dataframes.

# ### Create dataframe tha does not have null values

# DataFrames
# - Data structure in tabular form. Think of it as a SQL table.

# In[5]:


# Define the structure of the schema.
schema=StructType()         .add('user_id', 'string')         .add('country', 'string')         .add('browser', 'string')         .add('OS', 'string')         .add('age', 'integer')


# Storing the structure of the schema in the variable schema.
# 
# With StructType we are defining the type of data in each column of the dataframe.

# In[6]:


# Insert data to a DataFrame
df=spark.createDataFrame(                         [('A203','India','Chrome','WIN', 33)                         ,('A201','China','Safari','MacOS',35)                         ,('A205','UK','Mozilla','Linux',25)]                         ,schema=schema)


# In[7]:


# Printe schema of dataframe df.
df.printSchema()


# Notice that in the schema definition nothing was done about nulls.

# In[8]:


# Show the data of dataframe df
df.show()


# ### Create a dataframe the has null values

# In[9]:


# Use the same schema defined for df.
df_na=spark.createDataFrame(                            [('A203',None,'Chrome','WIN',33)                            ,('A201','China',None,'MacOS',35)                            ,('A205','UK','Mozilla','Linux',25)]                            ,schema=schema)


# In[10]:


df_na.show()


# In[11]:


# Replace null values of df_na.
# Use fillna()
df_na.fillna('0').show()


# Notice that fillna does not modify df_na. Instead, it creates a "virtual" dataframe that shows all the null values being replaced by 0.
# 
# Execute the line below to see the df is not modified.

# In[12]:


df_na.show()


# ### Replace null values in df_na with specific values

# In[13]:


df_na.fillna({'country': 'USA', 'browser': 'Safari'}).show()
# The syntax inside the parenthesis is similar to defining a JS object.


# ### Drop all rows of df_na that have a null value in any column.

# In[14]:


df_na.na.drop().show()


# ### Drop all rows where the country column is null.

# In[15]:


df_na.na.drop(subset='country').show()


# ### Replace a specific value with another value.

# In[16]:


df_na.replace('Chrome','Google Chrome').show()


# ### Drop the user_id column.

# In[17]:


df_na.drop('user_id').show()


# In[ ]:




