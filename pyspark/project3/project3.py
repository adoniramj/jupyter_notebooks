#!/usr/bin/env python
# coding: utf-8

# # Final Project CAP 4784

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


import pyspark.sql.functions as F


# In[3]:


from pyspark.sql.types import *


# ## Import growth_data.csv

# In[4]:


df_data=spark.read.csv('growth_data.csv'                       ,header=True                       ,inferSchema=True)


# In[5]:


# Verify data was imported.
df_data.count()


# In[6]:


# Verify schema of df_data
df_data.printSchema()


# ## Import country_codes.csv

# In[7]:


df_countries=spark.read.csv('country_codes.csv'                            ,header=True                            ,inferSchema=True)


# In[8]:


# Verify country_codes.csv was imported
df_countries.count()


# In[9]:


# Verify schema of df_countries
df_countries.printSchema()


# ## Join df_data and df_countries

# In[10]:


df_combined=df_data.join(df_countries,on='Country_Code')


# In[11]:


df_combined.printSchema()


# ## Total number of records

# In[12]:


df_combined.count()


# ## Using UDF to calculate the average growth per record

# In[13]:


from pyspark.sql.functions import col

marksColumns = [col('1960'), col('1961'),col('1962'),col('1963'),col('1964'),col('1965'),col('1966'),col('1967')                ,col('1968'),col('1969'),col('1970'),col('1971'),col('1972'),col('1973'),col('1974'),col('1975')                ,col('1976'),col('1977'),col('1978'),col('1979'),col('1980'),col('1981'),col('1982'),col('1983')                ,col('1984'),col('1985'),col('1986'),col('1987'),col('1988'),col('1989'),col('1990'),col('1991')                ,col('1992'), col('1993'),col('1994'),col('1995'),col('1996'),col('1997')                ,col('1998'), col('1999'),col('2000'),col('2001'),col('2002'),col('2003'),col('2004'),col('2005')                ,col('2006'),col('2007'),col('2008'),col('2009'),col('2010'),col('2011'),col('2012'),col('2013')                ,col('2014'),col('2015'),col('2016'),col('2017'),col('2018')]

averageFunc = sum(x for x in marksColumns)/len(marksColumns)

# add Avg_Growth to dataframe
df_average=df_combined.withColumn('Avg_Growth', averageFunc)


# ## Create a new dataframe
#     - Country_name
#     - Country_Code,
#     - Region,
#     - IncomeGroup,
#     - Avg_Growth

# In[14]:


spark.catalog.dropTempView("df_temp")
df_average.createOrReplaceTempView('df_temp')
query='''
        select 
            Country_name,
            Country_code,
            region,
            IncomeGroup,
            Avg_Growth
        from df_temp '''

df_summary=spark.sql(query)


# In[15]:


df_summary.show(df_summary.count())


# ## Use the filter function to show the records that have a null average growth.

# In[16]:


df_summary.filter(df_summary.Avg_Growth.isNull()).select(['Country_name']).show(df_summary.count())


# In[17]:


df_summary.filter(df_summary.Avg_Growth.isNull()).count()


# ## Use the where clause to show all the records that have a non-null average growth rate.

# In[18]:


df_summary.where(df_summary.Avg_Growth.isNotNull()).select(['Country_name']).show(df_summary.count())


# In[19]:


df_summary.where(df_summary.Avg_Growth.isNotNull()).count()


# ## Aggregation
# 
# Remove data with null values in avg_growth before using aggregation

# In[20]:


spark.catalog.dropTempView("df_temp")
df_summary.createOrReplaceTempView('df_temp')
query='''
        select 
            Country_name,
            Country_code,
            region,
            IncomeGroup,
            Avg_Growth
        from df_temp 
        where Avg_Growth IS NOT NULL'''

df_non_null_data=spark.sql(query)


# In[21]:


df_non_null_data.groupBy('Region').agg(F.mean('Avg_Growth')).show(truncate=False)


# In[22]:


df_non_null_data.groupBy('IncomeGroup').agg(F.mean('Avg_Growth')).show(truncate=False)


# In[23]:


#from pyspark.sql.functions import desc
df_non_null_data.groupBy('Region', 'IncomeGroup').agg(F.mean('Avg_Growth')).sort('Region').show(truncate=False)


# ## Collect set

# In[26]:


df_non_null_data.groupBy('Region').agg(F.collect_set('IncomeGroup')).show(truncate=False)


# ## Collect list

# In[25]:


df_non_null_data.groupBy('Region').agg(F.collect_list('IncomeGroup')).show(truncate=False)

