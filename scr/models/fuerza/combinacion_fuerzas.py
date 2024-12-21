#!/usr/bin/env python
# coding: utf-8

# ## Seleccion de datos para busqueda de comunidades

# En este notebook, seleccionaremos valores de contaminacion para poder encontrar todas las relaciones comerciales asociadas a esos nodos. Se guarda un archivo final con esas relaciones y la contaminacion de todos los involucrados (no tan solo los valores iniciales para iniciar la seleccion). Estos archivos se ocuparan en visualizacion de los grafos y en las busqueda de comunidades por medio de diferentes algoritmos. 

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
from pyspark.sql.types import StringType,TimestampType
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt


# In[2]:


spark = SparkSession.builder \
  .appName("Test")  \
  .config("spark.yarn.access.hadoopFileSystems","abfs://data@datalakesii.dfs.core.windows.net/") \
  .config("spark.executor.memory", "24g") \
  .config("spark.driver.memory", "12g")\
  .config("spark.executor.cores", "12") \
  .config("spark.executor.instances", "24") \
  .config("spark.driver.maxResultSize", "12g") \
  .getOrCreate()

warnings.filterwarnings('ignore', category=DeprecationWarning)
sc=spark.sparkContext
sc.setLogLevel ('ERROR')
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")




spark.read.options(header=True,inferSchema=True,delimiter=",").csv("data/processed/fuerza_representante.csv").createOrReplaceTempView("fuerza1")
spark.read.options(header=True,inferSchema=True,delimiter=",").csv("data/processed/fuerza_iva.csv").createOrReplaceTempView("fuerza2")

# In[ ]:


spark.sql("SELECT fuerza1.emisor as emisor1, fuerza1.receptor as receptor1, fuerza2.emisor as emisor2, fuerza2.receptor as receptor2, fuerza1.Fi as Fi1, fuerza2.Fi as Fi2 from fuerza1 full join fuerza2 on (fuerza1.emisor=fuerza2.emisor and fuerza1.receptor=fuerza2.receptor)").createOrReplaceTempView("fuerza") 


# In[ ]:


spark.sql("SELECT case when emisor1 is null then emisor2 else emisor1 end as emisor, case when receptor1 is null then receptor2 else receptor1 end as receptor, case when Fi1 is null then Fi2 else Fi1 end as FiA, case when Fi2 is null then Fi1 else Fi2 end as FiB  from fuerza").createOrReplaceTempView("fuerza")


spark.sql("SELECT emisor, receptor, (FiA/2+FiB/2) as Fi from fuerza").createOrReplaceTempView("fuerza")

df=spark.sql("SELECT * from fuerza where Fi>0")

df.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/contaminacion/fuerza_iva_representante")

spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/contaminacion/fuerza_iva_representante").createOrReplaceTempView("fuerza")

df2=spark.sql("SELECT * from fuerza").toPandas()

df2.to_csv("data/processed/fuerza_iva_representante.csv", index=False)  


