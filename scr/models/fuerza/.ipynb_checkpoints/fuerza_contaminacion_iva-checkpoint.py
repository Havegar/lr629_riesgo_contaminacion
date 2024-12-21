#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: Fuerza entre entidades tributaria relacionada a probabilidad de contaminacion, definicion en base a IVA.
# ## Henry Vega (henrry.vega@api-ux.com)
# ## Data scientist

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
from pyspark.sql.types import StringType,TimestampType


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


# En primer lugar, leemos la data de los arcos comerciales correspondientes.

# In[3]:


spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/LibSDF/JBA_ARCOS_E").createOrReplaceTempView("comercial")
spark.sql("SELECT count(*) from comercial where Monto_IVA<=0").show()
spark.sql("SELECT count(*) from comercial where Monto_IVA>0").show()


# A partir de la información anterior, calculamos que el 0.98 % de todos los Monto_IVA tienen valores cero o negativos. Ahora comparemos los montos.

# In[4]:


spark.sql("SELECT sum(Monto_IVA) from comercial where Monto_IVA<=0").show()
spark.sql("SELECT sum(Monto_IVA) from comercial where Monto_IVA>0").show()


# En terminos de valor absoluto, solo el 0.29% de todo el monto registrado como IVA tiene valor negativo. Resulta sensto, dadas las caracteristicas de la data, donde la ejecucion de una nota de credito puede hacer que el remanente de IVA sea negativo, no considerar estos valores. A continuacion observaremos como es la distribucion de IVA en un histograma de frecuencia. 

# In[5]:


df=spark.sql("SELECT Monto_IVA FROM comercial where Monto_IVA>0 and Monto_IVA<1e+6")


# In[6]:


fig, ax = plt.subplots()
hist(ax, df.select('Monto_IVA'), bins = 30, color=['blue'])


# Ahora calculamos la fraccion de IVA para cada contriibuyente A que ha generado documentos tributarios al contribuyente B.

# In[7]:


spark.sql("SELECT PARU_RUT_E0, PARU_RUT_E2, Monto_IVA FROM comercial where Monto_IVA>0 order by PARU_RUT_E2 asc").createOrReplaceTempView("comercial")
spark.sql("SELECT PARU_RUT_E2, sum(Monto_IVA) as Total_IVA FROM comercial group by PARU_RUT_E2 order by PARU_RUT_E2 asc").createOrReplaceTempView("comercial_aux")
spark.sql("SELECT PARU_RUT_E0,comercial.PARU_RUT_E2,ROUND(Monto_IVA/Total_IVA,4) as Fi from comercial left join comercial_aux on comercial.PARU_RUT_E2= comercial_aux.PARU_RUT_E2").createOrReplaceTempView("comercial")


# In[8]:


spark.sql("SELECT PARU_RUT_E0 as emisor, PARU_RUT_E2 as receptor,  Fi from comercial order by Fi desc").show()


# Finalmente guardamos la data para poder utilizarla posteriormente en la propagacion. 

# In[ ]:


iva=spark.sql("SELECT PARU_RUT_E0 as emisor, PARU_RUT_E2 as receptor, Fi from comercial").toPandas()
iva.to_csv('data/processed/fuerza_iva.csv', index=False)

