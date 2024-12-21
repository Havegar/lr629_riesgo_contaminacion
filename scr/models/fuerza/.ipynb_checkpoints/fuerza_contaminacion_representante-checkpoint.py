#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: Fuerza entre entidades tributaria relacionada a probabilidad de contaminacion, definicion en base a IVA y contaminacion de representantes.
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


# In[3]:


df=spark.sql("select dhdr_rut_emisor,dhdr_rut_recep,dhdr_iva,dhdr_mnt_total,dhdr_fch_emis_int from dwbgdata.header_dte_consolidada_enc_sas_analitica where (dtdc_codigo ='33' or dtdc_codigo='34')")
df = df.withColumn("dhdr_fch_emis_int", from_unixtime(unix_timestamp(col("dhdr_fch_emis_int").cast("string"),"yyyyMMdd"),"yyyy-MM-dd HH:mm:ss"))
#df = df.withColumn("dhdr_fch_emis_int",df["dhdr_fch_emis_int"].cast(StringType()))


# In[4]:


df.createOrReplaceTempView("comercial")


# In[5]:


df.schema


# In[6]:


#spark.sql("select count(*) from comercial").show()


# In[7]:


df2=spark.sql("select REPR_FECHA_INICIO_VO, CONT_RUT,CONT_RUT_REPRESENTANTE,REPR_FECHA_TERMINO_VO from dw.dw_trn_djr_representantes_e ")
df2.createOrReplaceTempView("representante")
#spark.sql(" select count(*) from representante").show()
#df2.show()


# In[8]:


df2.schema


# In[9]:


a=spark.sql("select * from comercial left join representante on comercial.dhdr_rut_recep=representante.CONT_RUT ")
a.createOrReplaceTempView("tabla")
#spark.sql(" select count(*) from tabla").show()

#a.show()


# In[10]:


b=spark.sql("select * from tabla where tabla.CONT_RUT is not null")
b.createOrReplaceTempView("tabla")
#spark.sql(" select count(*) from tabla").show()
#b.show()


# In[ ]:


df3 = spark.read.options(header=True,inferSchema=True,delimiter=",").csv("data/processed/contaminados.csv")
df3.createOrReplaceTempView("contaminados")


# In[ ]:


spark.sql("select * from tabla left join contaminados on tabla.CONT_RUT_REPRESENTANTE=contaminados.cont_rut").createOrReplaceTempView("tabla")


# In[ ]:


#spark.sql(" select count(*) from tabla").show()
#spark.sql("select * from tabla").show()


# In[ ]:


spark.sql("select dhdr_rut_emisor,dhdr_rut_recep,dhdr_iva,REPR_FECHA_INICIO_VO, dhdr_fch_emis_int,REPR_FECHA_TERMINO_VO,CONT_RUT_REPRESENTANTE,score from tabla where (dhdr_fch_emis_int>=REPR_FECHA_INICIO_VO) and (dhdr_fch_emis_int<=REPR_FECHA_TERMINO_VO or REPR_FECHA_TERMINO_VO is null) and dhdr_iva>0").createOrReplaceTempView("tabla")
#spark.sql(" select count(*) from tabla").show()


# In[ ]:


spark.sql("select dhdr_rut_emisor,dhdr_rut_recep, count(*) as count1,sum(dhdr_iva) as sum1 from tabla where score is not null group by dhdr_rut_emisor,dhdr_rut_recep").createOrReplaceTempView("aux2")
spark.sql("select dhdr_rut_emisor, dhdr_rut_recep, count(*) as count2,sum(dhdr_iva) as sum2 from tabla group by dhdr_rut_emisor,dhdr_rut_recep").createOrReplaceTempView("aux1")


# In[ ]:


spark.sql("select * from aux1").show()


# In[ ]:


spark.sql("select aux1.dhdr_rut_emisor as dhdr_rut_emisor, aux1.dhdr_rut_recep as dhdr_rut_recep, CASE WHEN count1 is null then 0 else count1 end as count1, CASE WHEN sum1 is null then 0 else sum1 end as sum1, count2, sum2 from aux1 left join aux2 on (aux1.dhdr_rut_emisor=aux2.dhdr_rut_emisor and aux1.dhdr_rut_recep=aux2.dhdr_rut_recep)").createOrReplaceTempView("tabla")
spark.sql("select * from tabla").show()


# In[ ]:


spark.sql("select dhdr_rut_emisor,dhdr_rut_recep,count1/count2 as Fa, sum1/sum2 as Fb from tabla").createOrReplaceTempView("tabla")
spark.sql("SELECT *  from tabla").show() 


# In[ ]:


spark.sql("select dhdr_rut_emisor,dhdr_rut_recep, round(Fa/2+Fb/2,5) as Fuerza from tabla").createOrReplaceTempView("fuerza")
spark.sql("select dhdr_rut_emisor as emisor,dhdr_rut_recep as receptor, Fuerza as Fi  from fuerza ").createOrReplaceTempView("fuerza")



# In[ ]:


spark.sql("SELECT *  from fuerza order by Fi asc").show() 
spark.sql("SELECT *  from fuerza order by Fi desc").show() 


# In[ ]:


representante=spark.sql("select *  from fuerza").toPandas()
representante.to_csv('data/processed/fuerza_representante.csv', index=False)


# In[ ]:





# In[ ]:




