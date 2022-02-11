# Databricks notebook source
# MAGIC %md
# MAGIC ## Teste de Contexto

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação
# MAGIC "Sc" é uma conexão com o Spark Cluster criado no momento da execução do comando PySpark. Estamos no Python, portanto, temos que nos conectar ao cluster Spark.

# COMMAND ----------

#Contexto criado automaticamente

# COMMAND ----------

print(sc)

# COMMAND ----------

print(sc.version)

# COMMAND ----------

#Criando o contexto importando o SparkContext

from pyspark import SparkContext

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

print(sc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando o contexto com opções

# COMMAND ----------

#Rodar em arquivos py
#Dá erro em cluster simulado onde só há uma máquina

from pyspark import SparkContext
sc = SparkContext(master="local", appName="Aplicacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto SQL
# MAGIC Permite processamento de arquivos JSON e integra-se com o Hive. Permite também a consulta no estilo sQL utilizando JDBC

# COMMAND ----------

print(sqlContext)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de Dados

# COMMAND ----------

#Criando RDDs através de dados

dados_nativos = sc.parallelize([('Brasil', 5), ('Alemanha', 4), ('Italia', 4), ('Argentina', 2)])

# COMMAND ----------

#Criando RDDs através de arquivos locais

#path = 'C:\\Spark\\projetos\\datasets\\airlines.csv'
path = 'dbfs:/FileStore/henrique.mesquita@blueshift.com.br/airlines.csv'

dados_arquivo = sc.textFile(path)

# COMMAND ----------

#Leitura do HDFS Hadoop

dados_hdfs = "hdfs:///user/diretorio1/diretorio2/arquivo.csv"

# COMMAND ----------

#Leitura do DBFS Databricks

dados_dbfs = "dbfs:/FileStore/henrique.mesquita@blueshift.com.br/airlines.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificando a Tipagem

# COMMAND ----------

type(dados_nativos)

# COMMAND ----------

type(dados_arquivo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imprimindo os Dados

# COMMAND ----------

dados_nativos

# COMMAND ----------

dados_arquivo

# COMMAND ----------

dados_nativos.collect()

# COMMAND ----------

#Dez primeiras linhas do arquivo CSV apontado no DBFS pelo seu Spark API path

dados_arquivo.take(10)

#Nesta cédula o retorno está sendo realizado a partir do sourceFile apontando para o DBFS, uma vez que a simulação destes notebooks são totalmente executadas em ambiente Databricks, não localmente utilizando Spark e Jupyter.

# COMMAND ----------

# MAGIC %md
# MAGIC Operações com RDD's no próximo notebook