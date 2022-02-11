# Databricks notebook source
# MAGIC %md
# MAGIC ## Reduce
# MAGIC Aplica uma função a todos os elementos de uma lista, até que os mesmos sejam reduzidos a um só

# COMMAND ----------

#A função reduce() não faz parte das funções built in do Python, temos que importar a biblioteca functools

from functools import reduce

# COMMAND ----------

elementos = range(1, 100)

# COMMAND ----------

print(elementos)

# COMMAND ----------

#Função para acumular os valores (somar um pelo outro) dentro de uma lista. Esse acumulado é o princípio do Reduce, somar todos os valores dentro de uma lista e reduzir a um único.

def acumulador(lista):
    acumulado = 0
    for item in lista:
        acumulado = item + acumulado
    return acumulado

# COMMAND ----------

print(acumulador(elementos))

# COMMAND ----------

# MAGIC %md
# MAGIC Aplicando a função reduce()

# COMMAND ----------

soma = reduce((lambda x,y: x + y), elementos)

# COMMAND ----------

soma

# COMMAND ----------

# MAGIC %md
# MAGIC Determinando o maior número da lista com reduce()

# COMMAND ----------

lista = [470, 11, 42, 102, 13, 4, 645, 87]
reduce(lambda a,b: a if (a > b) else b, lista)

# COMMAND ----------

lista = [470, 11, 42, 102, 13, 4, 645, 87]

f = lambda a,b: a if (a > b) else b
reduce(f, lista)

# COMMAND ----------

# MAGIC %md
# MAGIC Determinando o menor número da lista com reduce()

# COMMAND ----------

f = lambda a,b: a if (a < b) else b
reduce(f, lista)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilizando reduce como função do Spark

# COMMAND ----------

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

rddList = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

soma = rddList.reduce(lambda acumulador, numero: acumulador + numero)
print(soma)

# COMMAND ----------

#Aplicando redução com multiplicação

multi = rddList.reduce(lambda acumulador, numero: acumulador * numero)
print(multi)

# COMMAND ----------

#Podemos também definir uma função

def soma(acum, n):
    return acum + n

resultado = rddList.reduce(soma)
print(resultado)

# COMMAND ----------

rddList = sc.parallelize(range(100000000), 10)

soma = rddList.reduce(lambda acumulador, numero: acumulador + numero)
print(soma)