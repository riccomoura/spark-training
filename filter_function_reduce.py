# Databricks notebook source
# MAGIC %md
# MAGIC ##Utilizando a função Filter
# MAGIC A função filter() é aplicada à uma lista de elementos, retornando valores booleanos, True ou False, de acordo com o filtro aplicado.

# COMMAND ----------

#Definindo um array com a função built-in range()
lista = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# COMMAND ----------

print(lista)

# COMMAND ----------

type(lista)

# COMMAND ----------

#Função para verificar se um número é par

def par(num):
    return num % 2 == 0

# COMMAND ----------

par(500)

# COMMAND ----------

# MAGIC %md
# MAGIC Revisando

# COMMAND ----------

#Aplicaremos a função utilizando o map como no notebook anterior de Map para booleanos
list(map(par, lista))

# COMMAND ----------

# MAGIC %md
# MAGIC A função filter tem o mesmo princípio de mapeamento da função map, porém ela retorna apenas os valores onde a expressão é verdade

# COMMAND ----------

#Filter
list(filter(par, lista))

# COMMAND ----------

#Map
list(filter(par, lista))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introdução ao Reduce
# MAGIC A função reduce() aplica uma função a uma sequência de elementos, até reduzir a sequência a um único elemento

# COMMAND ----------

from functools import reduce

# COMMAND ----------

soma = reduce((lambda x,y: x + y), range(9000000))

# COMMAND ----------

soma