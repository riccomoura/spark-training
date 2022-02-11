# Databricks notebook source
# MAGIC %md
# MAGIC ##List Comprehension
# MAGIC É a forma com que fazemos filtros e mapeamentos dentro de uma lista. Assemelham-se às funções filter() e map().

# COMMAND ----------

lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# COMMAND ----------

print(lista)

# COMMAND ----------

type(lista)

# COMMAND ----------

dobro = lambda x: x*2
dobro(lista)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Agora multiplicaremos cada item da lista por 2 de forma eficiente, mas ainda em Python. No notebook anterior a tentativa deu erro e clonamos os elementos da lista ao tentar multiplicar por dois.

# COMMAND ----------

#Imprimindo uma lista com o quadrado de todos os números da lista

lst = []

for i in lista:
    num = i**2
    lst.append(num)
    
print(lst)

# COMMAND ----------

# MAGIC %md
# MAGIC Usando list comprehensions

# COMMAND ----------

#Elevar ao quadrado para cada item da lista

[item ** 2 for item in lista]

# COMMAND ----------

#Para cada item na lista, verificar se o item é par

[item for item in lista if item % 2 == 0]

# COMMAND ----------

#Elevar ao quadrado e para cada item da lista, verificar se é par

[item ** 2 for item in lista if item % 2 == 0]

# COMMAND ----------

#Exercicio: Para cada elemento da lista, programar que o mesmo seja multiplicado por 2

[item * 2 for item in lista]

#"[item * 2" é o retorno, o bloco "for item in lista]" é a programação ou primeira etapa. A leitura que se faz é "Para cada item na lista, multiplique o item por 2"

# COMMAND ----------

#Converta para float cada item da lista e imprima os maiores ou iguais a 3

lista = ['1', '2', '3', '4', '5']

[float(item) for item in lista if item >= '3']

# COMMAND ----------

#Converta para int cada item da lista e imprima os maiores ou iguais a 3
[item for item in lista if item >= '3']