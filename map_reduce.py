# Databricks notebook source
# MAGIC %md
# MAGIC #Utilizando a função Map
# MAGIC A função map() tem o objetivo de mapear valores, de forma a realizar transformações mais rapidamente, sem a necessidade de iterações. Essa característica é muito utilizada e útil no processamento de Big Data, mais precisamente com os frameworks Hadoop Map Reduce e Spark

# COMMAND ----------

#Definindo uma lista de valores

lista = [2, 4, 6, 8, 10]

# COMMAND ----------

#Imprimindo a lista
lista

# COMMAND ----------

#Função simples, para somar mais 10 a cada número

def soma_dez(num):
    return num + 10

# COMMAND ----------

soma_dez(20)

# COMMAND ----------

#Para aplicar a função na lista definida acima, necessitamos criar um loop for para percorrê-la

for elemento in lista:
    print(soma_dez(elemento))

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando a função map

# COMMAND ----------

#Com a função map() esse processo é facilitado e a função é mapeada sendo aplicada a cada elemento da lista, como um mapeamento - elemento - valor. No exemplo abaixo, função map armazenando em variável.

lista_map = map(soma_dez, lista)

# COMMAND ----------

#Forma simples. Atenção ao valor retornado.

map(soma_dez, lista)

# COMMAND ----------

#A função map() sempre retorna um objeto iterável

print(lista_map)

# COMMAND ----------

#Um objeto iterável significa que se pode executar um FOR para cada um dos elementos, assim como é o caso de uma lista. Ele pode ser convertido em lista e assim podemos visualizar o seu conteúdo.

print(list(lista_map))

# COMMAND ----------

# MAGIC %md
# MAGIC Outras aplicações

# COMMAND ----------

#Poderíamos por exemplo, criar um range de dados

numeros = range(10)

for n in numeros:
    print(n)

# COMMAND ----------

#Para realizar um cast nos números eu necessito de outro for

for n in numeros:
    m = float(n)
    print(m)

# COMMAND ----------

#Podemos encurtar esses passos
#Estou aplicando a função float() em cada elemento do range

lista_map = map(float, range(10))

# COMMAND ----------

lista_map


# COMMAND ----------

print(list(lista_map))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aninhando funções com MAP
# MAGIC Objetivo: 
# MAGIC 
# MAGIC Imprimir os números pares de 0 a 1000, em cuma única linha de código, convertendo para float.

# COMMAND ----------

# MAGIC %md
# MAGIC Método tradicional:

# COMMAND ----------

#Passo 01 - Criar a lista
lista = range(0, 1000, 2)

lista

# COMMAND ----------

#Iterando a lista e convertendo

for i in lista:
    print(float(i))

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando Map

# COMMAND ----------

print(list(map(float, range(0, 1000, 2))))

# COMMAND ----------

# MAGIC %md
# MAGIC Função MAP com booleanos

# COMMAND ----------

#Definindo uma lista de valores

lista = [1, 2, 4, 6, 7, 8, 10, 13]

# COMMAND ----------

#Imprimindo a lista
lista

# COMMAND ----------

#Função simples para somar mais 10 em cada número

def impar(num):
    if num % 2 > 0:
        return True
    else:
        return False

# COMMAND ----------

impar(21)

# COMMAND ----------

#Dará erro pois não se pode aplicar uma função em uma lista, é preciso iterar usando FOR
impar(lista)

# COMMAND ----------

#Iterando para criar um loop e percorrer a lista com FOR

for elemento in lista:
    print(impar(elemento))

# COMMAND ----------

#Usando o Map para retornar objeto iterável

map(impar, lista)

# COMMAND ----------

#Com a função mapeada anteriormente, armazenamos em uma variável

lista_map = map(impar, lista)

# COMMAND ----------

#A função map() SEMPRE retorna um objeto iterável, mas o conteúdo ainda não pode ser visualizado.

print(lista_map)

# COMMAND ----------

#O objeto iterável pode ser convertido em lista, e assim podemos visualizar seu conteúdo.

print(list(lista_map))