# Databricks notebook source
# MAGIC %md
# MAGIC #Função Lambda
# MAGIC 
# MAGIC As funções lambda não são definidas, portanto, não recebem nome. São conhecidas por isso como funções anônimas. A vantagem das funções lambda são uma menor utilização de memória, já que são executadas "on the fly" e deixam o código mais limpo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função convencional

# COMMAND ----------

# MAGIC %md
# MAGIC Com 3 linhas de código

# COMMAND ----------

def quadrado(num):
    a = num**2
    return a

# COMMAND ----------

quadrado(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Com 2 linhas de código

# COMMAND ----------

def quadrado(num):
    return num**2

# COMMAND ----------

quadrado(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Com 1 linha de código

# COMMAND ----------

def quadrado(num): return num**2

# COMMAND ----------

quadrado(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lambda

# COMMAND ----------

# MAGIC %md
# MAGIC Gerando o mesmo resultado das funções anteriores, mas agora em lambda. Principal vantagem de uso aplicada na circunstância de poucos ou raros casos de uso de uma função, que é o fato de não ficar armazenado na memória a criação da função. 

# COMMAND ----------

lambda num: num**2

# COMMAND ----------

quadrado = lambda num: num** 2

# COMMAND ----------

quadrado(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Exemplo: Mapeamento de posição em string

# COMMAND ----------

#Retornando o último elemento de uma string
ultimo = lambda s: s[-1]


#Função criada e já removida da memória após execução

# COMMAND ----------

ultimo("HENRIQUE")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lambda Booleana

# COMMAND ----------

#Numero impar
impar = lambda x: x%2 > 0

# COMMAND ----------

impar(3)

# COMMAND ----------

#Numero maior que 10
maior = lambda x: x > 10

# COMMAND ----------

maior(11)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Listas

# COMMAND ----------

# MAGIC %md
# MAGIC Resultado inesperado no interesse de multiplicar os itens de uma lista na função lambda. Observe que, para multiplicar os itens de uma lista por 2 ao invés de duplicar os itens da lista, é necessário utilizar uma função MAP, que não estará neste notebook ainda.

# COMMAND ----------

lista = [1, 2, 3, 4, 5]

# COMMAND ----------

#Multiplicar os itens da lista pelo valor definido na função lambda. Não dará certo.
dobro = lambda x: x*2

# COMMAND ----------

#Aqui temos que utilizar a função MAP, do contrário os itens serão duplicados e não multiplicados por 2.
dobro(lista)

# COMMAND ----------

# MAGIC %md
# MAGIC Retornando a soma de X e Y

# COMMAND ----------

soma = lambda x,y: x+y

# COMMAND ----------

soma(10,10)

# COMMAND ----------

#Ordenando inversamente

reverse = lambda s: s[::-1]

# COMMAND ----------

reverse("HENRIQUE")