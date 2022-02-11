# Databricks notebook source
# MAGIC %md
# MAGIC ## Entendendo as RDDs com operações de Big Data nos textos do livro Primo Basílio e na Bíblia Sagrada

# COMMAND ----------

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# COMMAND ----------

print(sc)

# COMMAND ----------

#Criação de uma RDD simples

rdd1 = sc.parallelize([1, 2, 3])
rdd1.collect()

# COMMAND ----------

#Utilizando o NumPy

import numpy as np
A = np.array(range(100))
A

# COMMAND ----------

rdd2 = sc.parallelize(np.array(range(100)))
rdd2.take(10)

# COMMAND ----------

#Verificando a paralelização
#Observe que a paralelização da RDD não ocorreu se o teste for realizado no ambiente Spark local. Ela não será dividida. Se for ambiente Databricks, o glom fará a divisão entre [] de cada bloco de processament e será mostrado na saída.

rdd3 = sc.parallelize(np.array(range(100)))
print(rdd3.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### O particionamento das RDDs é feito por padrão pelo tamanho do cluster, mas também podemos especificar explicitamente, de preferência com o número de Cores do PC

# COMMAND ----------

#Paralelizando explicitamente passando parâmetro de blocos de processamento dentro do parallelize()

rdd4 = sc.parallelize(np.array(range(100)), 10)

# COMMAND ----------

#Verificando a paralelização. O array será dividido em 10 blocos de processamento independentes.

print(rdd4.glom().collect())

# COMMAND ----------

#Conferindo e validando o número de partições inferidas anteriormente
print(rdd4.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Paralelizando Arquivos

# COMMAND ----------

#Lendo o Primo Basílio - Eça de Queiroz
#Leitura realizada de path local no caso de Spark com Jupyter e DBFS no caso de Databricks

rddBasilio = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Basilio_UTF8.txt")

print(rddBasilio.take(1000))

# COMMAND ----------

print(rddBasilio.getNumPartitions())

# COMMAND ----------

rddBasilioPart = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Basilio_UTF8.txt", 100)

# COMMAND ----------

print(rddBasilioPart.getNumPartitions())

# COMMAND ----------

print(rddBasilioPart.glom().collect())

# COMMAND ----------

print(rddBasilioPart.glom())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura da Bíblia

# COMMAND ----------

#Verifique se o arquivo está em ANSI
#O objetivo dessa visualização é reconhecer os problemas implícitos na codificação ANSI

rddANSI = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Biblia_ANSI.txt")

print(rddANSI.take(1000))

# COMMAND ----------

#Altere o arquivo para UTF-8
#Salve o arquivo como UTF-8 para trabalhar com ele e poder visualizar corretamente os dados de texto

rddBiblia = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Biblia_UTF8.txt")

print(rddBiblia.take(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformações
# MAGIC 1 - São operações em um RDD que devolvem um novo RDD
# MAGIC 2 - Normalmente executam uma função anônima (lambda) sobre cada um dos elementos do RDD
# MAGIC 3 - Operam sobre lazy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilizando o Intersect

# COMMAND ----------

dados1 = sc.parallelize(["A", "B", "C", "D", "E"])
dados2 = sc.parallelize(["A", "E", "I", "O", "U"])

result = dados1.intersection(dados2)
result.take(10)

# COMMAND ----------

jose = rddBiblia.filter(lambda linha: "José" in linha)

# COMMAND ----------

maria = rddBiblia.filter(lambda linha: "Maria" in linha)

# COMMAND ----------

biblia = jose.intersection(maria)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ações
# MAGIC 1 - Devolvem um resultado
# MAGIC 2 - Faz todas as transformações anteriores serem executadas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilizando o takeSample para gerar amostras

# COMMAND ----------

#Criando um sorteio de números com definição de range e usando takeSample pra determinar o número de elementos na amostra.

#Criando um RDD com range de números
rdd5 = sc.parallelize(np.array(range(100)))

#Contando o número de elementos
contador = rdd5.count()

#Imprimindo o número de elementos
print("Números sorteados compreendidos entre 0 e {0}".format(contador))
                                               
#Utilizando um for para imprimir cada elemento
#True - Possibilita elementos REPETIDOS
#Número - Indica o tamanho da amostra
for l in rdd5.takeSample(True, 10):
    print(l)

# COMMAND ----------

type(rdd5)

# COMMAND ----------

#Teste com o dataset Biblia UTF-8
#Amostra com 5 linhas não repetíveis
lines = biblia.count()

print("Número de linhas: {0}".format(lines))

for l in biblia.takeSample(False, 5):
    print(l)

# COMMAND ----------

#Amostra de 8 linhas não repetíveis
for l in biblia.takeSample(False, 8):
    print(l)

# COMMAND ----------

#Amostra de 10 linhas repetíveis
for l in biblia.takeSample(True, 10):
    print(l)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compreendendo todo o código

# COMMAND ----------

#Aqui, criamos o RDD lendo o arquivo
rddBiblia = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Biblia_UTF8.txt")

#Criamos duas variáveis utilizando filter e lambda, trazendo apenas as linhas com determinada palavra
jose = rddBiblia.filter(lambda linha: "José" in linha)
maria = rddBiblia.filter(lambda linha: "Maria" in linha)


#Criamos uma segunda RDD com intersection
biblia = jose.intersection(maria)

#Criamos uma variável int com o número de linhas da RDD resultado da intersecção
lines = biblia.count()
print()

#Imprimimos o número de linhas
print("O número de linhas com José e Maria é de {0}".format(lines))
print()

#Iteramos a RDD com a intersection utilizando takeSample, que retorna um objeto iterável
for l in biblia.takeSample(False, 5):
    print(l)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Podemos utilizar variáveis

# COMMAND ----------

rddBiblia = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Biblia_UTF8.txt")

A= "Jesus"
B = "Cristo"

linhas1 = rddBiblia.filter(lambda linha: A in linha)
linhas2 = rddBiblia.filter(lambda linha: B in linha)

inter = linhas1.intersection(linhas2)

#Aqui conta-se o total de vezes que Jesus + Cristo aparecem juntos no dataset
lines = inter.count()
print("Número de linhas: " + str(lines))

#O for retorna somente uma amostra de 10 momentos onde A e B convergem no nome "Jesus Cristo"
for l in inter.takeSample(False, 10):
    print(l)


# COMMAND ----------

# MAGIC %md
# MAGIC ##E se quisermos todas as linhas?

# COMMAND ----------

rddBiblia = sc.textFile("dbfs:/FileStore/henrique.mesquita@blueshift.com.br/Biblia_UTF8.txt")

A= "Jesus"
B = "Cristo"

linhas1 = rddBiblia.filter(lambda linha: A in linha)
linhas2 = rddBiblia.filter(lambda linha: B in linha)

inter = linhas1.intersection(linhas2)

#Aqui conta-se o total de vezes que Jesus + Cristo aparecem juntos no dataset
lines = inter.count()
print("Número de linhas: " + str(lines))

#Altera o parâmetro de amostragem passando um count que percorre todas as linhas do dataset, mostrando TODAS as vezes na contagem de linhas onde aparecem o nome "Jesus Cristo"
for l in inter.takeSample(False, lines):
    print(l)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Podemos utilizar o collect()

# COMMAND ----------

#Podemos iterar?

A = "Jesus"
B = "Cristo"

linhas1 = rddBiblia.filter(lambda linha: A in linha)
linhas2 = rddBiblia.filter(lambda linha: B in linha)

inter = linhas1.intersection(linhas2)

lines = inter.count()

print("Número de linhas: " + str(lines))
print("Tipo da RDD inter: " + str(type(inter)))
print()

#O collect somente mostra as linhas, mas não retorna um objeto iterável, o qual possa ser realizada uma alteração em todos os elementos do mesmo. Para um objeto iterável, é necessário utilizar o FOR.
for l in inter.collect():
    print(l)

# COMMAND ----------

