import findspark
findspark.init()
from pyspark import SparkContext

# Crea un contexto Spark
sc = SparkContext("local", "EjemploRDD")

# Carga un archivo de texto como un RDD
rdd = sc.textFile("archivo.txt")


# Realiza una transformación: filtra líneas que contienen la palabra "Python"
rdd_filtrado = rdd.filter(lambda linea: "Python" in linea)


# Realiza una acción: muestra los resultados
print(rdd_filtrado.collect())

# Detén el contexto Spark
#sc.stop() no se debe parar el cluster
