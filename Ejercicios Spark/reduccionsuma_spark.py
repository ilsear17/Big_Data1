import findspark
findspark.init()
from pyspark import SparkContext

# Crea un contexto Spark
sc = SparkContext("local", "EjemploRDD")

# Crea un RDD a partir de una lista de números
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Realiza una reducción para sumar todos los elementos
suma = rdd.reduce(lambda x, y: x + y)

# Muestra el resultado
print(suma)

# Detén el contexto Spark
#sc.stop() no se debe parar el cluster
