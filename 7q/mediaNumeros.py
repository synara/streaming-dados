import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from statistics import mean
teste = []

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="avgNumbers")
	ssc = StreamingContext(sc, 3) # 5 segundos de intervalo de tempo

	#/opt/spark/bin/spark-submit mediaNumeros.py localhost 9999
	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	avg = lines.flatMap(lambda line: line.split(" "))\
				.filter(lambda number: number.isdigit())\
				.map(lambda number: (number, 1))\
				.reduceByWindow(
					lambda x, y: ((int(x[0]) + int(y[0])), (int(x[1]) + int(y[1]))),
					lambda x, y: ((int(x[0]) - int(y[0])), (int(x[1]) - int(y[1]))),
					15,
					3
					)\
				.filter(lambda number: number[1] > 0)\
				.map(lambda number: number[0]/number[1])

				#para não dar division by 0
				#.filter(lambda number: number[1] > 0)


	avg.pprint()
	#count.pprint()

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
