import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# !!!! COMANDO PARA EXECUTAR CODIGO !!!!
# /opt/spark/bin/spark-submit contagemInfo.py localhost 9999

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingInfoCount")
	ssc = StreamingContext(sc, 2) # 2 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta


	# e) Exibir o número de vezes em que a palavra ‘INFO’ aparece nos últimos 20 segundos.
	contagemInfo = lines.flatMap(lambda line: line.split(" "))\
                    .filter(lambda word:"INFO" in word.upper())\
                    .map(lambda word : (word, 1))\
                    .reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 20, 2)


	contagemInfo.pprint()

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
