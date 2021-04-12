import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# !!!! COMANDO PARA EXECUTAR CODIGO !!!!
# /opt/spark/bin/spark-submit somaCaracteresNaoVazios.py localhost 9999

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 5) # 5 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	# # b) Exibir a soma dos caracteres não vazios de uma frase inserida via netcat. 
	# # 	Exemplo:
	# # 		Entrada: Streaming de dados
	# # 		Saída: 16 caracteres

	somaCaracteresNaoVazios = lines.map(lambda line: len(line.replace(" ", "")))
	somaCaracteresNaoVazios.pprint()

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
