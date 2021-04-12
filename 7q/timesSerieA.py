import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# start-master.sh
# start-slave.sh spark://posgrad-vm:7077

# !!!! COMANDO PARA EXECUTAR CODIGO !!!!
# /opt/spark/bin/spark-submit timesSerieA.py localhost 9999

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 2) # 5 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta


	# d) Exibir apenas nomes de times da série A do brasileiro que chegam via 
	# 	 streaming (netcat) em uma janela de 30 segundos
	def verificarTime(time):
		serieA = [
			'américa-mg', 'america-mg', 
			'atlético', 'atletico', 
			'atlético-go', 'atletico-go',
			'atlético-mg', 'atlético-mg', 
			'bahia', 'bragantino', 
			'ceará', 'ceara',	
			'chapecoense', 'corinthians', 
			'cuiabá', 'cuiaba', 
			'flamengo', 'fortaleza', 
			'fluminense', 'grêmio', 
			'gremio', 'internacional', 
			'juventude', 'palmeiras', 
			'santos', 'são paulo', 
			'sao paulo', 'sport'		
		]

		if(serieA.__contains__(time)):
			return time
		else:
			return None


	timesSerieA = lines.flatMap(lambda line: line.split(" "))\
                    .filter(lambda word: verificarTime(word.lower()) is not None)\
                    .map(lambda word : (word, 1))\
                    .reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 30, 2)

	
	timesSerieA.pprint()

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
