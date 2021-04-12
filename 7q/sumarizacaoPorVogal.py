import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# !!!! COMANDO PARA EXECUTAR CODIGO !!!!
# /opt/spark/bin/spark-submit sumarizacaoPorVogal.py localhost 9999

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 5) # 5 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta



	def contarPalavras(newValues, lastSum):
		if lastSum is None :
			lastSum = 0
		return sum(newValues, lastSum)

	# c) Acumular a contagem de palavras à medida que os dados vão chegando 
	#    via streaming e sumarizar pela quantidade de palavras que existem com um 
	#    certo número de vogais. Exemplo: 
	#    	Entrada: Paraguai, Uruguai, Brasil, Argentina, Cuba, Peru
	# 		Saída: 
	# 			2: 3 // (3 palavras com 2 vogais: Brasil, Cuba, Peru)
	# 			4: 1 // (1 palavra com 4 vogais: Argentina)
	# 			5: 2 // (2 palavras com 5 vogais: Paraguai, Uruguai)
	contagemDePalavras = lines.flatMap(lambda line: line.split(" "))\
							.map(lambda word: (sum(Counter(re.sub('[^aeiou]', '', word.lower())).values()), 1))\
							.updateStateByKey(contarPalavras)
	contagemDePalavras.pprint() 

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
