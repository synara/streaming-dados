import sys
import re
from collections import Counter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 3) # 5 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/posgrad/checkpoint") # tolerancia a falhas

	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta


	count = lines.countByWindow(15, 3).collet()
	print(int(count))

	media = lines.reduceByWindow(
                lambda x, y: int(x) / int(y),
                lambda x, y: int(x) * int(y),
                15,
                3
        )

	media.pprint()

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
