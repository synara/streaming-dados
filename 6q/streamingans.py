from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':
    spark = SparkSession.builder.appName("streamingans").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    #/opt/spark/bin/spark-submit streamingans.py
    #print("{} {}".format(nome, sobre)) exemplo de interpolação

    df = spark.read.json("ans.json", multiLine  = "true")
    #df.show()

    #a) Qual a média de valor investido nos municípios nos anos de 2010 e 2011? 
    base_2010_2011 =  df.filter((df["ano"] == "2010") | (df["ano"] == "2011"))  
    #base_2010_2011.show()
    valorTotalInvestido = base_2010_2011.select(sum("valor")).take(1)[0]["sum(valor)"]
    mediaValorInvestido = valorTotalInvestido/base_2010_2011.count()
    print("A média de valor investido nos municípios nos anos de 2010 e 2011 foi {}.".format(mediaValorInvestido)) #.show()

    #b) Qual o ID do município do IBGE que recebeu mais aportes, ou seja, mais investimentos ao longo de todos os anos? 
    #voos15.groupBy("DEST_COUNTRY_NAME").sum("count")
    # .withColumnRenamed("sum(count)", "total_destino")
    # .sort(desc("total_destino")).limit(5).take(5)

    municipioComMaisApotes = df.groupBy("municipio_ibge")\
                                .sum("valor")\
                                .withColumnRenamed("sum(valor)", "total_investido")\
                                .sort(desc("total_investido"))\
                                .select("municipio_ibge")\
                                .limit(1).take(1)[0]["municipio_ibge"]

    print('O município do IBGE que mais recebeu aportes foi o de ID {}.'.format(municipioComMaisApotes))


    #c) Quais os 10 municípios que menos receberam aportes ao longo de todos os anos? 
    municipiosMenosAportes = df.groupBy("municipio_ibge")\
                                .sum("valor")\
                                .withColumnRenamed("sum(valor)", "total_investido")\
                                .sort(asc("total_investido"))\
                                .select("municipio_ibge")\
                                .limit(10).take(10)

    #print(municipiosMenosAportes)

    print('Abaixo, os 10 municípios que menos receberam aportes: ')
    for i in range(len(municipiosMenosAportes)):
        print("ID:  {};".format(municipiosMenosAportes[i]["municipio_ibge"]))