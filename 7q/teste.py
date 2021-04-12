from statistics import mean
import sys
import os

teste = []

def checkIsInt(number):
    return number.isdigit()



def contarPalavras(newValues, ok):
	if newValues is not None:
		teste.append(newValues)

	return statistics.mean(teste)

if __name__ == "__main__":
    teste = checkIsInt(' dsfojdoif sdf sdf ')
    print(teste)
