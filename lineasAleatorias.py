from pyspark import SparkContext
import sys
import string
import random

porcentaje = 40

def concatena(lst):
	l = ""
	for i in lst:
		l = l+"\n"+i
	return l
def main(filename):
	with SparkContext() as sc:
		sc.setLogLevel("ERROR")
		data = sc.textFile(filename)
		dado = random.randint(0, 100)
		if dado <= porcentaje:
			lineas = data.count()
			porCiento = (lineas // 100)*porcentaje
			resultado = data.take(porCiento)
			devuelve = concatena(resultado)
			with open("quijote_s05.txt", "a") as archivo:
				archivo.write(devuelve)
			
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
