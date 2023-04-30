from pyspark import SparkContext
import sys
import string

def word_split(line):
    for c in string.punctuation+"¿!":
        line = line.replace(c, ' ')
    line = line.lower()
    return line.split()

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(filename)

        words_rdd = data.map(lambda x: len(x.split()))
        print('RESULTS-----------------')
        print('words_count', words_rdd.sum())
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
