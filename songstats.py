from pyspark import SparkConf, SparkContext
from sys import argv

def words(singername,sentence):
	global singer
	if (singername == sentence):
		singer+=1
	return sentence.split(" ")

def main(sc):
	
	#path from the file we are going to analyze.
	path=argv[1]
	#I upload the file, the first sentence of the file file is always the singer name.
	textRRDD = sc.textFile(path)
	singername=textRRDD.first()
	print("el cantante es {0}".format(str(singername)))
	#I filter the most the sentences.
	sentences=textRRDD.flatMap(lambda x:x.split('\n')).filter(lambda x:x[1:])
	#this RDD should stay in memory, because we are going to execute multiple actions over it.
	sentences=sentences.persist()
	#number of sentences 
	number_of_sentences=sentences.count()
	print("el numero de frases es {0}".format(str(number_of_sentences)))
	#chorus
	chorus_sentences=sentences.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>3)
	print("el estribillo es  :")
	chorus=chorus_sentences.collect()
	for i in chorus:
		print(i[0])
	
	#this code could be more eficient if we use a shared variable.
	words=sentences.flatMap(lambda x:x.split(" "))
	number_of_words=words.count()
	print("el numero de palabras es {0}".format(number_of_words))
	singername_repeated_in_song=sentences.filter(lambda x:singername in x).count()
	print("el numero de veces que el cantante repite su nombre es: {0}".format(singername_repeated_in_song))

	#shared variable.
	#we create an accumulator.
	singer = sc.accumulator(0)
	number_of_words=sentences.map(lambda x:words(singername,x))
	words=number_of_words.count()
	singer_name_repetead=singer.value
	#words=number_of_words.count()
	print("el numero de palabras es {0}".format(words))
	print("el numero de veces que el cantante repite su nombre es:  {0}".format(singer_name_repetead))
if __name__=="__main__":
   	# Configure Spark
   	conf = SparkConf().setAppName("GAME")
   	conf = conf.setMaster("local[*]")
   	sc = SparkContext(conf=conf)
   	main(sc)
