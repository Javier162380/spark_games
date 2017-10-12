from pyspark import SparkConf, SparkContext
from operator import add
from sys import argv

path=argv[1]

conf = SparkConf().setAppName("My App")
conf = conf.setMaster("local[*]")
sc = SparkContext(conf = conf)
textRRDD = sc.textFile(path)
singername=textRRDD.first()
print("el cantante es {0}".format(str(singername)))
sentences=textRRDD.flatMap(lambda x:x.split('\n')).filter(lambda x:x[1:])
sentences=sentences.persist()
number_of_sentences=sentences.count()
print("el numero de frases es {0}".format(str(number_of_sentences)))

chorus_sentences=sentences.map(lambda x: (x,1)).reduceByKey(add).filter(lambda x:x[1]>3)
print("el estribillo es  :")
chorus=chorus_sentences.collect()
for i in chorus:
	print(i[0])

words=sentences.flatMap(lambda x:x.split(" "))
number_of_words=words.count()
print("el numero de palabras es {0}".format(number_of_words))

singername_repeated_in_song=sentences.filter(lambda x:singername in x).count()
print("el numero de veces que el cantante repite su nombre es: {0}".format(singername_repeated_in_song))



#print("El estribillo de la cancion es  {0}")