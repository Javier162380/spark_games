from pyspark import SparkConf, SparkContext
from sys import  argv

def main(sc):
    #paramenters
    sentiment_file_path=argv[1]
    songs_path=argv[2]
    #sentiment analysis file.
    sentiment_file = open(sentiment_file_path, 'r')
    file_erase_tabs = [line.replace('\t', ' ').replace('\n', '') for line in sentiment_file]
    dict_file = {(i.split(' ')[0]).upper(): float(i.split(' ')[1]) for i in file_erase_tabs if '.' in i.split(' ')[1]}
    #songs
    songs=sc.wholeTextFiles(songs_path)
    #analysis
    sentiment_songs=songs.flatMap(lambda x:x[1].split('\n')).flatMap(lambda x:(x.split(' ')))
    analysis_results=sentiment_songs.flatMap(lambda x: (x, dict_file.get(str(x).upper(), None)))

if __name__=="__main__":
    # Configure Spark
    conf = SparkConf().setAppName("sentiment")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    main(sc)