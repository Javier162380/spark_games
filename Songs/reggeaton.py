from urllib.request import urlopen
from bs4 import BeautifulSoup
from os import makedirs
from sys import argv

class Reggeatton(object):
	"""This class is perform for recovering song lyrics. """
	def __init__(self, url):

		self.url = url


	def singer(self):
		try:
			return str(self.url.split('.com/')[1].split('/')[0])
		except:
			raise ValueError("url format not correct").

	def songname(self):
		try:
			return str(self.url.split('.com/')[1].split('/')[1].replace('-',' ').replace('/',''))
		except:
			raise ValueError("url format not correct").
		
	def songlyrics(self,tag,item):

		url=urlopen(self.url)
		parser=BeautifulSoup(url,'lxml')
		lyrics=''
		try:
			for i in parser.find(class_=tag).findAll(item):
				lyrics+=i.get_text('\n')
				lyrics.lstrip()
		except Exception as e:
			print('error al acceder a la tag o item. {0}'.format(e))
			lyrics=None
		return lyrics
			

def createdir(path):

	try:
		makedirs(path)
	except OSError:
		pass

def main():

	#we insert the parameters.
	path=argv[1]
	url=argv[2]
	tag=argv[3]
	item=argv[4]

	#we create a directory where we are going to store the songs.
	path=createdir(path)

	#we create an instance for our class, and get the info we want.
	reggeatton=Reggeatton(url)
	lyrics=reggeatton.songlyrics(tag,item)
	name=reggeatton.songname()
	nametxt=name.replace(' ','-')
	singer=reggeatton.singer

    #we insert the data in a txt file.
	file=open(path+nametxt+'.txt','w',encoding='utf-8')
	file.write(singer)
	file.write(lyrics)
	file.close()

if __name__ == '__main__':
	main()
	