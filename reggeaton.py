from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
from os import makedirs

class Reggeatton(object):
	"""docstring for ClassName"""
	def __init__(self, url):

		self.url = url

	def singer(self):
		try:
			return str(self.url.split('.com/')[1].split('/')[0])
		except:
			raise "url format not correct"

	def songname(self):
		try:
			return str(self.url.split('.com/')[1].split('/')[1].replace('-',' ').replace('/',''))
		except:
			raise "url format not correct"
		
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

	path=argv[1]
	url=argv[2]
	tag=argv[3]
	item=argv[4]

	path=createdir(path)
	a=Reggeatton(url)
	b=a.songlyrics(tag,item)
	name=a.songname()
	nametxt=name.replace(' ','-')
	singer=a.singer()
	file=open(path+nametxt+'.txt','w',encoding='utf-8')
	file.write(singer)
	file.write(b)
	file.close()

if __name__ == '__main__':
	main()
	