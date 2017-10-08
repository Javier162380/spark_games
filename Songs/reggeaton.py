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

	path=createdir('/Users/javierllorente/Desktop/Spark/Songs')
	a=Reggeatton('https://www.letras.com/j-balvin/mi-gente/')
	b=a.songlyrics("cnt-letra p402_premium","article")
	name=a.songname()
	nametxt=name.replace(' ','-')
	singer=a.singer()
	file=open('/Users/javierllorente/Desktop/Spark/Songs/'+nametxt+'.txt','w',encoding='utf-8')
	file.write(singer)
	file.write(b)
	file.close()

if __name__ == '__main__':
	main()
	