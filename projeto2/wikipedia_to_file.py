import wikipedia
import sys
import time

language = "fr"
if len(sys.argv)>=2:
	language = str(sys.argv[1])


filename = time.strftime("%d%m%Y%H%M") + language
f = file(filename, 'w')
wikipedia.set_lang(language)
#titouan = wikipedia.page("New York")
#text = titouan.content.encode('utf-8')
#f.write(text)

for x in range(0,100) :
	try:
		pageName = wikipedia.random()
		print  str(x) + " : " + pageName + "\n"
		page = wikipedia.page(pageName)
		text = page.content.encode('utf-8')
		f.write(text)
	except Exception as exception:
		print "No loaded\n"

f.close()
