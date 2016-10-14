#PUT IMPORTS HERE
import wikipedia
import requests
import thread
import time
import threading
import Queue
import sets
import json
#***DONT TOUCH HERE
from pyspark import SparkConf, SparkContext
from datetime import datetime,tzinfo
import sys
TEST_URL = "spark://143.106.73.43:7077"
PROD_URL = "spark://143.106.73.44:7077"
CALZOLARI_URL = "spark://143.106.73.61:7077"
LOCAL_URL = "local[*]"
CREATION_TIME = datetime.utcnow()
#****END DONT TOUCH HERE

#**************************
#*   TWEAK THE DEFINES    *
#**************************

#DEFINE APP_NAME
APP_NAME = "Wiki to File"
#DEFINE MASTER_URL
MASTER_URL = TEST_URL
#DEFINE OVERWRITE_STD
OVERWRITE_STDIO=True
#DEFINE FILE_STDOUT
FILE_STDOUT = "py_out.txt"
#DEFINE FILE_STDERR
FILE_STDERR = "py_err.txt"



#DEFINE MULTITHREAD
PAGE_LIMIT = 4096
MULTITHREAD_LIMIT = 4
FLUSH_IO_BATCH = 128
RANDOM_TIMES = 5
SET_LOCK = threading.RLock()

#Main Code:
def main(sc):
    languages = {"en","fr","pt","de"}
    wikipedia.set_rate_limiting(True)
    multiLimitRange = range(MULTITHREAD_LIMIT)
    for language in languages:
        try:
            wikipedia.set_lang(language)
            flang=open("wikipedia_"+language+".json","a")
            allSet = sets.Set()
            for i in xrange(RANDOM_TIMES):
                try:
                    allSet.update(wikipedia.random(pages=10))
                except Exception as e:
                    print >> sys.stderr, e
            readySet = sets.Set()
            readySet.update(allSet)
            getPages_threads={i:threading.Thread(target=getPages,args=(flang,allSet,readySet)) for i in multiLimitRange}
            for i in multiLimitRange:
                try:
                    getPages_threads[i].start()
                except Exception as e:
                    print >> sys.stderr, e
            for i in multiLimitRange:
                try:
                    if getPages_threads[i].isAlive():
                        getPages_threads[i].join()
                except Exception as e:
                    print >> sys.stderr, e
            print "== %s: %d Done ==" % (language,len(allSet))
        except wikipedia.exceptions.PageError as e:
            print >> sys.stderr, e
        except requests.exceptions.ConnectionError as e:
            print >> sys.stderr, e
        except wikipedia.exceptions.WikipediaException as e:
            print >> sys.stderr, e
        except Exception as e:
            print >> sys.stderr, e
        pass

def getPages(flang,allSet,readySet):
    #Creates a local dictionary of the pages acquired and per requests
    wikidict = {}
    #Try to acquire a page
    i=0
    while addPage(wikidict,allSet,readySet):
        i +=1
        if i%FLUSH_IO_BATCH == 0:
            try:
                my_thread = threading.Thread(target=flushIO,args=(wikidict,flang))
                my_thread.start()
                wikidict = {}
            except Exception as e:
                print >> sys.stderr, e
        pass
    flushIO(wikidict,flang)

def flushIO(wikidict,flang=sys.stdout):
    try:
        print >> flang, json.dumps(wikidict, sort_keys=True,indent=4)
    except Exception as e:
        print >> sys.stderr, e

def queuePages(pageNames,allSet,readySet):
    for pageName in pageNames:
        if (len(allSet)<=PAGE_LIMIT):
            with SET_LOCK:
                if not pageName in allSet:
                    allSet.add(pageName)
                    readySet.add(pageName)
        pass


def addPage(wikidict,allSet,readySet):
    pageName = ""
    try:
        if len(readySet)>0:
            pageName=readySet.pop().encode('utf-8')
        else:
            return False;
    except Exception as e:
        print >> sys.stderr, e
        return False;
    try:
        page = wikipedia.page(pageName)
        links = page.links
        pagedict = {"pageName":pageName,"content":page.content,"links":links,"images":page.images,"categories":page.categories}
        wikidict[pageName]=pagedict
        queuePages(links,allSet,readySet)
    except wikipedia.exceptions.DisambiguationError as e:
        print >> sys.stderr, e
        queuePages(e.options,allSet,readySet)
    except wikipedia.exceptions.PageError as e:
        print >> sys.stderr, e
    except requests.exceptions.ConnectionError as e:
        print >> sys.stderr, e
    except wikipedia.exceptions.WikipediaException as e:
        print >> sys.stderr, e
    return True;

#DONT TOUCH HERE
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    # Configure stdout and stderr
    if OVERWRITE_STDIO:
        sys.stderr = open(FILE_STDERR,"a")
        sys.stdout = open(FILE_STDOUT,"a")
    EXECUTION_ID = "\n\nAPP_NAME: %s\nAPP_ID: %s\nMASTER_URL: %s\nUTC: %s\n\n" % (APP_NAME, sc.applicationId, MASTER_URL, CREATION_TIME)
    sys.stderr.write (EXECUTION_ID)
    sys.stdout.write (EXECUTION_ID)
    # Execute Main functionality
    main(sc)
    #try:
        #main(sc)
    #except Exception as e:
        #print >> sys.stderr, e
#****END DONT TOUCH HERE
