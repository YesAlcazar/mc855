#PUT IMPORTS HERE
import wikipedia
import requests
import thread
import time
import threading
import Queue
import sets
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
OVERWRITE_STD=True
#DEFINE FILE_STDOUT
FILE_STDOUT = "py_out.txt"
#DEFINE FILE_STDERR
FILE_STDERR = "py_err.txt"



#DEFINE MULTITHREAD
PAGE_LIMIT = 256
MULTITHREAD_LIMIT = 8
FLUSH_IO_BATCH = 32
PAGE_LIMIT_SEMAPHORE = threading.Semaphore()
READY_SET = sets.Set()
ALL_SET = sets.Set()
SET_LOCK = threading.RLock()
MAIN_SEMAPHORE = threading.Semaphore()

#Main Code:
def main(sc):
    languages = {"en","pt","fr"}
    for language in languages:
        WIKI_DICT = {}
        try:
            MAIN_SEMAPHORE = threading.Semaphore(MULTITHREAD_LIMIT)
            PAGE_LIMIT_SEMAPHORE = threading.Semaphore(PAGE_LIMIT)
            wikipedia.set_lang(language)
            flang=open("wikipedia_"+language+".json","a")
            randPageNames = wikipedia.random(pages=10)
            ALL_SET.update(randPageNames)
            READY_SET.update(ALL_SET)
            for i in range(1,MULTITHREAD_LIMIT):
                thread.start_new_thread(getPages,(flang,))
                pass
            time.sleep(5)
            for i in range(1,MULTITHREAD_LIMIT):
                MAIN_SEMAPHORE.acquire()
                pass
        except wikipedia.exceptions.DisambiguationError as e:
            print >> sys.stderr, e
        except wikipedia.exceptions.PageError as e:
            print >> sys.stderr, e
        except requests.exceptions.ConnectionError as e:
            print >> sys.stderr, e
        except wikipedia.exceptions.WikipediaException as e:
            print >> sys.stderr, e
        except Exception as e:
            print >> sys.stderr, e
        pass
    return

def getPages(flang):
    MAIN_SEMAPHORE.acquire()
    #Creates a local dictionary of the pages acquired and per requests
    wikidict = {}
    i=0
    #Try to acquire a page
    while PAGE_LIMIT_SEMAPHORE.acquire(False):
        try:
            #Get a request and execute it
            addPage(wikidict)
            i+=1
            if i%FLUSH_IO_BATCH == 0:
                thread.start_new_thread(flushIO,(wikidict,flang,))
                wikidict = {}
        except Exception as e:
            print >> sys.stderr, e
    flushIO(wikidict,flang)
    MAIN_SEMAPHORE.release()

def flushIO(wikidict,flang=sys.stdout):
    try:
        print >> flang, wikidict
    except Exception as e:
        print >> sys.stderr, e

def queuePages(pageNames):
    for pageName in pageNames:
        with SET_LOCK:
            if not pageName in ALL_SET:
                ALL_SET.add(pageName)
                READY_SET.add(pageName)
        pass


def addPage(wikidict={},pageName =""):
    pageName=READY_SET.pop()
    try:
        page = wikipedia.page(pageName)
        links = page.links
        pagedict = {"pageName":pageName,"content":page.content,"links":links,"images":page.images}
        wikidict[pageName]=pagedict
        queuePages(links)
    except wikipedia.exceptions.DisambiguationError as e:
        #Common Error
        queuePages(e.options)
    except wikipedia.exceptions.PageError as e:
        print >> sys.stderr, e
    except requests.exceptions.ConnectionError as e:
        print >> sys.stderr, e
    except wikipedia.exceptions.WikipediaException as e:
        print >> sys.stderr, e
    return wikidict

#DONT TOUCH HERE
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    # Configure stdout and stderr
    if OVERWRITE_STD:
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
