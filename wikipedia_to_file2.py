#PUT IMPORTS HERE
import wikipedia
import requests
import thread
import time
import threading
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
MASTER_URL = LOCAL_URL
#DEFINE OVERWRITE_STD
OVERWRITE_STD=True
#DEFINE FILE_STDOUT
FILE_STDOUT = "py_out.txt"
#DEFINE FILE_STDERR
FILE_STDERR = "py_err.txt"
#
#Extra Code HERE:

#Main Code:
def main(sc):
    #************************
    #*   WRITE CODE HERE    *
    #************************
    #languages = wikipedia.languages()
    #for language in languages:
    wikidict = {}
    language = "en"
    print language
    wikipedia.set_lang(language)
    try:
        randPageNames = wikipedia.random(pages=10)
        addPagesRecursive(randPageNames,wikidict)
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
    finally:
        time.sleep(5*PAGE_RECURSIVE_LIMIT)
        with MULTITHREAD_DICT_LOCK:
            print >> open("wikipediaTest.txt","w"), wikidict

#DEFINE MULTITHREAD
PAGE_RECURSIVE_LIMIT = 1
MULTITHREAD_PAGE = True
MULTITHREAD_DELAY = 0.5
MULTITHREAD_DICT_LOCK = threading.RLock()

def addPagesRecursive(pageNames,wikidict={},recursive=PAGE_RECURSIVE_LIMIT):
    if MULTITHREAD_PAGE:
        for pageName in pageNames:
            if not pageName in wikidict:
                thread.start_new_thread(addPageRecursive,(pageName,wikidict,recursive-1))
                time.sleep(MULTITHREAD_DELAY)
            pass
    else:
        for pageName in pageNames:
            if not pageName in wikidict:
                addPageRecursive(pageName,wikidict,recursive-1)
            pass

def addPageRecursive(pageName,wikidict={},recursive=PAGE_RECURSIVE_LIMIT):
    #pageName=pageName.encode('utf-8')
    try:
        print pageName
        pagedict = {"pageName":pageName}
        page = wikipedia.page(pageName)
        #pagedict["page"]=page
        links = page.links
        pagedict["links"]=links
        with MULTITHREAD_DICT_LOCK:
            wikidict[pageName]=pagedict
        if recursive !=0:
            addPagesRecursive(links,wikidict,recursive-1)
    except wikipedia.exceptions.DisambiguationError as e:
        #Common Error
        addPagesRecursive(e.options,wikidict,recursive-1)
    except wikipedia.exceptions.PageError as e:
        print >> sys.stderr, e
    except requests.exceptions.ConnectionError as e:
        print >> sys.stderr, e
    except wikipedia.exceptions.WikipediaException as e:
        print >> sys.stderr, e
    except Exception as e:
        print >> sys.stderr, e
    return

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
