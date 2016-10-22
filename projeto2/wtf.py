#***Essential Data and Imports (Do not modify, except USE_SPARK)
#DEFINE USE_SPARK
USE_SPARK = True
try:
    from pyspark import SparkConf, SparkContext
except ImportError as e:
    if USE_SPARK:
        raise e
from datetime import datetime
import sys
class MasterURL():
    test = "spark://143.106.73.43:7077"
    prod = "spark://143.106.73.44:7077"
    calzolari = "spark://143.106.73.61:7077"
    local = "local[*]"
CREATION_TIME = datetime.utcnow()

#****Tweak Defines (Modify freely, just don't erase)
#DEFINE APP_NAME
APP_NAME = "WikitoFile Local"
#DEFINE CHOSEN_MASTER
CHOSEN_MASTER = MasterURL.test
#DEFINE OVERWRITE_STD
OVERWRITE_STDIO=True
#DEFINE FILE_STDOUT
FILE_STDOUT = "py_out.txt"
#DEFINE FILE_STDERR
FILE_STDERR = "py_err.txt"

#****Put Extra Imports Here
import wikipedia
import requests
import thread
import time
import threading
import sets
import json

#****Put Globals Here
PAGE_LIMIT = 256
MULTITHREAD_LIMIT = 4
FLUSH_IO_BATCH = 128
RANDOM_TIMES = 5

#****Write Main Code:
def main(sc=None):
    languages = {"en"}#{"en","fr","pt","de"}
    wikipedia.set_rate_limiting(True)
    multiLimitRange = range(MULTITHREAD_LIMIT)
    for language in languages:
        try:
            wikipedia.set_lang(language)
            allSet = sets.Set()
            for i in xrange(RANDOM_TIMES):
                try:
                    allSet.update(wikipedia.random(pages=10))
                except Exception as e:
                    print >> sys.stderr, e
            readySet = sets.Set()
            readySet.update(allSet)
            getPages_threads={i:threading.Thread(target=getPages,args=(language,allSet,readySet)) for i in multiLimitRange}
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

def getPages(language,allSet,readySet):
    #Creates a local dictionary of the pages acquired and per requests
    wikidict = {}
    linkdict = {}
    #Try to acquire a page
    i=0
    try:
        while addPage(wikidict,linkdict,allSet,readySet):
            i +=1
            if i%FLUSH_IO_BATCH == 0:
                try:
                    my_thread = threading.Thread(target=dictsToJson,args=(wikidict,linkdict,language))
                    my_thread.start()
                    wikidict = {}
                    linkdict = {}
                except Exception as e:
                    print >> sys.stderr, e
            pass
    except Exception as e:
        print >> sys.stderr, e
    dictsToJson(wikidict,linkdict,language)

IO_LOCK = threading.RLock()
DTJ_COUNTER = 0
def dictsToJson(wikidict,linkdict,language):
    currCounter = 0
    with IO_LOCK:
        DTJ_COUNTER +=1
        currCounter = DTJ_COUNTER
    flushIO(wikidict,"wiki",language,currCounter)
    flushIO(linkdict,"link",language,currCounter)

def dictToJson(dict,fileID,language="",uniqueID=0):
    try:
        #data\wiki_en_20161021_120000_000001.json
        fjson = open ("data\\%s_%s_%s_%06d.json" % (fileID, language, CREATION_TIME.strftime("%Y%m%d_%H%M%S") , uniqueID), "w")
        json.dump(dict,fjson, sort_keys=True,indent=4)
        fjson.close()
    except Exception as e:
        print >> sys.stderr, e
        return False
    finally:
        return True

SET_LOCK = threading.RLock()
def queuePages(pageNames,allSet,readySet):
    for pageName in pageNames:
        if (len(allSet)<=PAGE_LIMIT):
            with SET_LOCK:
                if not pageName in allSet:
                    allSet.add(pageName)
                    readySet.add(pageName)
        pass

def addPage(wikidict,linkdict,allSet,readySet):
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
        linkdict[pageName]=links
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
    except Exception as e:
        print >> sys.stderr, e
        time.sleep(1)
    return True;

#***Configure Out and Error, and print ExecutionID
def configPrint():
    if OVERWRITE_STDIO:
        sys.stderr = open(FILE_STDERR,"a")
        sys.stdout = open(FILE_STDOUT,"a")
    EXECUTION_ID = "\n\nAPP_NAME: %s\nAPP_ID: %s\nMASTER_URL: %s\nUTC: %s\n\n" % (APP_NAME, sc.applicationId, CHOSEN_MASTER, CREATION_TIME)
    sys.stderr.write (EXECUTION_ID)
    sys.stdout.write (EXECUTION_ID)

#***Executor of Code
if USE_SPARK:
    #With Spark
    if __name__ == "__main__":
        # Configure Spark
        conf = SparkConf().setMaster(CHOSEN_MASTER).setAppName(APP_NAME)
        sc   = SparkContext(conf=conf)
        # Configure stdout and stderr
        configPrint()
        # Execute Main functionality
        main(sc)
else:
    #Without Spark
    configPrint()
    main()
