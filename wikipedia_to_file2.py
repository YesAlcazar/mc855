#PUT IMPORTS HERE
import wikipedia
import requests
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
#DEFINE FILE_STDOUT
FILE_STDOUT = "py_out.txt"
#DEFINE FILE_STDERR
FILE_STDERR = "py_err.txt"

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
        for randPageName in randPageNames:
            addPageRecursive(randPageName,wikidict)
    except wikipedia.exceptions.DisambiguationError as e:
        print >> sys.stderr, e
        print >> sys.stderr, e.options
    except wikipedia.exceptions.PageError as e:
        print >> sys.stderr, e
    except requests.exceptions.ConnectionError as e:
        print >> sys.stderr, e
    except wikipedia.exceptions.WikipediaException as e:
        print >> sys.stderr, e
    except Exception as e:
        print >> sys.stderr, e
    print >> open("wikipedia.txt","w"), wikidict

#DEFINE LIMIT
PAGE_RECURSIVE_LIMIT = 2

def addPageRecursive(pageName,wikidict={},recursive=PAGE_RECURSIVE_LIMIT):
    pageName=pageName.encode('utf-8')
    if wikidict.has_key(pageName):
        return wikidict
    try:
        print pageName
        pagedict = {"pageName":pageName}
        page = wikipedia.page(pageName)
        pagedict["page"]=page
        links = page.links
        pagedict["links"]=links
        wikidict[pageName]=pagedict
        if recursive !=0:
            for link in links:
                addPageRecursive(link,wikidict,recursive-1)
                pass
    except wikipedia.exceptions.DisambiguationError as e:
        print >> sys.stderr, e
        print >> sys.stderr, e.options
    except wikipedia.exceptions.PageError as e:
        print >> sys.stderr, e
    except requests.exceptions.ConnectionError as e:
        print >> sys.stderr, e
    except wikipedia.exceptions.WikipediaException as e:
        print >> sys.stderr, e
    except Exception as e:
        print >> sys.stderr, e
    return wikidict

#DONT TOUCH HERE
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    # Configure stdout and stderr
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
