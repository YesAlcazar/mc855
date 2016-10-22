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
CHOSEN_MASTER = MasterURL.local
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
SET_LOCK = threading.RLock()

#****Write Main Code:
def main(sc=None):
    fileID="test"
    language = "en"
    try:
        fjson=open("data\\"+fileID+"_"+language+"_"+datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")+".json","w")
        json.dump({"Name":["A","E"],"Names":["a","b","c","d","e"]},fjson,sort_keys=False,indent=4)
        fjson.close()
    except Exception as e:
        print >> sys.stderr, e
        return False
    finally:
        return True

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
