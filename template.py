#PUT IMPORTS HERE


#***DONT TOUCH HERE
from pyspark import SparkConf, SparkContext
from datetime import datetime
import sys
TEST_URL = "spark://143.106.73.43:7077"
PROD_URL = "spark://143.106.73.44:7077"
CALZOLARI_URL = "spark://143.106.73.61:7077"
LOCAL_URL = "local [*]"
#****END DONT TOUCH HERE

#**************************
#*   TWEAK THE DEFINES    *
#**************************

#DEFINE APP_NAME
APP_NAME = "TEMPLATE"
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


#DONT TOUCH HERE
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    # Configure stdout and stderr
    sys.stderr = open(FILE_STDERR,"a")
    sys.stdout = open(FILE_STDOUT,"a")
    EXECUTION_ID = "\n\nAPP_NAME: %s\nAPP_ID: %s\nMASTER_URL: %s\nNOW: %s\nUTC: %s\n\n" % (APP_NAME, sc.applicationId, MASTER_URL, datetime.now(), datetime.utcnow())
    sys.stderr.write (EXECUTION_ID)
    sys.stdout.write (EXECUTION_ID)
    # Execute Main functionality
    main(sc)
    print "\n"
#****END DONT TOUCH HERE
