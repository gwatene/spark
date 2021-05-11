from __future__ import print_function
from __future__ import division
from pyspark.sql import SparkSession
import datetime
import uuid
from datetime import date , timedelta,time
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import random
import string
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import input_file_name
import shutil




path=""

def getSparkSession():
    if ("sparksession" is not globals()):
            globals()["sparksession"] = SparkSession \
                .builder \
                .appName("imsi reports") \
                .config("spark.driver.extraClassPath", "/opt/mapr/lib/ojdbc6.jar")\
                .getOrCreate()
    return globals()["sparksession"]


def getFileMetadata():
        df=getSparkSession().read.format("jdbc") \
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","table_name") \
                .option("user", "") \
                .option("password","")\
                .load()
        return df
        
ef getFiles(path):
        ddf=getSparkSession().read.text(path)
#       ddf=path()
        files=getFileMetadata()
        #ddf=getSparkSession().streamingContext.textFileStream(path)
        df2=ddf.withColumn('value',regexp_replace('value', '!time', '|time'))\
            .withColumn("filename", input_file_name())
        df3=df2.withColumn('value',explode(split(df2['value'],"\\|")))
        #df3.show(5,False)
        df3.createOrReplaceTempView('timsi')
        files.createOrReplaceTempView('src_files')
        fg= getSparkSession().sql("select substr(filename, instr(filename,':///')+4) as filename,substr(SPLIT(value,'\\,')[0], instr(SPLIT(value,'\\,')[0],'=')+1,9) as SRC_DT,substr(SPLIT(value,'\\,')[3], instr(SPLIT(value,'\\,')[3],'=')+1) as SRC_DS_ORG,substr(SPLIT(value,'\\,')[26], instr(SPLIT(value,'\\,')[26],'=')+1) as SRC_DS_RESPNS,substr(SPLIT(value,'\\,')[0], instr(SPLIT(value,'\\,')[0],'=')+12,10) as SRC_TM,current_timestamp() as timestamp,substr(SPLIT(value,'\\,')[38], instr(SPLIT(value,'\\,')[38],'=')+1) CSTMR_NR from timsi \
 where substr(filename, instr(filename,':///')+4) not in (SELECT file_name FROM src_files) ")
        #fg.show(10,False)
        #fd.createOrReplaceTempView('files')
        return fg


def getAggregate():
        df=getFiles(path)
#       gf=df.groupBy(['organisation','time']).count()
        gf=df.select('SRC_DT','SRC_TM','SRC_DS_ORG','SRC_CSTMR_NR','SRC_DS_RESPNS','timestamp','filename')
        getProcessedFiles()
        return gf
        #gf.show(20,False)


def getProcessedFiles():
        # gf=df.groupBy(['organisation','time']).count()
        df=getFiles(path)
        df.createOrReplaceTempView('files')
        fg=getSparkSession().sql("select filename as FILE_NAME,count(*) REC_COUNT from files group by filename")
        return fg

def writeTableSource():
        getAggregate().write.format("jdbc") \
                .mode('append') \
                .option("url","jdbc:oracle:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","table_name") \
                .option("user", "") \
                .option("password","") \
                .save()

def writeTableFiles():
        getProcessedFiles().write.format("jdbc") \
                .mode('append') \
                .option("url","jdbc:oracle:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","<table_anme>") \
                .option("user", "") \
                .option("password","") \
                .save()



writeTableSource()

writeTableFiles()


#getFiles(path)
