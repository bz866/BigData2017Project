from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from dateutil.parser import parse
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from dateutil.parser import parse
from datetime import datetime, timedelta


if __name__ == "__main__":
    sc = SparkContext()
    data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
    header=data.filter(lambda l: 'CMPLNT_NUM' in l)
    Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))


    FDT = "%m/%d/%Y"
    FCT = "%H:%M:%S"
    total_duration = Noheader.filter(lambda x: x[1]!=''and x[2]!=''and x[3]!=''and x[4]!='' and x[2]!='24:00:00' and x[4]!= '24:00:00' and x[13]!='' )\
        .map(lambda x: [(datetime.combine(datetime.strptime(x[1],FDT),(datetime.strptime(x[2],FCT).time()))),(datetime.combine(datetime.strptime(x[3],FDT),(datetime.strptime(x[4],FCT).time()))),x[13]])\
        .map(lambda x: [x[2],x[0]-x[1]])\
            .reduceByKey(lambda x,y: x+y)

    count_borough = Noheader.filter(lambda x: x[1]!=''and x[2]!=''and x[3]!=''and x[4]!='' and x[2]!='24:00:00' and x[4]!= '24:00:00' and x[13]!='')\
        .map(lambda x: [x[13],1])\
        .reduceByKey(lambda x,y: x+y)

    total_duration.saveAsTextFile('total_duration.out')
    count_borough.saveAsTextFile('count_borough.out')





