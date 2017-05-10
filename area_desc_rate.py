from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from dateutil.parser import parse
from time import time
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()
    data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
    header=data.filter(lambda l: 'CMPLNT_NUM' in l)
    Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))

#(area,desc,rate)
#area_code col_15, desc col_8
    result_1 = Noheader.filter(lambda x: x[14]!='' and x[7]!='')\
                        .map(lambda x: [(x[14],x[7]),1])\
                        .reduceByKey(lambda x,y: x+y)\
                        .sortByKey()\
                        .map(lambda x:[x[0][0],(x[0][1],x[1])])
    
    result_2 = Noheader.filter(lambda x: x[14]!='' and x[7]!='')\
                        .map(lambda x: [x[14],1])\
                        .reduceByKey(lambda x,y:x+y)\
                        .sortByKey()
    
    result = result_1.join(result_2)\
                    .map(lambda x: [x[0],x[1][0][0],x[1][0][1]/x[1][1]])
    
    result.saveAsTextFile('area_desc_rate.out')
