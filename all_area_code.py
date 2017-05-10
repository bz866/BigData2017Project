from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from time import time
from datetime import datetime


if __name__ == "__main__":
    sc = SparkContext()
    data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
    header=data.filter(lambda l: 'CMPLNT_NUM' in l)
    Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))

#all area code number
    result = Noheader.filter(lambda x: x[14]!='' and x[7]!='' )\
        .map(lambda x: [x[14],1])\
        .reduceByKey(lambda x,y: x+y)\
        .map(lambda x: x[0])\
        .sortByKey()
    result.saveAsTextFile('all_area_code.out')
