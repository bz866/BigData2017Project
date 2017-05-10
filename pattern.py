from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from time import time
from datetime import datetime



#module load python/gnu/3.4.4
#export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
#export PYTHONHASHSEED=0
#export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
#pyspark


#data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
#header=data.filter(lambda l: 'CMPLNT_NUM' in l)
#Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))



if __name__ == "__main__":
    sc = SparkContext()
    data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
    header=data.filter(lambda l: 'CMPLNT_NUM' in l)
    Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))

#Crime count by year
#(year, count)

    result = Noheader.filter(lambda x: x[1]!='')\
                     .map(lambda x: [x[1][6:10],1])\
                     .reduceByKey(lambda x,y: x+y)\
                     .sortByKey()
    result.saveAsTextFile('year_crime_count.out')

#borough crime count by month
#(borough, month, count)

    result = Noheader.filter(lambda x: x[1]!='' and x[13]!='')\
                     .map(lambda x: [(x[13],x[1][0:2]),1])\
                     .reduceByKey(lambda x,y: x+y)\
                     .sortByKey()
    result.saveAsTextFile('borough_month_count.out')

#offence_desc_from_month
#((offence_desc, from_month, count))

    result = Noheader.filter(lambda x: x[1]!='' and x[7]!='')\
                     .map(lambda x: [(x[7],x[1][0:2]),1])\
                     .reduceByKey(lambda x,y: x+y)\
                     .sortByKey()\
                     .map(lambda x: (x[0][0],x[0][1],x[1]))
    result.saveAsTextFile('offence_desc_from_month.out')


#Borough_frequency without law category
#(Borough, frequency_count)

    result = Noheader.filter(lambda x: x[13]!='')\
                    .map(lambda x: [x[13],1])\
                    .reduceByKey(lambda x,y: x+y)
    result.saveAsTextFile('Borough_frequency_without_law_cat.out')

#Borough_frequency with law category
#((Borough, law_cat), frequency_count)

    result = Noheader.filter(lambda x: x[11]!='' and x[13]!='')\
                    .map(lambda x: [(x[13], x[11]),1])\
                    .reduceByKey(lambda x,y: x+y)
    result.saveAsTextFile('Borough_frequency_with_law_cat.out')

#offence_desc_borough
#(offence_desc, borough, count)

    result = Noheader.filter(lambda x: x[13]!='' and x[7]!='')\
                    .map(lambda x: [(x[7],x[13]),1])\
                    .reduceByKey(lambda x,y: x+y)\
                    .sortByKey()\
                    .map(lambda x: (x[0][0],x[0][1],x[1]))
    result.saveAsTextFile('offence_desc_borough.out')

#count by year
#(year, count)


    result = Noheader.filter(lambda x: x[1]!='')\
        .map(lambda x: [x[1][6:10],1])\
            .reduceByKey(lambda x,y: x+y)\
            .sortByKey()
    result.saveAsTextFile('year_crime_count.out')





#partition of different criminals in different police department
#plot with coordinates
    result = Noheader.filter(lambda x: x[8]!='' and x[11]!='')\
                    .map(lambda x: [(x[8],x[11]),1])\
                    .reduceByKey(lambda x,y: x+y)\
                    .map(lambda x: (x[0][0],x[0][1],x[1]))\
                    .sortBy(lambda x: x[0])

    pd_count = Noheader.filter(lambda x: x[8]!='' and x[11]!='')\
                  .map(lambda x:[x[8],1])\
                  .reduceByKey(lambda x,y:x+y)\
                  .sortByKey()

    [(101, 'MISDEMEANOR', 438130), (104, 'FELONY', 220), (105, 'FELONY', 11648), (106, 'FELONY', 18376), (107, 'FELONY', 51), (109, 'FELONY', 154046), (110, 'FELONY', 786), (111, 'MISDEMEANOR', 863), (112, 'FELONY', 704), (113, 'MISDEMEANOR', 70185)]
    [('101', 438130), ('104', 220), ('105', 11648), ('106', 18376), ('107', 51), ('109', 154046), ('110', 786), ('111', 863), ('112', 704), ('113', 70185)]

#department numbers in different borough

    result = Noheader.filter(lambda x: x[8]!='' and x[13]!='')\
        .map(lambda x: [(x[8], x[13]), 1])\
        .reduceByKey(lambda x,y: x+y)\
        .sortByKey()\
        .map(lambda x: [x[0][1],x[1]])\
        .reduceByKey(lambda x,y: x+y)

    result_2 = Noheader.filter(lambda x: x[13]!='')\
        .map(lambda x: [x[13],1])\
        .reduceByKey(lambda x,y: x+y)

#    [(('101', 'BRONX'), 104926), (('101', 'BROOKLYN'), 138015), (('101', 'MANHATTAN'), 86766), (('101', 'QUEENS'), 88822), (('101', 'STATEN ISLAND'), 19577), (('104', 'BRONX'), 50), (('104', 'BROOKLYN'), 43), (('104', 'MANHATTAN'), 72), (('104', 'QUEENS'), 50), (('104', 'STATEN ISLAND'), 5), (('105', 'BRONX'), 2745), (('105', 'BROOKLYN'), 4076), (('105', 'MANHATTAN'), 1650), (('105', 'QUEENS'), 2767), (('105', 'STATEN ISLAND'), 410), (('106', 'BRONX'), 4981), (('106', 'BROOKLYN'), 5619), (('106', 'MANHATTAN'), 4284), (('106', 'QUEENS'), 2913), (('106', 'STATEN ISLAND'), 577), (('107', 'BRONX'), 10), (('107', 'BROOKLYN'), 15), (('107', 'MANHATTAN'), 6), (('107', 'QUEENS'), 15), (('107', 'STATEN ISLAND'), 5), (('109', 'BRONX'), 40063), (('109', 'BROOKLYN'), 52756), (('109', 'MANHATTAN'), 27092), (('109', 'QUEENS'), 29588), (('109', 'STATEN ISLAND'), 4522), (('110', 'BRONX'), 201), (('110', 'BROOKLYN'), 264), (('110', 'MANHATTAN'), 149), (('110', 'QUEENS'), 135), (('110', 'STATEN ISLAND'), 37), (('111', 'BRONX'), 236), (('111', 'BROOKLYN'), 283), (('111', 'MANHATTAN'), 186), (('111', 'QUEENS'), 125), (('111', 'STATEN ISLAND'), 33), (('112', 'BRONX'), 162), (('112', 'BROOKLYN'), 264), (('112', 'MANHATTAN'), 129), (('112', 'QUEENS'), 116), (('112', 'STATEN ISLAND'), 33), (('113', 'BRONX'), 17773), (('113', 'BROOKLYN'), 25262), (('113', 'MANHATTAN'), 10562), (('113', 'QUEENS'), 13811), (('113', 'STATEN ISLAND'), 2776)]










