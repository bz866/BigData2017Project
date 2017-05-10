from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext

def replace_format_duration(x):
    x = x.replace('datetime.timedelta(','')\
        .replace('(','')\
        .replace(')','')
    return x

def replace_format_number_in_borough(x):
    x = x.replace('(','')\
         .replace(')','')
    return x

if __name__ == "__main__":
    sc = SparkContext()
    duration=sc.textFile('total_duration.out')
    number_in_borough = sc.textFile('count_borough.out')

    duration = duration.map(lambda x: replace_format_duration(x))\
                       .map(lambda x: x[0:-1].split(','))\
                       .map(lambda x: [x[0],abs(int(x[1])),int(x[2])])\
                       .map(lambda x: [x[0],x[1]*24*60*60+x[2]])
    duration = duration.collect()
#[["'QUEENS'", 643768019], ["'BROOKLYN'", 296698314], ["'STATEN ISLAND'", 130896384], ["'BRONX'", 446867802], ["'MANHATTAN'", 153018540]]
    number_in_borough = number_in_borough.map(lambda x: replace_format_number_in_borough(x))\
                                         .map(lambda x: x[0:-1].split(','))\
                                         .map(lambda x: [x[0],int(x[1])])
    number_in_borough = number_in_borough.collect()

    time_list =[]
    for i in range(0,5):
        time_list.append(duration[i][0])
        time_list.append(duration[i][1]/number_in_borough[i][1])


    rdd = sc.textFile('average_compliant_duration_in_each_borough.tx')
    sc.parallelize(time_list).saveAsTextFile('average_compliant_duration_in_each_borough.out')

