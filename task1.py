import sys
import json
import operator
import pyspark
from pyspark import SparkContext,SparkConf

def data_read(sc,input_path_access):
  data_review=sc.textFile(input_path_access).map(lambda a:json.loads(a))
  return(data_review)

def cal_total_review(data_review):
  total_count=data_review.map(lambda a:a['review_id'])
  return(total_count)

def total_review_year(data_review):
  total_count=data_review.filter(lambda a:a['date'][:4]=='2018')
  return(total_count)

def total_distinct_user(data_review):
  total_count=data_review.map(lambda a:a['review_id'])
  return(total_count.distinct())

def red_ord(temp1):
  temp=temp1.reduceByKey(operator.add).takeOrdered(10,key=lambda a:(-a[1],a[0]))
  return(temp)

def total_top10_user(data_review):
  temp=data_review.map(lambda a:(a['user_id'],a['review_id']))
  temp1=temp.distinct()
  total_count=red_ord(temp1.map(lambda a:(a[0],1)))
  return(total_count)

def total_distinct_business(data_review):
  total_count=data_review.map(lambda a:a['business_id'])
  return(total_count.distinct())

def total_top10_business(data_review):
  temp=data_review.map(lambda a:(a['business_id'],a['review_id']))
  temp1=temp.distinct()
  total_count=red_ord(temp1.map(lambda a:(a[0],1)))
  return(total_count)

input_path_access=sys.argv[1]
output_path_access=sys.argv[2]
sc=SparkContext("local","task1").getOrCreate()
data_review=data_read(sc,input_path_access)
output_result={}
output_result['n_review']=(cal_total_review(data_review)).count()
output_result['n_review_2018']=(total_review_year(data_review)).count()
output_result['n_user']=(total_distinct_user(data_review)).count()
output_result['top10_user']=total_top10_user(data_review)
output_result['n_business']=(total_distinct_business(data_review)).count()
output_result['top10_business']=total_top10_business(data_review)
output_file=open(output_path_access,"w")
json.dump(output_result,output_file)
output_file.close()
