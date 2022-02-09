import sys
import json
import operator
import pyspark
import time
from pyspark import SparkContext,SparkConf

input_path_access=sys.argv[1]
output_path_access=sys.argv[2]
partition=sys.argv[3]
partition=int(partition)
sc=SparkContext("local","task2").getOrCreate()

def data_read(sc,input_path_access):
  data_review=sc.textFile(input_path_access).map(lambda a:json.loads(a))
  return(data_review)

time1=time.time()
data_review=data_read(sc,input_path_access)

def get_partition(output_result_business):
  temp=output_result_business.getNumPartitions()
  return(temp)

def red_ord(temp1):
  temp=temp1.reduceByKey(operator.add).takeOrdered(10,key=lambda a:(-a[1],a[0]))
  return(temp)

def total_top10_business_default(data_review):
  temp=data_review.map(lambda a:(a['business_id'],a['review_id']))
  temp1=temp.distinct()
  total_count=red_ord(temp1.map(lambda a:(a[0],1)))
  return(total_count,temp)

def n_get_items(output_result_business):
  output_result_business=output_result_business.map(len)
  return(output_result_business)

output_result_business1,output_result_business2=total_top10_business_default(data_review)
diff=time.time()-time1
output_result_default={}
output_result_default["n_partition"]=get_partition(output_result_business2)
output_result_default["n_items"]=(n_get_items(output_result_business2.glom())).collect()
output_result_default["exe_time"]=diff

output_result={}
output_result['default']=output_result_default

def total_top10_business_customized(data_review,partition,output_result_business2):
  temp=output_result_business2.partitionBy(partition,lambda a:ord(a[:1]))
  temp1=temp.distinct()
  total_count=red_ord(temp1.map(lambda a:(a[0],1)))
  return(total_count,temp)

time1=time.time()
output_result_business3,output_result_business4=total_top10_business_customized(data_review,partition,output_result_business2)
diff=time.time()-time1
output_result_customized={}
output_result_customized["n_partition"]=get_partition(output_result_business4)
output_result_customized["n_items"]=(n_get_items(output_result_business4.glom())).collect()
output_result_customized["exe_time"]=diff
output_result['customized']=output_result_customized
output_file=open(output_path_access,"w")
json.dump(output_result,output_file)
output_file.close()


