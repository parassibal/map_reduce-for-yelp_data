import sys
import json
import operator
import pyspark
import time
from pyspark import SparkContext

input_path_access_review=sys.argv[1]
input_path_access_business=sys.argv[2]
output_path_access_q1=sys.argv[3]
output_path_access_q2=sys.argv[4]
sc=SparkContext("local","task3").getOrCreate()

def data_read_review(sc,input_path_access_review):
  data_review=sc.textFile(input_path_access_review).map(lambda a:json.loads(a))
  return(data_review)

def data_read_business(sc,input_path_access_business):
  data_review=sc.textFile(input_path_access_business).map(lambda a:json.loads(a))
  return(data_review)

data_review=data_read_review(sc,input_path_access_review)
data_business=data_read_business(sc,input_path_access_business)

def data_collect(data_review,data_business):
  temp1=data_business.map(lambda a:(a['business_id'],a['city']))
  temp=data_review.map(lambda a:(a['business_id'],a['stars']))
  return(temp,temp1)

star_data,city_data=data_collect(data_review,data_business)
cumm_result=city_data.join(star_data).map(lambda a:a[1])

def map_red_sort(res):
  temp=res.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))
  return(temp.mapValues(lambda a:a[0]/a[1]))

temp=map_red_sort(cumm_result.mapValues(lambda a:(a,1)))

#Method1 collect and then sort
time1=time.time()
result_a1=temp.collect()
result_a=sorted(result_a1,key=lambda a:(-a[1],a[0]))[0:10]
len_a=len(result_a)
diff1=time.time()-time1

#Method2 sort and then collect
temp=map_red_sort(cumm_result.mapValues(lambda a:(a,1)))
time2=time.time()
result_b1=temp.sortBy(lambda a:(-a[1],a[0]),ascending=True)
result_b=result_b1.collect()[0:10]
diff2=time.time()-time2

out_res=["city,","stars\n"]
out_file1=open(output_path_access_q1,"w+")
out_file1.write(str(out_res[0]))
out_file1.write(str(out_res[1]))
for i in result_a[0:len_a]:
  out_file1.write(str(i[0])+","+str(i[1])+"\n")
out_file1.close()
 
out_res={}
out_res["m1"]=diff1
out_res["m2"]=diff2
out_res["reason"]="Python sort is faster that spark sort because spark sort requires shuffling of data"
out_file2=open(output_path_access_q2,"w+")
json.dump(out_res,out_file2)
out_file2.close()