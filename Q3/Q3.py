from pyspark import SparkContext
from pyspark import SparkConf
'''
List the 'user id' and 'rating' of users that reviewed businesses located in “Stanford”
Sample output
                                                   
User id	Rating
0WaCdhr3aXb0G0niwTMGTg	4.0
'''

conf = SparkContext.setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

review = sc.textFile("/FileStore/tables/review.csv").map(lambda line: line.split("::"))
business = sc.textFile("/FileStore/tables/business.csv").map(lambda line: line.split("::"))
all_business = business.map(lambda x:(x[0],x[1]))
stanford_business=all_business.filter(lambda x:"Stanford, CA" in x[1]).coalesce(1)
combined_result = stanford_business.map(lambda x:(x[0],x[1])).join(review.map(lambda x:(x[2],(x[1],x[3]))))
final_result=combined_result.map(lambda x:(x[1][1][0], x[1][1][1])).coalesce(1).collect()
print(final_result)