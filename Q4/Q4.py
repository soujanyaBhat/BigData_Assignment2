from pyspark import SparkContext
from pyspark import SparkConf
'''
List the  business_id , full address and categories of the Top 10 businesses using the average ratings.  

Sample output:
business id               full address           categories                                    avg rating
xdf12344444444,      CA 91711       List['Local Services', 'Carpet Cleaning']	5.0

'''

conf = SparkContext.setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)
review = sc.textFile("/FileStore/tables/review.csv").map(lambda line: line.split("::"))
business = sc.textFile("/FileStore/tables/business.csv").map(lambda line: line.split("::"))
review=review.map(lambda x:(str(x[2]),x[3])).map(lambda x:(str(x[0]),float(x[1])))
business=business.map(lambda x:(str(x[0]),str(x[1]),x[2]))
avg_rating= review.combineByKey(lambda x: (x,1), lambda y,x:(y[0]+x, y[1]+1), lambda y,z: (y[0]+z[0],y[1]+z[1]))
avg_rating= avg_rating.map(lambda x:(x[0],x[1][0]/x[1][1])).map(lambda x: (x[0],x[1]))
result=avg_rating.join(business.map(lambda x:(x[0],(x[1],x[2]))))
result=result.map(lambda x:(x[1][0],(x[0],x[1][1])))
top_10 = result.top(10, key=lambda x: x)
print(top_10)