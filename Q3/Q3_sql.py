from pyspark.sql.functions import split
from pyspark.sql.functions import udf, desc
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

review = sc.textFile("FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF()
business = sc.textFile("FileStore/tables/business.csv").map(lambda line: line.split("::")).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))
business = business.select(business._1.alias('business_id'), business._2.alias('address'), business._3.alias('categories'))
result= review.join(business, 'business_id')
dbutils.fs.rm("/q3", True)
dbutils.fs.rm("/FileStore/tables/q3_output.txt", True)
result=result.filter(result.address.like('%Stanford, CA%')).distinct().select('user_id','stars')
result.write.save("FileStore/tables/answer.txt")
#.saveAsTextFile("/q3")
dbutils.fs.cp("./q1/part-00000","dbfs:/FileStore/tables/q3_output.txt", True)