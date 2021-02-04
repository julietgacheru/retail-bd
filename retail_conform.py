# curated layer layer transformations: cleanse data and move data to conformed layer 
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import when,col,concat 

def conformed(spark):

	# create dataframe
	storeDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-curated/store/*.csv")
	salesDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-curated/sales/*.csv")
	productDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-curated/product/*.csv")

	# create new sale type column 

	salesDF = salesDF.withColumn("sales_type",when(col("sales_amt") < 0, "return").otherwise("sales"))


	## create a new column in product(dept_vendor). value will be concatenation of dept_id and vendor_id   
	productDF = productDF.withColumn("dept_vendor", concat(productDF.dept_id,productDF.vendor_id))

	# save data to conformed 

	salesDF.write.format("csv").mode("overwrite").option("header","True").save("s3://juliet-conformed/sales")
	storeDF.write.format("csv").mode("overwrite").option("header","True").save("s3://juliet-conformed/store")
	productDF.write.format("csv").mode("overwrite").option("header","True").save("s3://juliet-conformed/product")




def main():
	# create spark session 
	spark = SparkSession\
		.builder \
		.master("local")\
		.appName("conformed")\
		.getOrCreate() 

	sc = spark.sparkContext
	# set log level 
	sc.setLogLevel("ERROR")
	conformed(spark)

if __name__ == "__main__":main()



