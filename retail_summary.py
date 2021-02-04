# conformed layer transformations : save data in analytics lalyer 

from pyspark import SparkContext 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,sum,avg,max


def summary(spark):
	# create dataframe 
	storeDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-conformed/store/*.csv")
	salesDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-conformed/sales/*.csv")
	productDF = spark.read.format("csv").options(header="True",inferSchema="True").load("s3://juliet-conformed/product/*.csv")


	##### perfom analytics 

	# 1. Total sales by dept 
	sales_prod_storeDF = salesDF.join(productDF, salesDF.product_id == productDF.product_id, "inner").join(storeDF,salesDF.store_id == storeDF.store_id,"inner")

	deptSummDF = sales_prod_storeDF.groupby("dept_id","dept_nm").agg(sum("sales_unit").alias("total_units"),sum("sales_amt").alias("total_sales"))

	# 2. total sales by store
	storeSummDF  = sales_prod_storeDF.groupby(storeDF.store_id,storeDF.store_nm,storeDF.city,storeDF.state).agg(sum("sales_unit").alias("total_units"),sum("sales_amt").alias("total_sales"))

	# 3. total sales by vendor
	vendSummDF  = sales_prod_storeDF.groupby(productDF.vendor_nm).agg(sum("sales_unit").alias("total_units"),sum("sales_amt").alias("total_sales"))

	# save data as parquet 
	deptSummDF.write.format("parquet").mode("append").option("header","True").save("s3://juliet-analytics/department_summary")
	storeSummDF.write.format("parquet").mode("append").option("header","True").save("s3://juliet-analytics/store_summary")
	vendSummDF.write.format("parquet").mode("append").option("header","True").save("s3://juliet-analytics/vendor_summary")
	

def main():
	spark = SparkSession\
		.builder\
		.master("local")\
		.appName("summary")\
		.getOrCreate()

	sc = spark.sparkContext 

	sc.setLogLevel("ERROR")
	
	summary(spark)



if __name__ == "__main__":main()
