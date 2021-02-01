# raw layer layer transformations : cleanse data and move data to curated layer
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import trim


def cleanse(spark):
    # create dataframe from 3 files
    storeDF = spark.read.format("csv").options(header="True", inferSchema="True").load("s3://juliet-raw/store.dat")
    salesDF = spark.read.format("csv").options(header="True", inferSchema="True").load("s3://juliet-raw/sales.dat")
    productDF = spark.read.format("csv").options(header="True", inferSchema="True").load("s3://juliet-raw/product.dat")

    # deduplicate
    storeDF = storeDF.distinct()
    salesDF = salesDF.distinct()
    productDF = productDF.distinct()

    # Null values and empty spaces

    storeDF.createOrReplaceTempView("store")
    salesDF.createOrReplaceTempView("sales")
    productDF.createOrReplaceTempView("product")

    # check NULL values & referential key check
    good_storeDF = spark.sql( \
        "SELECT * FROM store \
        WHERE store_id IS NOT NULL \
        AND store_nm IS NOT NULL AND \
        trim(store_nm) != '' \
        AND trim(state) != ''")

    bad_storeDF = spark.sql( \
        "SELECT * FROM store \
        WHERE store_id IS NULL \
        OR store_nm IS NULL \
        OR trim(store_nm) == '' \
        OR trim(state) == ''")

    good_productDF = spark.sql( \
        "SELECT * FROM product \
        WHERE product_id IS NOT NULL \
        AND dept_id IS NOT NULL \
        AND vendor_id IS NOT NULL")

    bad_productDF = spark.sql( \
        "SELECT * FROM product \
        WHERE product_id IS  NULL \
        OR dept_id IS  NULL \
        OR vendor_id IS  NULL")

    good_salesDF = spark.sql( \
        "SELECT * \
        FROM sales \
        WHERE (product_id IS NOT NULL \
        AND store_id IS NOT NULL \
        AND product_id IN (SELECT product_id FROM product)) \
        AND (store_id IN (SELECT store_id FROM store))")

    bad_sales_productDF = salesDF.join(good_productDF, salesDF.product_id == good_productDF.product_id, "leftanti")
    bad_sales_storeDF = salesDF.join(good_storeDF, salesDF.store_id == good_storeDF.store_id, "leftanti")

    bad_salesDF = bad_sales_productDF.union(bad_sales_storeDF).distinct()

    # save output into curated
    good_storeDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-curated/store")
    good_productDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-curated/product")
    good_salesDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-curated/sales")

    bad_storeDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-rejected/store")
    bad_productDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-rejected/product")
    bad_salesDF.write.format("csv").mode("overwrite").option("header", "True").save("s3://juliet-rejected/sales")


def main():
    # create spark session
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("dataCleanse") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    cleanse(spark)


if __name__ == "__main__": main()