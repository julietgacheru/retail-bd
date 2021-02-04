import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "retaildb", table_name = "department_summary", transformation_ctx = "deptdatasource"]
## @return: deptdatasource
## @inputs: []
deptdatasource = glueContext.create_dynamic_frame.from_catalog(database = "retaildb", table_name = "department_summary", transformation_ctx = "deptdatasource")
## @type: ApplyMapping
## @args: [mapping = [("dept_id", "int", "dept_id", "int"), ("dept_nm", "string", "dept_nm", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "deptapplymapping"]
## @return: deptapplymapping
## @inputs: [frame = deptdatasource]
deptapplymapping = ApplyMapping.apply(frame = deptdatasource, mappings = [("dept_id", "int", "dept_id", "int"), ("dept_nm", "string", "dept_nm", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "deptapplymapping")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "deptresolvechoice"]
## @return: deptresolvechoice
## @inputs: [frame = deptapplymapping]
deptresolvechoice = ResolveChoice.apply(frame = deptapplymapping, choice = "make_cols", transformation_ctx = "deptresolvechoice")
## @type: DropNullFields
## @args: [transformation_ctx = "deptdropnullfields"]
## @return: deptdropnullfields
## @inputs: [frame = deptresolvechoice]
deptdropnullfields = DropNullFields.apply(frame = deptresolvechoice, transformation_ctx = "deptdropnullfields")
## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"dbtable": "department_summary", "database": "department_summary"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: deptdatasink
## @inputs: [frame = deptdropnullfields]
deptdatasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = deptdropnullfields, catalog_connection = "redshift", connection_options = {"dbtable": "department_summary", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "deptdatasink")




## @type: DataSource
## @args: [database = "retaildb", table_name = "store_summary", transformation_ctx = "storedatasource"]
## @return: storedatasource
## @inputs: []
storedatasource = glueContext.create_dynamic_frame.from_catalog(database = "retaildb", table_name = "store_summary", transformation_ctx = "storedatasource")
## @type: ApplyMapping
## @args: [mapping = [("store_id", "int", "store_id", "int"), ("store_nm", "string", "store_nm", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "storeapplymapping"]
## @return: storeapplymapping
## @inputs: [frame = storedatasource]
storeapplymapping = ApplyMapping.apply(frame = storedatasource, mappings = [("store_id", "int", "store_id", "int"), ("store_nm", "string", "store_nm", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "storeapplymapping")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: storeresolvechoice
## @inputs: [frame = storeapplymapping]
storeresolvechoice = ResolveChoice.apply(frame = storeapplymapping, choice = "make_cols", transformation_ctx = "storeresolvechoice")
## @type: DropNullFields
## @args: [transformation_ctx = "storedropnullfields"]
## @return: storedropnullfields
## @inputs: [frame = storeresolvechoice]
storedropnullfields = DropNullFields.apply(frame = storeresolvechoice, transformation_ctx = "storedropnullfields")
## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"dbtable": "store_summary", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "storedatasink"]
## @return: storedatasink
## @inputs: [frame = dropnullfields4]
storedatasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = storedropnullfields, catalog_connection = "redshift", connection_options = {"dbtable": "store_summary", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "storedatasink")




## @type: DataSource
## @args: [database = "retaildb", table_name = "vendor_summary", transformation_ctx = "vendordatasource"]
## @return: vendordatasource
## @inputs: []
vendordatasource = glueContext.create_dynamic_frame.from_catalog(database = "retaildb", table_name = "vendor_summary", transformation_ctx = "vendordatasource")
## @type: ApplyMapping
## @args: [mapping = [("vendor_nm", "string", "vendor_nm", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "vendorapplymapping"]
## @return: vendorapplymapping
## @inputs: [frame = datasource0]
vendorapplymapping = ApplyMapping.apply(frame = vendordatasource, mappings = [("vendor_nm", "string", "vendor_nm", "string"), ("total_units", "long", "total_units", "long"), ("total_sales", "long", "total_sales", "long")], transformation_ctx = "vendorapplymapping")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "vendorresolvechoice"]
## @return: vendorresolvechoice
## @inputs: [frame = vendorapplymapping]
vendorresolvechoice = ResolveChoice.apply(frame = vendorapplymapping, choice = "make_cols", transformation_ctx = "vendorresolvechoice")
## @type: DropNullFields
## @args: [transformation_ctx = "vendordropnullfields"]
## @return: dropnullfields5
## @inputs: [frame = vendorresolvechoice]
vendordropnullfields = DropNullFields.apply(frame = vendorresolvechoice, transformation_ctx = "vendordropnullfields")
## @type: DataSink
## @args: [catalog_connection = "redshift", connection_options = {"dbtable": "vendor_summary", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "vendordatasink"]
## @return: vendordatasink
## @inputs: [frame = vendordropnullfields]
vendordatasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = vendordropnullfields, catalog_connection = "redshift", connection_options = {"dbtable": "vendor_summary", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "vendordatasink")




job.commit()