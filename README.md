## Retail Store ETL solution

### Description

Sample automated data pipeline on AWS that triggers a step function from lambda after uploading files to S3. The Step function automates AWS Glue jobs and uses AWS CloudWatch for monitoring and triggering AWS Glue Crawler to allow querying with Athena and Redshift. 


##### Creating S3 Buckets 


```bash
aws s3api create-bucket --bucket <bucket_name> --region us-east-1


```


### License
[MIT](https://choosealicense.com/licenses/mit/)