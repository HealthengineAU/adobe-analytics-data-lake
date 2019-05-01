# adobe-analytics-data-lake
CloudFormation script to set up an Adobe Analytics data lake in AWS.

## Setup

### Create script S3 bucket and upload script

    aws s3 mb <bucket-name> 
    aws s3 cp hit-data.py <bucket-name>

### Validate Template

    aws cloudformation validate-template --template-body file://adobe-analytics-data-lake.yaml

### Run Template

    aws cloudfromation create-stack --template-body file://adobe-analytics-data-lake.yaml

### Set up Adobe Analytics Data Feed

* Admin -> Data Feeds -> Add
* Set up as you wish, but you must use this particular configs:
    * Feed Information
        * Feed Interval: Hourly
        * Start & End Dates: Continuous Feed
    * Destination
        * Type: S3
        * Bucket: <bucket-name>
        * Path: incoming
        * Access Key: Get from AWS IAM
       * Secret Key: Get from AWS IAM
    * Data Column Definitions
        * Remove Escaped Characters: Yes
        * Compression Format: Gzip
        * Packaging Type: Multiple Files
        * Column Template: All Columns Standard (Oct 2016)

## Using Athena

### Basic Query

    SELECT * FROM "adobe-analytics-data-lake"."hit_data" LIMIT 10

### Partition Query

Note: data in newly created partitions will not be visible in Athena until the Crawler 
is run, which will update the list of partitions.

    SELECT * FROM "adobe-analytics-data-lake"."hit_data" WHERE date = '2019-05-01'

