# Adobe Analytics Data Lake for AWS

CloudFormation script to set up an Adobe Analytics data lake in AWS.

## Overview

In Adobe Analytics, a Data Feed is set up (using the All Columns Standard 2016)
specification.  It is configured to write hourly to an incoming S3 bucket.
Hourly writes avoid huge files that can cause out of memory issues during
processing.

Once a day, an AWS Glue Job runs that takes the compressed tab separated hit
data files in the incoming bucket and writes them to the target bucket in
Parquet format, partitioning using a calculated date column that is added to the
data set.  The reason for this translation is the reduction in the amount of
data that needs to be scanned to answer a query by a factor of 1,000.  This also
reduces AWS costs for querying by a factor of 1,000.

Once a day, after the Glue Job has run, a Glue Crawler runs on the target bucket
to update the schema definition of the data stored there.  The column
information won't usually change, but new partitions need to be recorded,
otherwise they will not appear in queries

Users can use AWS Athena to run queries directly over the S3 Parquet data, using
ANSI SQL.

To support joining the Adobe Analytics data with other data sets, Redshift
Spectrum is set up.  This creates a virtual schema and table in Redshift, that
acts just like a regular database table but in fact uses Athena and S3 under the
hood to return query results.  Having the data available in Redshift lowers the
barrier to entry to use the data and allows it to be enriched with business data
from other systems.

## Setup

Make sure that you set your CLI's default region (or feed's CloudFormation region in multi-region deployment) to one of the supported ones listed [here](https://marketing.adobe.com/resources/help/en_US/reference/r_feed-destination.html). Alternatively you can use the [--region option](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-options.html) as shown in the code below.

### Run Single-Region Template

    export PREFIX=mycompany
    export REGION=myregion
    aws cloudformation create-stack \
       --stack-name adobe-analytics-data-lake \
       --template-body file://adobe-analytics-data-lake.yaml \
       --capabilities CAPABILITY_NAMED_IAM \
       --parameters \
           ParameterKey=AdobeAnalyticsDataFeedS3BucketName,ParameterValue=$PREFIX-adobe-analytics-data-feed \
           ParameterKey=AdobeAnalyticsDataLakeS3BucketName,ParameterValue=$PREFIX-adobe-analytics-data-lake \
           ParameterKey=AdobeAnalyticsGlueJobScriptsS3BucketName,ParameterValue=$PREFIX-glue-scripts \
           ParameterKey=GlueDataLakeDatabaseName,ParameterValue=adobe-analytics-data-lake \
           ParameterKey=AdobeAnalyticsGlueJobName,ParameterValue=adobe-analytics-data-feed-to-lake \
           ParameterKey=RedshiftSpectrumRoleName,ParameterValue=redshift-spectrum-adobe-analytics\
       --region $REGION

### Run Multi-Region Template

```bash
    export PREFIX=mycompany
    export REGION_FEED=us-east-1 # Pick an Adobe supported region for the feed
    export REGION_BASE=us-east-2 # Any AWS region that supports the core services (Glue, RedShift Spectrum, etc.)
    aws cloudformation create-stack \
       --stack-name adobe-analytics-data-lake-base \
       --template-body file://adobe-analytics-data-lake-multi-base.yaml \
       --capabilities CAPABILITY_NAMED_IAM \
       --parameters \
           ParameterKey=AdobeAnalyticsDataFeedS3BucketName,ParameterValue=$PREFIX-adobe-analytics-data-feed \
           ParameterKey=AdobeAnalyticsDataLakeS3BucketName,ParameterValue=$PREFIX-adobe-analytics-data-lake \
           ParameterKey=AdobeAnalyticsGlueJobScriptsS3BucketName,ParameterValue=$PREFIX-glue-scripts \
           ParameterKey=GlueDataLakeDatabaseName,ParameterValue=adobe-analytics-data-lake \
           ParameterKey=AdobeAnalyticsGlueJobName,ParameterValue=adobe-analytics-data-feed-to-lake \
           ParameterKey=RedshiftSpectrumRoleName,ParameterValue=redshift-spectrum-adobe-analytics\
       --region $REGION_BASE

    aws cloudformation create-stack \
       --stack-name adobe-analytics-data-lake-base \
       --template-body file://adobe-analytics-data-lake-multi-feed.yaml \
       --capabilities CAPABILITY_NAMED_IAM \
       --parameters \
           ParameterKey=AdobeAnalyticsDataFeedS3BucketName,ParameterValue=$PREFIX-adobe-analytics-data-feed \
       --region $REGION_FEED
```

### Upload the Glue Job Python script

    aws s3 cp hit-data.py s3://$PREFIX-glue-scripts/

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

### Enable Glue Job Trigger

* Go to the AWS Console
* Go to AWS Glue
* Go to ETL -> Triggers
* Select (tick) the job trigger 'adobe-analytics-feed-to-lake-hit-data-daily'
* Choose Action -> Enable trigger

## Using Athena

### Basic Query

```sql
SELECT * FROM "adobe-analytics-data-lake"."hit_data" LIMIT 10
```

### Partition Query

Note: data in newly created partitions will not be visible in Athena until the Crawler 
is run, which will update the list of partitions.

```sql
SELECT * FROM "adobe-analytics-data-lake"."hit_data" WHERE date = '2019-05-01'
```

## Using Redshift Spectrum

Access the Redshift console in AWS and add the defined Redshift IAM Role created by
the CloudFormation script.  This grants Redshift the ability to read from the data
lake S3 bucket.

Create the virtual schema in Redshift that points to the Glue database (Glue tables will
automatically appear as Redshift tables).  Use the ARN assigned to the Redshift Spectrum
role returned by the CloudFormation script.

```sql
CREATE EXTERNAL SCHEMA adobe_analytics_data_lake
    FROM DATA CATALOG DATABASE 'adobe-analytics-data-lake' 
    IAM_ROLE 'arn:aws:iam::<aws-account-id>:role/redshift-spectrum';
```

Grant access to database users to query the data lake:

```sql
GRANT USAGE ON SCHEMA adobe_analytics_data_lake TO <user>;
```

Then query the data:

```sql
SELECT * FROM adobe_analytics_data_lake.hit_data WHERE date = '2019-05-01'
```
