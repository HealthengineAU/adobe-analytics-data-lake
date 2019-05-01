# adobe-analytics-data-lake
CloudFormation script to set up an Adobe Analytics data lake in AWS.

## Create script S3 bucket and upload script

    aws s3 mb <bucket-name> 
    aws s3 cp hit-data.py <bucket-name>

## Validate Template

    aws cloudformation validate-template --template-body file://adobe-analytics-data-lake.yaml

## Run Template

    aws cloudfromation create-stack --template-body file://adobe-analytics-data-lake.yaml

