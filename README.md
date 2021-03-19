# object-lambda-transform

An S3 Object Lambda example, converting CSV to Parquet on the fly.

See the full article covering this example [here](https://eoins.medium.com/using-s3-object-lambdas-to-generate-and-transform-on-the-fly-874b0f27fb84)!

## Deployment

The deployment consists of a Lambda Function, IAM Role, Log Group, S3 Bucket and Access Points.

Deployment uses [The Serverless Framework](https://serverless.com).

```
npx serverless deploy
```

The bucket name and Lambda access point names are generated based on your account ID and region.

```
export BUCKET_NAME=object-lambda-transform-dev-1234567890123-us-east-1
export LAP_NAME=object-lambda-transform-dev-lambda-ap
```

## Running

A sample CSV file is included. To test the function, first copy the csv to your bucket.

```
aws s3 cp data/weather.csv s3://${BUCKET_NAME}/
```

Then, use the AWS SDK (here, we use boto3) to test retrieval of Parquet and CSV data.
The following three invocations of `get_object` will all provide the same data but one request,
for `weather.parquet`, refers to a missing key that will be generated and return on the fly!

```
from io import BytesIO
import boto3
import pandas as pd

# Read CSV directly using the bucket
pd.read_csv(s3_client.get_object(
    Bucket=os.environ['BUCKET_NAME'],
    Key='weather.csv'
)['Body'])

# Read CSV using the Lambda Access Point
pd.read_csv(s3_client.get_object(
    Bucket=os.environ['LAP_ARN'],
    Key='weather.csv'
)['Body'])

# Read *Parquet* using the Lambda Access Point - dynamically generated!
pd.read_parquet(BytesIO(s3_client.get_object(
    Bucket=os.environ['LAP_ARN'],
    Key='weather.parquet'
)['Body'].read()))
```


