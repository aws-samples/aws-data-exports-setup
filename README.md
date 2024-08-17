# Tools for migration to CUR 2.0

We recommend you migrating to CUR 2.O as it is more performant and future proof way of getting the most detailed information about your AWS cost and usage.

For creation of CUR 2.0 export from scratch please use a [CloudFormation Stack from published on Cloud Intelligence Dashboard](https://catalog.workshops.aws/awscid/en-US/data-exports) workshop. This solution can create the CUR2.0, transfer of data to a dedicated FinOps account and create needed Athena table for your analysis. 

This repository provides a set of tools that helps you simplify migration to CUR 2.0 if you have existing integration with 3rd party tools or if you have your set of queries that you want to use with CUR 2.0 .



## Convert CUR1 query to CUR2.0 with Amazon Bedrock

This repository provides ready to use command line tool that takes as interactive export your CUR queries and transforms them to CUR2.0 format using Amazon Bedrock.

### Installation

1. Open CloudShell in `us-east-1` region.
2. Run following command:
```
  pip3 install -U git+https://github.com/aws-samples/aws-data-exports-setup
```
### Run
Run the interactive prompt, and insert your CUR1 queries
```
query-convertor
```

The tool also can read GitHub public urls.


## Create CUR 2.0 Data Export that is equivalent to your Legacy CUR

This tool detects currently deployed [legacy CUR](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cur.html), reads the file to define a format, produces a query that is equivalent to your legacy CUR data export and creates the [new CUR 2.0](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bcm-data-exports.html) to the same bucket.

1. Read Parquet File from legacy CUR bucket
2. Analyze the schema of parquet file
3. Create CUR 2.0 Query which will have the same schema as legacy.
4. Updates Current CUR bucket permissions
5. Creates new CUR

### Installation
1. Open CloudShell in `us-east-1` region.
2. Run following command:
```
  pip3 install git+https://github.com/aws-samples/aws-data-exports-setup
```

### Usage
Run migration script
```
migrate-cur1
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

