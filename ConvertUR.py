'''
Write in console:

    python ConvertUR.py file_name.parquet

to read your CUR parquet file and output a .txt file with a CUR2 query for Data Exports
'''

import re
import json
from datetime import datetime

import boto3
import questionary
import pandas as pd


account_id = boto3.client("sts").get_caller_identity()["Account"]


def choose_report():
    cur = boto3.client("cur", region_name='us-east-1')
    reports = cur.describe_report_definitions()['ReportDefinitions']
    choices = [r['ReportName'] for r in reports]
    while True:
        if not choices:
            print('No Legacy CUR found')
            exit(-1)
        report_name = questionary.select(message='select', choices=choices).ask()
        report = next((r for r in reports if r['ReportName'] == report_name))
        choices.remove(report['ReportName'])

        if report['Format'] != 'Parquet':
            print (f"The report {report['ReportName']} is not Parquet. Cannot proceed. Please chose another one.")
            continue
        if 'ATHENA' not in report['AdditionalArtifacts']:
            print (f"The report {report['ReportName']} is has no Athena Integration. Cannot proceed. Please chose another one.")
            continue
        if report['ReportVersioning'] != 'OVERWRITE_REPORT':
            print (f"The report {report['ReportName']} has no option 'OVERWRITE_REPORT'. Cannot proceed. Please chose another one.")
            continue

        return report


def get_legacy_cur_file_location(report):
    current_month = datetime.now().month
    current_year = datetime.now().year
    bucket = report['S3Bucket']
    prefix = report['S3Prefix']
    name = report['ReportName']
    return bucket, f'{prefix}/{name}/{name}/year={current_year}/month={current_month}/{name}-00001.snappy.parquet'


def get_parquet_file(bucket, location, local_file):
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket)
    bucket.download_file(location, local_file)

def extract_col_list(parquet_file):
    df = pd.read_parquet(parquet_file)
    return df.columns.tolist()

def replace_legacy_columns(columns):
    '''Takes a list of columns from CUR and outputs a list of columns matching the CUR 2.0 schema,
    including any aliasing (AS) needed to match the original CUR column names in the CUR 2.0 export output'''

    #the product columns from CUR that are kept in CUR 2.0
    product_columns_keep=[
        "product",
        "product_sku",
        "product_comment",
        "product_fee_code",
        "product_fee_description",
        "product_from_location",
        "product_from_location_type",
        "product_from_region_code",
        "product_instance_family",
        "product_instance_type",
        "product_instanceSKU",
        "product_location",
        "product_location_type",
        "product_operation",
        "product_pricing_unit",
        "product_product_family",
        "product_region_code",
        "product_servicecode",
        "product_to_location",
        "product_to _location_type",
        "product_to_region_code",
        "product_usagetype"
    ]

    #the discount columns from CUR that are kept in CUR 2.0
    discount_columns_keep=[
        "discount_total_discount",
        "discount_bundled_discount",
    ]
    special_columns_keep = product_columns_keep + discount_columns_keep

    #column prefixes that don't change
    unchanged_prefixes = [
        "line_item",
        "identity",
        "bill",
        "reservation",
        "savings_plan",
        "reservation",
        "pricing",
        "split_line_item"
    ]

    cur2_columns = []

    for column in columns:
        if any([column.startswith(unchanged_prefix) for unchanged_prefix in unchanged_prefixes]) :
            cur2_columns.append(column)
        elif column in special_columns_keep:
            cur2_columns.append(column)
        elif column.startswith("product"):
            new_col = re.sub('product_(.+)', r"product.\1", column)
            cur2_columns.append(new_col + " AS " + column)
        elif column.startswith("cost_category_"):
            new_col = re.sub('cost_category_(.+)', r"cost_category.\1", column)
            cur2_columns.append(new_col + " AS " + column)
        elif column.startswith("resource_tags_"):
            new_col = re.sub('resource_tags_(.+)', r"resource_tags.\1", column)
            cur2_columns.append(new_col + " AS " + column)
        elif column.startswith("discount_"):
            new_col = re.sub('discount_(.+)', r"discount.\1", column)
            cur2_columns.append(new_col + " AS " + column)
        else:
            print(f'WARNING: unknown column {column}. Will skip that.')
    return cur2_columns

def create_cur_sql_statement(columns):
    ''' Takes a list of columns and outputs a SQL statement in string format
    '''
    return (
        "SELECT \n " +
        '\n, '.join(columns) +
        "\nFROM COST_AND_USAGE_REPORT"
    )

def update_bucket_policy(bucket):
    # get Bucket policy
    s3_client = boto3.client('s3')
    res = s3_client.get_bucket_policy(Bucket=bucket)
    policy_json = res['Policy']
    policy = json.loads(policy_json)

    # adds new CUR 2.0 policy statement
    for statement in policy['Statement']:
        if statement.get('Sid') == "EnableAWSDataExportsToWriteToS3AndCheckPolicy":
            print('Bucket Policy is already set')
            return

    user_policy = {
            "Sid": "EnableAWSDataExportsToWriteToS3AndCheckPolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "billingreports.amazonaws.com",
                    "bcm-data-exports.amazonaws.com"
                ]
            },
            "Action": [
                "s3:PutObject",
                "s3:GetBucketPolicy"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket}/*",
                f"arn:aws:s3:::{bucket}"
            ],
            "Condition": {
                "StringLike": {
                    "aws:SourceAccount": account_id,
                    "aws:SourceArn": [
                        f"arn:aws:cur:us-east-1:{account_id}:definition/*", # region and partition hardcoded
                        f"arn:aws:bcm-data-exports:us-east-1:{account_id}:export/*"
                    ]
                }
            }
        }
    policy['Statement'].append(user_policy)
    new_policy = json.dumps(policy)
    s3_client.put_bucket_policy(Bucket=bucket, Policy=new_policy)
    print('Bucket Policy Updated')


def create_cur2(sql_string, bucket, report_name):
    client = boto3.client('bcm-data-exports')
    region = get_bucket_region(bucket)

    export = {
        'DataQuery': {
            'QueryStatement': sql_string,
            'TableConfigurations': {
                "COST_AND_USAGE_REPORT": {
                    "TIME_GRANULARITY": "HOURLY",
                    "INCLUDE_RESOURCES": "TRUE" ,
                    "INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY": "TRUE",
                    "INCLUDE_SPLIT_COST_ALLOCATION_DATA": "TRUE"
                }
            }
        },
        'Description': f'CUR 2.0 based on {report_name}',
        'DestinationConfigurations': {
            'S3Destination': {
                'S3Bucket': bucket,
                'S3OutputConfigurations': {
                    'Compression': 'PARQUET',
                    'Format': 'PARQUET',
                    'OutputType': 'CUSTOM',
                    'Overwrite': 'OVERWRITE_REPORT'
                },
                'S3Prefix': f"{report_name}-2",
                'S3Region': region,
            }
        },
        'Name': f'{report_name}-2',
        'RefreshCadence': {
            'Frequency': 'SYNCHRONOUS'
        }
    }

    existing_report = next(client.get_paginator('list_exports').paginate().search("Exports[? ExportName == 'cur1-2']"), None)
    if existing_report:
        print('report exists')
        if not questionary.confirm('Report exists. Update?').ask():
            exit(0)
        response = client.update_export(Export=export, ExportArn=existing_report['ExportArn'])
        if response['ResponseMetadata']['HTTPStatusCode'] not in [200, 201]:
            print('Something went wrong')
            print(response)
        else:
            print('CUR 2.0 updated')
    else:

        response = client.create_export(Export=export)
        if response['HTTPStatusCode'] not in [200, 201]:
            print('Something went wrong')
            print(response)
        else:
            print('CUR 2.0 created')


def get_bucket_region(bucket):
    return boto3.client('s3').get_bucket_location(Bucket=bucket).get('LocationConstraint') or 'us-east-1'

def main():

    print('\nStep 1/5: choosing legacy CUR report')
    report = choose_report()

    print('\nStep 2/5: Pulling the latest parquet file')
    bucket, location, = get_legacy_cur_file_location(report)
    local_file_name = "parquet_file.snappy.parquet"
    get_parquet_file(bucket, location, local_file_name)
    cur1_columns = extract_col_list(local_file_name)
    print(cur1_columns)

    print('\nStep 3/5: Generating SQL')
    cur2_columns = replace_legacy_columns(cur1_columns)
    cur2_columns += ['bill_payer_account_name', 'line_item_usage_account_name']
    sql_string = create_cur_sql_statement(cur2_columns)
    print(sql_string)

    print('\nStep 4/5: Updating bucket policy')
    update_bucket_policy(bucket)

    print('\nStep 5/5: Creating CUR 2.0')
    create_cur2(sql_string, bucket, report_name=report['ReportName'])

    print('Done. CUR will be populated in 24 hours.')


if __name__ == '__main__':
    main()