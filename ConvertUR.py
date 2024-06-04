import os
import pandas as pd
import sys
import boto3
from datetime import datetime
import json
from botocore.exceptions import ClientError

'''
Write in console:
    python ConvertUR.py file_name.parquet
    
to read your CUR parquet file and output a .txt file with a CUR2 query for Data Exports
'''

client = boto3.client("sts")
account_id = client.get_caller_identity()["Account"]

#inFile = sys.argv[1]
#inFile="test.parquet"
Legacy_CUR_name = os.environ['LEGACY_CUR'] #export LEGACY_CUR=mybillingreport

def get_legacy_cur(Legacy_CUR_name):
    client = boto3.client('cur')
    current_month = datetime.now().month
    current_year = datetime.now().year
    response = client.describe_report_definitions()
    for report in response['ReportDefinitions']:
        if report['ReportName'] == Legacy_CUR_name:
            S3Bucket = report['S3Bucket']
            S3Prefix = report['S3Prefix']
            Name = report['ReportName']
            location = f'{S3Prefix}/{Name}/{Name}/year={current_year}/month={current_month}/{Name}-00001.snappy.parquet'

    return location,S3Bucket

def get_parquet_file(S3Bucket, location):
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(S3Bucket)
    my_bucket.download_file(location, 'parquet_file.snappy.parquet')

def extract_col_list(inFile):  
    if inFile.lower().endswith(".parquet"):
        df = pd.read_parquet(inFile)
        
    else:
        print("Incompatible file type.")
        exit()
   
    return df.columns.tolist()

def replace_legacy_columns(column_list):
    '''Takes a list of columns from CUR and outputs a list of columns matching the CUR 2.0 schema,
    including any aliasing (AS) needed to match the original CUR column names in the CUR 2.0 export output'''
    
    product_columns_keep=["product", "product_sku", "product_comment","product_fee_code","product_fee_description","product_from_location","product_from_location_type","product_from_region_code","product_instance_family","product_instance_type","product_instanceSKU","product_location","product_location_type","product_operation","product_pricing_unit","product_product_family","product_region_code","product_servicecode","product_to_location","product_to _location_type","product_to_region_code","product_usagetype"]
    #the product columns from CUR that are kept in CUR 2.0
    
    discount_columns_keep=["discount_total_discount", "discount_bundled_discount"]
    #the discount columns from CUR that are kept in CUR 2.0
    
    special_columns_keep = product_columns_keep + discount_columns_keep
    
    unchanged_prefix = ("line_item", "identity", "bill", "reservation", "savings_plan", "reservation", "pricing", "split_line_item")
    #column prefixes that don't change
    
    CUR2_list = []
    #initialize empty list
    
    for column in column_list:
        if column.startswith(unchanged_prefix):
            CUR2_list.append(column)
            
        elif column in special_columns_keep:
            CUR2_list.append(column)
            
        elif column.startswith("product"):
            new_col = column.replace("product_", "product.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("cost_category_"):
            new_col = column.replace("cost_category_", "cost_category.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("resource_tags_"):
            new_col = column.replace("resource_tags_", "resource_tags.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("discount_"):
            new_col = column.replace("discount_", "discount.", 1)
            CUR2_list.append(new_col + " AS " + column)
    
    return CUR2_list
        
    
def create_cur_sql_statement(column_list):
    ''' Takes a list of columns and outputs a SQL statement in string format'''

    sql_string = "SELECT "
    
    for i in range(len(column_list)):
        if i < len(column_list)-1:
            sql_string += (column_list[i] + ", ")
        else:
            sql_string += (column_list[i] + " ")
    
    sql_string += "FROM COST_AND_USAGE_REPORT"
    return sql_string

def update_bucket_policy(S3Bucket):

    # use client to get Bucket policy
    s3_client = boto3.client('s3')
    res = s3_client.get_bucket_policy(Bucket=S3Bucket)
    policy_json = res['Policy']
    policy = json.loads(policy_json)

    # adds new CUR 2.0 policy statement
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
                f"arn:aws:s3:::{S3Bucket}/*",
                f"arn:aws:s3:::{S3Bucket}"
            ],
            "Condition": {
                "StringLike": {
                    "aws:SourceAccount": account_id,
                    "aws:SourceArn": [
                        f"arn:aws:cur:us-east-1:{account_id}:definition/*",
                        f"arn:aws:bcm-data-exports:us-east-1:{account_id}:export/*"
                    ]
                }
            }
        }
    policy['Statement'].append(user_policy)
    new_policy = json.dumps(policy)
    s3_client.put_bucket_policy(Bucket=S3Bucket, Policy=new_policy)


def create_cur2_file(sql_string, S3Bucket):
    client = boto3.client('bcm-data-exports')
    region_name = client.meta.region_name

    response = client.create_export(
    Export={
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
        'Description': f'CUR 2.0 based on {Legacy_CUR_name}',
        'DestinationConfigurations': {
            'S3Destination': {
                'S3Bucket': S3Bucket,
                'S3OutputConfigurations': {
                    'Compression': 'PARQUET',
                    'Format': 'PARQUET',
                    'OutputType': 'CUSTOM',
                    'Overwrite': 'OVERWRITE_REPORT'
                },
                'S3Prefix': f"{Legacy_CUR_name}-2",
                'S3Region': region_name
                }
            },
        'Name': f'{Legacy_CUR_name}-2',
        'RefreshCadence': {
            'Frequency': 'SYNCHRONOUS'
                }
            }
        )
    print(response)


def lambda_handler(event, context):
    
    #get legacy CUR par file
    try:
        location,S3Bucket = get_legacy_cur(Legacy_CUR_name)
        print(location)
        print(S3Bucket)
        get_parquet_file(S3Bucket, location)

        #create SQL for new one 
        inFile = "parquet_file.snappy.parquet"
        col_list = extract_col_list(inFile)
        new_columns = replace_legacy_columns(col_list)
        sql_string = create_cur_sql_statement(new_columns)
        #add name cols to sql 
        sql_string = sql_string.replace("SELECT", "SELECT bill_payer_account_name,line_item_usage_account_name,")
        # with open('CUR2_query.txt', 'w') as f:
        #     f.write(sql_string)


        #create CUR 2.0
        update_bucket_policy(S3Bucket)

        create_cur2_file(sql_string, S3Bucket)
    except ClientError as e:
        print("Unexpected error: %s" % e)
        

lambda_handler(None, None)