'''

This script uses bedrock to convert SQL queries from CUR1 to CUR2.
Run it with python and enter the SQL query in the interactive prompt.

'''
import sys
import json

import boto3
import click
import questionary

from convertur.cur1to2 import mapping as cur1to2_mapping
import convertur.utils as utils

mapping = "\n".join([f"{key} = {val}" for key, val in cur1to2_mapping.items()])

prompt_template = """
Human:
Transform the following SQL query from Table1 format to Table2 format.

Tag fields are transformed like this:
    resource_tags_user_application -> resource_tags['user_application']

For other fields use this mapping of Table1 to Table2 fields:
{mapping}

If the field is not in the list, stop and explain.

Original query:
{query}

Make sure to replace year and month as per mapping.

Keep original formatting when possible.

Response should contain only resulting Query for Table2. Explain the difference between the result and original query
Assistant:
"""


@click.command()
def main():
    bedrock = boto3.client('bedrock-runtime')

    answer = None
    while True:
        last_answer = answer
        answer = questionary.text(multiline=True, message='Enter CUR1 SQL query or GitHub URL (r=retry, q=quit').ask()

        if answer is None or answer.strip() == 'q' or answer.strip() == 'quit':
            break
        elif answer.strip() == 'r' or answer.strip() == 'retry':
            answer = last_answer

        if answer.strip().startswith('https://'):
            query = utils.get_code(answer.strip())
        else:
            query = answer

        print('Processing')
        response = bedrock.invoke_model_with_response_stream(
            modelId='anthropic.claude-3-sonnet-20240229-v1:0',
            accept='application/json',
            contentType='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 10000,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt_template.format(mapping=mapping, query=query)
                            }
                        ]
                    }
                ]
            }),
        )

        for event in response.get('body'):
            chunk = json.loads(event['chunk']['bytes'].decode())
            if chunk['type'] == 'content_block_delta':
                text = chunk['delta']['text']
                sys.stdout.write(text)
                sys.stdout.flush()
        print()


if __name__ == '__main__':
    main()