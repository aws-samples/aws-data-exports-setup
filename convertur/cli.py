'''

This script uses bedrock to convert SQL queries from CUR1 to CUR2.
Run it with python and enter the SQL query in the interactive prompt.

'''
import os
import sys
import json

import boto3
import click
from prompt_toolkit import PromptSession

from convertur.cur1to2 import mapping as cur1to2_mapping
from convertur import utils

MAPPING = "\n".join([f"{key} = {val}" for key, val in cur1to2_mapping.items()])

PROMPT_TEMPLATE = """
Human:
Transform the following SQL query from Table1 format to Table2 format.

Tag fields are transformed like this:
    resource_tags_XXX -> resource_tags['XXX']
here the example is for (XXX) but it can be any other string.

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


def prompt(**kwargs):
    session = PromptSession(**kwargs)
    if os.environ.get('AWS_EXECUTION_ENV') == 'CloudShell':
       session.app.paste_mode = lambda: True # avoid auto-indent in CloudShell
    return session.prompt()


@click.command()
def main():
    """ get user input and run prompt
    """
    bedrock = boto3.client('bedrock-runtime')

    answer = None
    while True:
        last_answer = answer
        answer = prompt(
            message="Enter CUR1 SQL query or GitHub URL (r=retry, q=quit):",
            multiline=True,
        )

        if answer is None or answer.strip() == 'q' or answer.strip() == 'quit':
            break
        if answer.strip() == 'r' or answer.strip() == 'retry':
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
                                "text": PROMPT_TEMPLATE.format(mapping=MAPPING, query=query)
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
