'''

This script uses bedrock to convert SQL queries from CUR1 to CUR2.
Run it with python and enter the SQL query in the interactive prompt.

'''
import os
import sys
import json

import boto3
import click
import pygments
from prompt_toolkit import PromptSession
from pygments.lexers.sql import SqlLexer
from pygments.token import Token
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.formatted_text import PygmentsTokens, HTML
from prompt_toolkit import print_formatted_text

from convertur.cur1to2 import mapping as cur1to2_mapping
from convertur import utils

MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

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

do not replace line_item_usage_start_date and line_item_usage_end_date field 

Keep ${{table_name}} and ${{date_filter}} as is unless said otherwise.

Original query:
{query}

Make sure to replace year and month as per mapping.

Keep original formatting when possible.

Response should contain only resulting Query for Table2. Explain the difference between the result and original query. Before explanation insert word: 'Explanation:'
Assistant:
"""


def prompt(**kwargs):
    session = PromptSession(**kwargs)
    if os.environ.get('AWS_EXECUTION_ENV') == 'CloudShell':
       session.app.paste_mode = lambda: True # avoid auto-indent in CloudShell
    return session.prompt()

def get_terminal_width():
    try:
        columns, _ = os.get_terminal_size()
        return columns
    except AttributeError:
        # os.get_terminal_size() not available (probably running in IDE)
        return 80  # default to 80 columns

def _print(text):
    try:
        sql = text.split('Explanation:')[0]
        other_text = text[len(sql):]
        tokens = list(pygments.lex(sql, lexer=SqlLexer()))
        print_formatted_text(PygmentsTokens(tokens))
        print(other_text)
    except:
        print(text)


@click.command()
@click.option('--syntax/--no-syntax', help='Activate syntax highlighting', default=True)
def main(syntax):
    """ get user input and run prompt
    """
    bedrock = boto3.client('bedrock-runtime')

    answer = None
    while True:
        last_answer = answer
        print()
        print_formatted_text(HTML('<p><b>Enter CUR1 SQL query or GitHub URL then Press Escape + Enter</b> <br/>(r=retry, q=quit): </p>  <br/>'))
        answer = prompt(
            message=' >',
            multiline=True,
            lexer=PygmentsLexer(SqlLexer) if syntax else None,
            rprompt=lambda: 'Press Escape + Enter',
        )

        if answer is None or answer.strip() == 'q' or answer.strip() == 'quit':
            break
        if answer.strip() == 'r' or answer.strip() == 'retry':
            answer = last_answer

        if answer.strip().startswith('https://'):
            query = utils.get_code(answer.strip())
        else:
            query = answer

        # fix some wired queries
        query = query.replace('${table_name}.','')

        print('\n\n\n')
        print('Processing..')
        print('\n\n')
        print_formatted_text(HTML('<b>Bedrock assistant</b>:'))
        print('\n\n')
        try:
            response = bedrock.invoke_model_with_response_stream(
                modelId=MODEL_ID,
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
        except Exception as exc:
            if "You don't have access to the model with the specified model ID." in str(exc):
                print(f"You don't have access to the model. Please open https://console.aws.amazon.com/bedrock and Request activate access to model {MODEL_ID}. This may take 5mins to be ready to use")
                exit(1)
            raise

        full_text = ''
        for event in response.get('body'):
            chunk = json.loads(event['chunk']['bytes'].decode())
            if chunk['type'] == 'content_block_delta':
                text = chunk['delta']['text']
                full_text += text
                sys.stdout.write(text)
                sys.stdout.flush()

        if syntax:
            # replace simple printed lines with syntax syntax
            lines_up = len(full_text.splitlines()) + 2
            sys.stdout.write('\033[F' * lines_up)
            sys.stdout.flush()
            width = get_terminal_width()
            # Clear the lines
            for _ in range(lines_up + 1):
                sys.stdout.write('\r' + ' ' * width + '\n')
            sys.stdout.write('\033[F' * lines_up)  # Move cursor up N lines
            sys.stdout.flush()
            _print(full_text)

if __name__ == '__main__':
    main()
