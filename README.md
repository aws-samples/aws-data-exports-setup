# Move to CUR 2.0

Write in console:
  ``` export LEGACY_CUR=<cur name from billing console>```
    ```python ConvertUR.py ``

pip install -r requirements.txt

### CloudShell
must be run in us-east-1

nano cur.py
command +v (paste)
Control+x
y
ENTER


## Process
1. Read Parquet File from legacy CUR bucket
2. Looks at schema of this
3. Write CUR 2.0 Query which will have the same schema as legacy. 
4. Updates Current CURS bucket permissions
5. Creates new CUR


# Legacy Query Converter
Will not work with nested queries 
save query in test.text           
```python query_converter.py     ```


## Why
- Some cols got collapsed and so they need to be expanded and the script will do it
- and renames 
-  there are a lot of cols so doing it by hand would be long


## Future
* Pull from S3 location listed in Console
* how to run in cloudshell?
* create github repo
* Also work with CSV


## Links
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bcm-data-exports.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cur.html
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

