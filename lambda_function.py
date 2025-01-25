import os
import boto3
import urllib
from io import StringIO
import pandas as pd
import urllib
import math
import json
from botocore.exceptions import ClientError


s3 = boto3.client('s3')
sqs = boto3.client('sqs')
#queue_url = 'https://sqs.us-east-1.amazonaws.com/635413725004/lambda-split-sqs'
queue_url = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    """
    Split a csv into several files.
    :param file: path to the original csv.
    :param output_path: path in which to output the resulting parts of the splitting.
    :param nrows: Number of rows to split the original csv by, also view pandas.read_csv doc.
    :param chunksize: View pandas.read_csv doc.
    :param low_memory: View pandas.read_csv doc.
    :param usecols: View pandas.read_csv doc.
    """
    # chunksize=1000
    #1 - Get the bucket name
    bucket = event['Records'][0]['s3']['bucket']['name']
    

    #2 - Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    

    #3 - Fetch the file from S3
    response = s3.get_object(Bucket=bucket, Key=key)

    #4 - Deserialize the file's content
    file_content = response["Body"].read().decode('utf-8')

    # Figure out the number of files this function will generate.
    nrows=1000
    df = pd.read_csv(StringIO(file_content))
    print(df.shape)
    nb_of_rows = len(df.index)
    full_path=key
    trim_path=os.path.relpath(full_path, 'input')

    # file_ext = trim_path.split(".")[-1]
    file_name_trunk = trim_path.split(".")[0]
    split_files_name_trunk = file_name_trunk + "_part_"

    # Number of chunks to partition the original file into
    nb_of_chunks = math.ceil(nb_of_rows / nrows)
    if nrows:
        print(f"The file '{trim_path}' contains {nb_of_rows} ROWS. " \
            f"\nIt will be split into {nb_of_chunks} chunks of a max number of rows : {nrows}.")
        # logging.debug(log_debug_process_start)

    for i in range(nb_of_chunks):
        # Number of rows to skip is determined by (the number of the chunk being processed) multiplied by (the nrows parameter).
        rows_to_skip = range(1, i * nrows) if i else None
        # print(rows_to_skip)
        output_file = f"{split_files_name_trunk}{i}.csv"

        # log_debug_chunk_processing = f"Processing chunk {i} of the file '{trim_path}'"
        # logging.debug(log_debug_chunk_processing)

        # Fetching the original csv file and handling it with skiprows and nrows to process its data
        if i==0:
            df_chunk = pd.read_csv(StringIO(file_content), nrows=nrows-1, skiprows=rows_to_skip)
        else:
            df_chunk = pd.read_csv(StringIO(file_content), nrows=nrows, skiprows=rows_to_skip)
        csv_buffer = StringIO()
        df_chunk.to_csv(csv_buffer, index=False)
        content = csv_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key='raw/'+output_file, Body=content)
        

        
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=30,
            MessageAttributes={
                'key': {
                    'DataType': 'String',
                    'StringValue': 'raw/'+output_file
                }
            },
            MessageBody=(bucket)
        )
        
        # print(response)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }