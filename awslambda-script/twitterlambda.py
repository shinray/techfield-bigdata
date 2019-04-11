import boto3  # aws client
import os
import errno
import uuid
import json
from json import JSONDecoder, JSONDecodeError
import re

s3_client = boto3.client('s3')

# https://stackoverflow.com/a/50384432
NOT_WHITESPACE = re.compile(r'[^\s]')
def decode_stacked(document, pos=0, decoder=JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return
        pos = match.start()

        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError:
            # do something sensible if there's some error
            raise
        yield obj


def clean_json(inpath, outpath):
    with open(inpath) as f:
        infile = f.read()
        for i in decode_stacked(infile):
            data = {}
            data['created_at'] = i['created_at']
            data['text'] = i['text']
            data['screen_name'] = i['user']['screen_name']
            data['followers_count'] = i['user']['followers_count']
            data['friends_count'] = i['user']['friends_count']
            data['location'] = i['user']['location']

            # my key seems to have directories in it...
            if not os.path.exists(os.path.dirname(outpath)):
                try:
                    os.makedirs(os.path.dirname(outpath))
                except OSError as exc:  # avoid race conditions
                    if exc.errno != errno.EEXIST:
                        raise

            with open(outpath, 'a+') as of:
                json.dump(data, of)
                of.write('\n')


# AWS looks for this function to run on trigger.
def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        download_path = '/tmp/{}'.format(uuid.uuid4())
        upload_path = '/tmp/clean-{}'.format(key)
        
        s3_client.download_file(bucket, key, download_path)
        clean_json(download_path, upload_path)
        # Upload to a bucket of the same name with '-out' appended
        s3_client.upload_file(upload_path, '{}-out'.format(bucket), key)
