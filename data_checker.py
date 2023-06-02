import boto3
import gzip
import json
import os
from math import ceil
from pymongo import MongoClient


def load_file(filepath):
    documents = {}
    lines = open(filepath, 'r').read().splitlines()
    for line in lines:
        columns = line.split('\t')
        documents[columns[0]] = columns[1]
    return documents



def check_file(remote_bucket, remote_filename):
    with open('/tmp/source.gz', 'wb') as dest:
        gcp_client.download_fileobj(remote_bucket, remote_filename, dest)
    with gzip.open('/tmp/source.gz', 'rb') as gzfile:
        byte_contents = gzfile.read()
        with open('/tmp/source.tsv', 'wb') as tsvfile:
            count = tsvfile.write(byte_contents)
    document_dict = load_file('/tmp/source.tsv')
    id_list = ['PMID:' + document_id for document_id in document_dict.keys()]
    print(id_list[:10])
    print(len(id_list))
    found_ids = []
    subs = ceil(len(id_list) / 10000)
    for i in range(subs):
        start = i * 10000
        end = min(start + 10000, len(id_list))
        sublist = [doc['document_id'] for doc in collection.find({'document_id': {'$in': id_list[start:end]}})]
        found_ids.extend(sublist)
        print(f'{len(sublist)} | {len(found_ids)}')
    unfound_ids = set(id_list) - set(found_ids)
    print(len(unfound_ids))
    missing_dict = {}
    for document_id in unfound_ids:
        if document_id not in document_dict:
            print('not sure what to do with this ID:' + document_id)
            continue
        filename = document_dict[document_id]
        if filename not in missing_dict:
            missing_dict[filename] = []
        missing_dict[filename].append(document_id)
    return missing_dict


def lambda_handler(event, context):
    if 'body' in event:
        body = json.loads(event['body'])
    else:
        body = event
    if os.environ and 'connection_string' in os.environ:
        client = MongoClient(os.environ['connection_string'])
    else:
        return 'Could not get database connection information', 500
    if 'source' not in body:
        return 'No source information provided', 400
    source_info = body['source']
    global gcp_client
    global collection
    gcp_client = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=source_info['hmac_key_id'],
        aws_secret_access_key=source_info['hmac_secret']
    )
    db = client['test']
    collection = db['documentMetadata']
    return check_file(source_info['bucket'], source_info['filepath'])
