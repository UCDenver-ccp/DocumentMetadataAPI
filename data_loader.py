import http.client

import boto3
import gzip
import json
import os
import botocore.exceptions
from pymongo import MongoClient, UpdateOne
from botocore.config import Config


def get_synonyms(document_ids):
    syn_dict = {}
    for doc in reference.find({'PM': {'$in': document_ids}}):
        syn_dict[doc['PM']] = [doc['PMC'] if 'PMC' in doc else '', doc['DOI'] if 'DOI' in doc else '']
    id_set = set(document_ids)
    lookup_dict = lookup_synonyms(list(id_set - syn_dict.keys()))
    return syn_dict | lookup_dict


def lookup_synonyms(ids: list[str], sublist_size: int = 200) -> dict:
    synonyms_dict = {}
    start_index = sublist_size
    end_index = len(ids)
    extra = end_index % sublist_size
    for cap in range(start_index, end_index, sublist_size):
        connection = http.client.HTTPConnection('www.ncbi.nlm.nih.gov')
        id_sublist = ids[cap - sublist_size: cap]
        numerical_ids = [x.replace('PMID:', '') for x in id_sublist]
        request_string = f'/pmc/utils/idconv/v1.0/?ids={",".join(numerical_ids)}'
        request_string += '&format=json&versions=no&tool=documentmetadataapi&email=edgargaticacu@gmail.com'
        connection.request('GET', request_string)
        response = connection.getresponse()
        if response.status == 200:
            response_text = response.read()
            response_data = json.loads(response_text)
            if 'records' in response_data:
                for record in response_data['records']:
                    synonyms_dict[record['pmid']] = [record['pmcid'] if 'pmcid' in record else '',
                                                     record['doi'] if 'doi' in record else '']
    id_sublist = ids[-extra:]
    numerical_ids = [x.replace('PMID:', '') for x in id_sublist]
    connection = http.client.HTTPConnection('www.ncbi.nlm.nih.gov')
    request_string = f'/pmc/utils/idconv/v1.0/?ids={",".join(numerical_ids)}'
    request_string += '&format=json&versions=no&tool=documentmetadataapi&email=edgargaticacu@gmail.com'
    connection.request('GET', request_string)
    response = connection.getresponse()
    if response.status == 200:
        response_text = response.read()
        response_data = json.loads(response_text)
        if 'records' in response_data:
            for record in response_data['records']:
                synonyms_dict[record['pmid']] = [record['pmcid'] if 'pmcid' in record else '',
                                                 record['doi'] if 'doi' in record else '']
    return synonyms_dict


def get_existing_documents(document_ids):
    return [doc['document_id'] for doc in collection.find({'document_id': {'$in': document_ids}})]


def insert_new_documents(pubmed_documents, synonyms_dict):
    other_documents = []
    for doc in pubmed_documents:
        pm = doc['document_id']
        pmc_id = synonyms_dict[pm]['PMC']
        doi = synonyms_dict[pm]['DOI']
        if pmc_id and len(pmc_id) > 0:
            pmc_record = doc.copy()
            pmc_record['document_id'] = pmc_id
            other_documents.append(pmc_record)
        if doi and len(doi) > 0:
            doi_record = doc.copy()
            doi_record['document_id'] = doi
            other_documents.append(doi_record)
    result = collection.insert_many(pubmed_documents)
    print(f'Inserted {len(pubmed_documents)} new PubMed documents')
    print(result)
    result = collection.insert_many(other_documents)
    print(f'Inserted {len(other_documents)} non-PubMed documents')
    print(result)
    return {'pubmed': len(pubmed_documents), 'other': len(other_documents)}


def upsert_documents(pubmed_documents, synonyms_dict):
    if len(pubmed_documents) == 0:
        return {'pubmed': 0, 'other': 0}
    pm_count = len(pubmed_documents)
    pmc_count = 0
    doi_count = 0
    ops_list = []
    for doc in pubmed_documents:
        pm = doc['document_id']
        pmid = pm.split(':')[-1]
        pmc_id = synonyms_dict[pmid][0] if pmid in synonyms_dict else ''
        doi = synonyms_dict[pmid][1] if pmid in synonyms_dict else ''
        ops_list.append(UpdateOne({'document_id': pm}, {'$set': doc}, upsert=True))
        if pmc_id and len(pmc_id) > 0:
            pmc_count += 1
            pmc_doc = doc.copy()
            pmc_doc['document_id'] = pmc_id
            ops_list.append(UpdateOne({'document_id': pmc_id}, {'$set': pmc_doc}, upsert=True))
        if doi and len(doi) > 0:
            doi_count += 1
            doi_doc = doc.copy()
            doi_doc['document_id'] = doi
            ops_list.append(UpdateOne({'document_id': doi}, {'$set': doi_doc}, upsert=True))
    result = collection.bulk_write(ops_list, ordered=False)
    print(f'Updated {pm_count} PubMed documents')
    print(f'Updated {pmc_count} PubMedCentral documents')
    print(f'Updated {doi_count} DOI documents')
    return {
        'pubmed': pm_count,
        'pmc': pmc_count,
        'doi': doi_count,
        'results': {
            'matched': result.matched_count,
            'upsert': result.upserted_count,
            'update': result.modified_count
        }
    }


def delete_existing_documents(document_ids):
    return collection.delete_many({'document_id': {'$in': document_ids}}).deleted_count


def load_file(filepath):
    documents = []
    print('loading ' + filepath)
    with open(filepath, 'r') as infile:
        for line in infile:
            columns = line.split('\t')
            if len(columns) < 10:
                print(line)
                continue
            documents.append({
                'document_id': columns[0],
                'pub_year': columns[1],
                'pub_month': columns[2],
                'pub_day': columns[3] if not columns[3] == '-' else '',
                'journal_name': columns[4] if len(columns[4]) > 1 else '',
                'journal_abbrev': columns[5] if len(columns[5]) > 1 else '',
                'volume': columns[6] if len(columns[6]) > 1 else '',
                'issue': columns[7] if len(columns[7]) > 1 else '',
                'article_title': columns[8] if len(columns[8]) > 1 else '',
                'abstract': columns[9] if len(columns[9]) > 1 else '',
            })
    print(f'{len(documents)} documents loaded')
    return documents


def get_file(remote_bucket, remote_filename):
    print(f'Attempting to get file {remote_filename} from bucket {remote_bucket}')
    try:
        with open('/tmp/source.gz', 'wb') as dest:
            gcp_client.download_fileobj(remote_bucket, remote_filename, dest)
        print('file downloaded, attempting to extract to plaintext file')
        with gzip.open('/tmp/source.gz', 'rb') as gzfile:
            byte_contents = gzfile.read()
            with open('/tmp/source.tsv', 'wb') as tsvfile:
                count = tsvfile.write(byte_contents)
        print('file created')
        os.remove('/tmp/source.gz')
        return '/tmp/source.tsv'
    except botocore.exceptions.ClientError as error:
        print("A ClientError happened")
        print(error)
        return None
    except:
        print("An error other than ClientError happened")
        return None


def process_file(local_filepath, is_delete=False):
    if is_delete:
        id_list = [x.strip() for x in open(local_filepath, 'r').readlines()]
        if len(id_list) == 0:
            return {'delete': 0}
        print(f'deleting existing documents ({len(id_list)})')
        return {'delete': delete_existing_documents(id_list)}
    pubmed_documents = load_file(local_filepath)
    print('file loaded')
    os.remove(local_filepath)
    id_list = [doc['document_id'].split(':')[-1] for doc in pubmed_documents]
    existing_ids = get_existing_documents(id_list)
    new_ids = list(set(id_list) - set(existing_ids))
    synonyms = get_synonyms(id_list)
    print('got synonyms\nupserting documents')
    upsert_results = upsert_documents(pubmed_documents, synonyms)
    return {'load_results': upsert_results, 'sample_ids': new_ids[:5]}


def lambda_handler(event, context):
    print('starting')
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
    print(source_info['bucket'], source_info['filepath'])
    global gcp_client
    global collection
    global reference
    print('connected to DocDB\nconnecting to GCP')
    gcp_client = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=source_info['hmac_key_id'],
        aws_secret_access_key=source_info['hmac_secret'],
        config=Config(connect_timeout=5, retries={'max_attempts': 0})
    )
    db = client['test']
    collection = db['documentMetadata']
    reference = db['documentIds']
    print('getting file')
    local_filepath = get_file(source_info['bucket'], source_info['filepath'])
    if not local_filepath:
        if '/' in source_info['filepath']:
            return {'result': f"could not get file {source_info['filepath'].split('/')[-1]}"}
        return {'result': f"could not get file {source_info['filepath']}"}
    print('processing file')
    return process_file(source_info['bucket'], source_info['filepath'])
