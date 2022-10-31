import boto3
import gzip
import json
import os
from pymongo import MongoClient


def get_synonyms(document_ids):
    syn_dict = {}
    for doc in reference.find({'PM': {'$in': document_ids}}):
        syn_dict[doc['PM']] = [doc['PMC'] if 'PMC' in doc else '', doc['DOI'] if 'DOI' in doc else '']
    return syn_dict


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


def update_existing_documents(pubmed_documents, synonyms_dict):
    bulk = collection.initialize_unordered_bulk_op()
    pm_count = len(pubmed_documents)
    pmc_count = 0
    doi_count = 0
    for doc in pubmed_documents:
        pm = doc['document_id']
        pmc_id = synonyms_dict[pm]['PMC']
        doi = synonyms_dict[pm]['DOI']
        bulk.find({'document_id': pm}).update({'$set': {
            'journal_name': doc['journal_name'] if 'journal_name' in doc else '',
            'journal_abbrev': doc['journal_abbrev'] if 'journal_abbrev' in doc else '',
            'article_title': doc['article_title'] if 'article_title' in doc else '',
            'volume': doc['volume'] if 'volume' in doc else '',
            'issue': doc['issue'] if 'issue' in doc else '',
            'pub_year': doc['pub_year'] if 'pub_year' in doc else '',
            'pub_month': doc['pub_month'] if 'pub_month' in doc else '',
            'pub_day': doc['pub_day'] if 'pub_day' in doc else '',
            'abstract': doc['abstract'] if 'abstract' in doc else ''
        }})
        if pmc_id and len(pmc_id) > 0:
            pmc_count += 1
            bulk.find({'document_id': pmc_id}).update({'$set': {
                'journal_name': doc['journal_name'] if 'journal_name' in doc else '',
                'journal_abbrev': doc['journal_abbrev'] if 'journal_abbrev' in doc else '',
                'article_title': doc['article_title'] if 'article_title' in doc else '',
                'volume': doc['volume'] if 'volume' in doc else '',
                'issue': doc['issue'] if 'issue' in doc else '',
                'pub_year': doc['pub_year'] if 'pub_year' in doc else '',
                'pub_month': doc['pub_month'] if 'pub_month' in doc else '',
                'pub_day': doc['pub_day'] if 'pub_day' in doc else '',
                'abstract': doc['abstract'] if 'abstract' in doc else ''
            }})
        if doi and len(doi) > 0:
            doi_count += 1
            bulk.find({'document_id': pmc_id}).update({'$set': {
                'journal_name': doc['journal_name'] if 'journal_name' in doc else '',
                'journal_abbrev': doc['journal_abbrev'] if 'journal_abbrev' in doc else '',
                'article_title': doc['article_title'] if 'article_title' in doc else '',
                'volume': doc['volume'] if 'volume' in doc else '',
                'issue': doc['issue'] if 'issue' in doc else '',
                'pub_year': doc['pub_year'] if 'pub_year' in doc else '',
                'pub_month': doc['pub_month'] if 'pub_month' in doc else '',
                'pub_day': doc['pub_day'] if 'pub_day' in doc else '',
                'abstract': doc['abstract'] if 'abstract' in doc else ''
            }})
    result = bulk.execute()
    print(f'Updated {pm_count} PubMed documents')
    print(f'Updated {pmc_count} PubMedCentral documents')
    print(f'Updated {doi_count} DOI documents')
    print(result)
    return {'pubmed': pm_count, 'pmc': pmc_count, 'doi': doi_count}


def load_file(filepath):
    documents = []
    lines = open(filepath, 'r').read().splitlines()
    for line in lines:
        columns = line.split('\t')
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
    return documents


def process_file(remote_bucket, remote_filename):
    with open('source.gz', 'wb') as dest:
        gcp_client.download_fileobj(remote_bucket, remote_filename, dest)
    with gzip.open('source.gz', 'rb') as gzfile:
        byte_contents = gzfile.read()
        with open('source.tsv', 'wb') as tsvfile:
            count = tsvfile.write(byte_contents)
    pubmed_documents = load_file('source.tsv')
    id_list = [doc['document_id'] for doc in pubmed_documents]
    ids_to_update = get_existing_documents(id_list)
    ids_to_create = list(set(id_list) - set(ids_to_update))
    synonyms = get_synonyms(id_list)
    insert_metrics = insert_new_documents([doc for doc in pubmed_documents if doc['document_id'] in ids_to_create], synonyms)
    update_metrics = update_existing_documents([doc for doc in pubmed_documents if doc['document_id'] in ids_to_update], synonyms)
    return {'insert': insert_metrics, 'update': update_metrics}


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
    global reference
    gcp_client = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=source_info['hmac_key_id'],
        aws_secret_access_key=source_info['hmac_secret']
    )
    db = client['test']
    collection = db['documentMetadata']
    reference = db['documentIds']
    return process_file(source_info['bucket'], source_info['filepath'])
