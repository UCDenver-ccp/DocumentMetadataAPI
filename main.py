import os
import re
import time
from functools import lru_cache

from flask import Flask, request
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

if os.environ and 'connection_string' in os.environ:
    client = MongoClient(os.environ['connection_string'])
    db = client['test']
else:
    print('local db')
    client = MongoClient()
    db = client['local']
collection = db['documentMetadata']


@app.route('/')
def health_check():
    ids = get_pm_ids(10)
    ids.extend(get_pmc_ids(10))
    ids.extend(get_other_ids(10))
    return {'ids': ids}


@app.route('/ids')
def func():
    ids = get_pm_ids(6000)
    ids.extend(get_pmc_ids(5000))
    ids.extend(get_other_ids(5000))
    return {'ids': ids}


@lru_cache()
def get_pm_ids(count):
    return [x['document_id'] for x in collection.find({'document_id': {'$regex': '^PMID'}}).limit(count)]


@lru_cache()
def get_pmc_ids(count):
    return [x['document_id'] for x in collection.find({'document_id': {'$regex': '^PMC'}}).limit(count)]


@lru_cache()
def get_other_ids(count):
    return [x['document_id'] for x in collection.find({'document_id': {'$not': {'$regex': '^PM'}}}).limit(count)]


@app.route('/publications')
def publication_lookup():
    t = time.perf_counter()
    args = request.args
    pub_ids = args['pubids'].split(',')
    corrected_pub_ids = []
    for pub_id in pub_ids:
        corrected_id = re.sub('PMC:', 'PMC', pub_id, flags=re.IGNORECASE)
        corrected_id = re.sub('DOI:', '', corrected_id, flags=re.IGNORECASE).strip()
        corrected_pub_ids.append(corrected_id)
    documents = [x for x in collection.find({'document_id': {'$in': corrected_pub_ids}})]
    results = {}
    for document in documents:
        results[document["document_id"]] = {
            'journal_name': document['journal_name'] if 'journal_name' in document else '',
            'journal_abbrev': document['journal_abbrev'] if 'journal_abbrev' in document else '',
            'article_title': document['article_title'] if 'article_title' in document else '',
            'volume': document['volume'] if 'volume' in document else '',
            'issue': document['issue'] if 'issue' in document else '',
            'pub_year': document['pub_year'] if 'pub_year' in document else '',
            'pub_month': document['pub_month'] if 'pub_month' in document else '',
            'pub_day': document['pub_day'] if 'pub_day' in document else '',
            'abstract': document['abstract'] if 'abstract' in document else ''
        }
    not_found = set(pub_ids) - set(results.keys())
    results['not_found'] = list(not_found)
    meta_object = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M%SZ', time.gmtime()),
        'n_results': len(results) - 1,
        'request_id': args['request_id'],
        'processing_time_ms': int((time.perf_counter() - t) * 1000.0),
        }
    response_object = {'_meta': meta_object, 'results': results}
    return response_object


if __name__ == '__main__':
    app.debug = True
    app.run()
