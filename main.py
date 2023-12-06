import http.client
import os
import re
import time
import json
import logging
from functools import lru_cache

from flask import Flask, request
from flask_cors import CORS
from pymongo import MongoClient
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

app = Flask(__name__)
CORS(app)
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({telemetery_service_name_key: "DocumentMetadataAPI"}))
)

jaeger_exporter = OTLPSpanExporter(endpoint="http://jaeger-otel-agent.sri:4318/v1/traces")
# jaeger_exporter = OTLPSpanExporter(endpoint="http://34.168.158.201:4318/v1/traces")
# jaeger_exporter = OTLPSpanExporter(endpoint="http://127.0.0.1:4318/v1/traces")
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)
FlaskInstrumentor().instrument_app(app, tracer_provider=trace)
# FlaskInstrumentor().instrument(enable_commenter=True, commenter_options={}, tracer_provider=trace)
PymongoInstrumentor().instrument()


if os.environ and 'connection_string' in os.environ:
    client = MongoClient(os.environ['connection_string'])
    db = client['test']
else:
    print('local db')
    client = MongoClient()
    db = client['local']
collection = db['documentMetadata']
reference = db['documentIds']


@app.route('/')
def health_check():
    ids = get_pm_ids(10)
    ids.extend(get_pmc_ids(10))
    ids.extend(get_other_ids(10))
    return {'ids': ids}


@app.route('/version')
def get_version():
    return "0.1.0", 200


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
    not_found = set(corrected_pub_ids) - set(results.keys())
    results['not_found'] = list(not_found)
    meta_object = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M%SZ', time.gmtime()),
        'n_results': len(results) - 1,
        'request_id': args['request_id'],
        'processing_time_ms': int((time.perf_counter() - t) * 1000.0),
        }
    print(meta_object)
    response_object = {'_meta': meta_object, 'results': results}
    return response_object


@app.route('/identifiers')
def id_lookup():
    args = request.args
    pub_ids = args['pubids'].split(',')
    logging.info(f"Total ids: {len(pub_ids)}")
    pmcids = [re.sub('PMC:', 'PMC', pub_id, flags=re.IGNORECASE) for pub_id in pub_ids if pub_id.upper().startswith('PMC')]
    dois = [re.sub('DOI:', '', pub_id, flags=re.IGNORECASE).strip() for pub_id in pub_ids if pub_id.upper().startswith('DOI')]
    pmids = [re.sub('PMID:', '', pub_id, flags=re.IGNORECASE) for pub_id in pub_ids if pub_id.upper().startswith('PMID')]
    logging.info(f"PMC: {len(pmcids)}\tDOI: {len(dois)}\tPMID: {len(pmids)}")
    results_dict = {}
    if len(pmcids) > 0:
        pmc_dict = {}
        found_ids = set([])
        records = [rec for rec in reference.find({'PMC': {'$in': pmcids}})]
        if len(records) > 0:
            for record in records:
                found_ids.add(record['PMC'])
                pmc_dict[record['PMC'].replace('PMC', 'PMC:')] = {
                    'PMID': 'PMID:' + record['PM'],
                    'DOI': 'DOI:' + record['DOI']
                }
        unfound_ids = set(pmcids) - found_ids
        logging.info(f"{len(found_ids)} PMC IDs found in DB")
        if len(unfound_ids) > 0:
            logging.debug(f"Checking PMC API for {len(unfound_ids)} PMC IDs")
            additional_identifiers = lookup_identifiers('pmcid', list(unfound_ids))
            logging.info(f"Found an additional {len(additional_identifiers.keys())} PMC IDs")
            pmc_dict.update(additional_identifiers)
        results_dict['PMC'] = pmc_dict
    if len(dois) > 0:
        doi_dict = {}
        found_ids = set([])
        records = [rec for rec in reference.find({'DOI': {'$in': dois}})]
        if len(records) > 0:
            for record in records:
                found_ids.add(record['DOI'])
                doi_dict['DOI:' + record['DOI']] = {
                    'PMID': 'PMID:' + record['PM'],
                    'PMC': record['PMC'].replace('PMC', 'PMC:')
                }
        unfound_ids = set(dois) - found_ids
        logging.info(f"{len(found_ids)} DOIs found in DB")
        if len(unfound_ids) > 0:
            logging.debug(f"Checking PMC API for {len(unfound_ids)} DOI IDs")
            additional_identifiers = lookup_identifiers('doi', list(unfound_ids))
            logging.info(f"Found an additional {len(additional_identifiers.keys())} DOIs")
            doi_dict.update(additional_identifiers)
        results_dict['DOI'] = doi_dict
    if len(pmids) > 0:
        pmid_dict = {}
        found_ids = set([])
        records = [rec for rec in reference.find({'PM': {'$in': pmids}})]
        if len(records) > 0:
            for record in records:
                found_ids.add(record['PM'])
                pmid_dict['PMID:' + record['PM']] = {
                    'PMC': record['PMC'].replace('PMC', 'PMC:'),
                    'DOI': 'DOI:' + record['DOI']
                }
        unfound_ids = set(pmids) - found_ids
        logging.info(f"{len(found_ids)} PMIDs found in DB")
        if len(unfound_ids) > 0:
            logging.debug(f"Checking PMC API for {len(unfound_ids)} PMIDs")
            additional_identifiers = lookup_identifiers('pmid', list(unfound_ids))
            logging.info(f"Found an additional {len(additional_identifiers.keys())} PMIDs")
            pmid_dict.update(additional_identifiers)
        results_dict['PMID'] = pmid_dict
    return results_dict


def lookup_identifiers(id_type: str, ids: list[str], sublist_size: int = 200) -> dict:
    synonyms_dict = {}
    start_index = sublist_size
    end_index = len(ids)
    extra = end_index % sublist_size
    for cap in range(start_index, end_index, sublist_size):
        connection = http.client.HTTPSConnection('www.ncbi.nlm.nih.gov')
        id_sublist = ids[cap - sublist_size: cap]
        request_string = f'/pmc/utils/idconv/v1.0/?ids={",".join(id_sublist)}&idtype={id_type}'
        request_string += '&format=json&versions=no&tool=documentmetadataapi&email=edgargaticacu@gmail.com'
        logging.debug('sending request to %s', request_string)
        connection.request('GET', request_string)
        response = connection.getresponse()
        if response.status == 200:
            response_text = response.read()
            response_data = json.loads(response_text)
            if 'records' in response_data:
                for record in response_data['records']:
                    if id_type.lower() == 'pmid':
                        key = 'PMID:' + record['pmid']
                        value = {
                            'PMC': record['pmcid'].replace('PMC', 'PMC:') if 'pmcid' in record else '',
                            'DOI': 'DOI:' + record['doi'] if 'doi' in record else ''
                        }
                    elif id_type.lower() == 'pmcid':
                        key = record['pmcid'].replace('PMC', 'PMC:')
                        value = {
                            'PMID': 'PMID:' + record['pmid'] if 'pmid' in record else '',
                            'DOI': 'DOI:' + record['doi'] if 'doi' in record else ''
                        }
                    else:
                        key = 'DOI:' + record['doi']
                        value = {
                            'PMC': record['pmcid'].replace('PMC', 'PMC:') if 'pmcid' in record else '',
                            'PMID': 'PMID:' + record['pmid'] if 'pmid' in record else '',
                        }
                    synonyms_dict[key] = value
    id_sublist = ids[-extra:]
    connection = http.client.HTTPSConnection('www.ncbi.nlm.nih.gov')
    request_string = f'/pmc/utils/idconv/v1.0/?ids={",".join(id_sublist)}&idtype={id_type}'
    request_string += '&format=json&versions=no&tool=documentmetadataapi&email=edgargaticacu@gmail.com'
    logging.debug('sending request to %s', request_string)
    connection.request('GET', request_string)
    response = connection.getresponse()
    if response.status == 200:
        response_text = response.read()
        response_data = json.loads(response_text)
        if 'records' in response_data:
            for record in response_data['records']:
                if id_type.lower() == 'pmid':
                    key = 'PMID:' + record['pmid']
                    value = {
                        'PMC': record['pmcid'].replace('PMC', 'PMC:') if 'pmcid' in record else '',
                        'DOI': 'DOI:' + record['doi'] if 'doi' in record else ''
                    }
                elif id_type.lower() == 'pmcid':
                    key = record['pmcid'].replace('PMC', 'PMC:')
                    value = {
                        'PMID': 'PMID:' + record['pmid'] if 'pmid' in record else '',
                        'DOI': 'DOI:' + record['doi'] if 'doi' in record else ''
                    }
                else:
                    key = 'DOI:' + record['doi']
                    value = {
                        'PMC': record['pmcid'].replace('PMC', 'PMC:') if 'pmcid' in record else '',
                        'PMID': 'PMID:' + record['pmid'] if 'pmid' in record else '',
                    }
                synonyms_dict[key] = value
    return synonyms_dict


if __name__ == '__main__':
    app.debug = True
    app.run()
