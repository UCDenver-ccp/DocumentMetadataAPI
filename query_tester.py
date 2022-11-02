import http.client
import random
import json
import time
import urllib.parse


def get_ids():
    connection = http.client.HTTPSConnection("e2ewfsmmxz.us-east-1.awsapprunner.com")
    connection.request('GET', '/ids')
    response = connection.getresponse()
    response_object = json.loads(response.read())
    return response_object


def get_metadata(publication_ids):
    safe_ids = [urllib.parse.quote_plus(x) for x in publication_ids]
    connection = http.client.HTTPSConnection("e2ewfsmmxz.us-east-1.awsapprunner.com")
    connection.request('GET', f'/publications?pubids={",".join(safe_ids)}&request_id=412')
    response = connection.getresponse()
    if response.status != 200:
        return None
    r2 = response.read()
    data = json.loads(r2)
    return data['_meta'] if '_meta' in data else None


def multi_request(id_list, start_index, requests_count, step):
    random.shuffle(id_list)
    metrics = []
    for end_index in range(start_index + step, requests_count * step + start_index + step, step):
        single_metrics = get_metadata(id_list[start_index:end_index])
        if single_metrics:
            metrics.append(single_metrics)
        start_index = end_index
    return metrics


def random_trial(publication_ids):
    requests_count = 100
    metrics_list = []
    start_time = time.perf_counter()

    tens_metrics = multi_request(publication_ids, 0, requests_count, 10)
    time_total = sum(metrics['processing_time_ms'] for metrics in tens_metrics)
    time_average = time_total / float(len(tens_metrics))
    print(f"Subtotal for 10s: ({time_total},{time_average})")
    metrics_list.extend(tens_metrics)
    tens_time = time.perf_counter()

    fifties_metrics = multi_request(publication_ids, 0, requests_count, 50)
    time_total = sum(metrics['processing_time_ms'] for metrics in fifties_metrics)
    time_average = time_total / float(len(fifties_metrics))
    print(f"Subtotal for 50s: ({time_total},{time_average})")
    metrics_list.extend(fifties_metrics)
    time_total = sum(metrics['processing_time_ms'] for metrics in metrics_list)
    time_average = time_total / float(len(metrics_list))
    print(f"Overall totals: ({time_total},{time_average})")
    fifties_time = time.perf_counter()

    hundreds_metrics = multi_request(publication_ids, 0, requests_count, 100)
    time_total = sum(metrics['processing_time_ms'] for metrics in hundreds_metrics)
    time_average = time_total / float(len(hundreds_metrics))
    metrics_list.extend(hundreds_metrics)
    print(f"Subtotal for 100s: ({time_total},{time_average})")
    time_total = sum(metrics['processing_time_ms'] for metrics in metrics_list)
    time_average = time_total / float(len(metrics_list))
    print(f"Overall totals: ({time_total},{time_average})")
    hundreds_time = time.perf_counter()

    print(f'Turnaround times: \nOverall: {hundreds_time - start_time}')
    print(f'Tens: {tens_time - start_time}')
    print(f'Fifties: {fifties_time - tens_time}')
    print(f'Hundreds: {hundreds_time - fifties_time}')


if __name__ == '__main__':
    all_ids = get_ids()['ids']
    random_trial(all_ids)
