import requests
import random
import json
import time


def get_ids():
    response = requests.get("https://e2ewfsmmxz.us-east-1.awsapprunner.com/ids")
    response_object = json.loads(response.text)
    return response_object


def get_metadata(publication_ids):
    response = requests.get("https://e2ewfsmmxz.us-east-1.awsapprunner.com/publications",
                            params={"pubids": ','.join(publication_ids), "request_id": 154})
    data = json.loads(response.text)
    return data['_meta'] if '_meta' in data else None


def multi_request(id_list, start_index, requests_count, step):
    random.shuffle(id_list)
    metrics = []
    for end_index in range(start_index + step, requests_count * step + start_index + step, step):
        metrics.append(get_metadata(id_list[start_index:end_index]))
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
