import concurrent
import logging
import os
from concurrent import futures

from waiter import http_util, terminal


def query_across_clusters(clusters, query_fn):
    """Attempts to query entities from the given clusters."""
    count = 0
    all_entities = {'clusters': {}}
    max_workers = os.cpu_count()
    logging.debug('querying with max workers = %s' % max_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cluster = {query_fn(c, executor): c for c in clusters}
        for future, cluster in future_to_cluster.items():
            entities = future.result()
            cluster_count = entities['count']
            if cluster_count > 0:
                all_entities['clusters'][cluster['name']] = entities
                count += cluster_count
    all_entities['count'] = count
    return all_entities


def get_token(cluster, token_name, include=None):
    """Gets the token with the given name from the given cluster"""
    params = {'token': token_name}
    if include:
        params['include'] = include
    token_data, headers = http_util.make_data_request(cluster, lambda: http_util.get(cluster, 'token', params=params))
    etag = headers.get('ETag', None)
    return token_data, etag


def no_data_message(clusters):
    """Returns a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    message = terminal.failed(f'No matching data found in {clusters_text}.')
    message = f'{message}\nDo you need to add another cluster to your configuration?'
    return message


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    print(no_data_message(clusters))