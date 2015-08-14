
import sys
import threading
import argparse
import os
from os.path import join
import time
import Import
from elasticsearch import Elasticsearch

usleep = lambda x: time.sleep(x/1000000.0)

def main():
    args = argparse.ArgumentParser(description='Load the contents of all json files in a directory recursively and bulk index them into ElasticSearch')
    args.add_argument('path', help='Path to the directory to traverse')
    args.add_argument('-t', '--threads', default=5, help='The maximum number of threads (default: 5)')
    parsed_args = args.parse_args()

    e = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

    for root, dirs, files in os.walk(parsed_args.path):
        # Removes anything that starts with a '.'
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        files[:] = [f for f in files if not f.startswith('.')]

        for f in files:
            if f.endswith('.json'):
                while threading.activeCount() >= parsed_args.threads:
                    usleep(500000) # 1/2 second sleep while max threads are currently running

                t = threading.Thread(
                    name='import',
                    target=Import.processFile,
                    args=(join(root, f), e)
                )
                t.start()

if __name__ == "__main__":
    main()
