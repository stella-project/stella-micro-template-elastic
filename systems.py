from flask import request, jsonify
from elasticsearch import Elasticsearch
import json
import os
import ast

PATH = '/data/index'

class Ranker(object):

    def __init__(self):
        self.idx = None
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def index(self):
        docs = 0

        for file in os.listdir(PATH):
            if file.endswith(".jsonl"):
                with open(os.path.join(PATH, file), 'r', encoding="utf-8") as f:
                    for line in f:
                        try:
                            doc = ast.literal_eval(line)
                            self.es.index(index=INDEX, doc_type=doc['type'], id=doc['id'], body=doc)
                            docs += 1
                        except:
                            pass

        return 'Index built with ' + str(docs) + ' docs', 200

    def rank_publications(self, query, page, rpp):

        itemlist = []

        if (query is not None):

            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

            result = es.search(index=INDEX,
                               from_=start,
                               size=rpp,
                               body={"query": {"query_string": {"query": query, "default_field": "*"}}})

            for res in result["hits"]["hits"]:
                try:
                    response['itemlist'].append(res['_source']['id'])
                except:
                    pass

        return {
            'page': page,
            'rpp': rpp,
            'query': query,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }


class Recommender(object):

    def __init__(self):
        self.idx = None

    def index(self):
        pass

    def recommend_datasets(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }

    def recommend_publications(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }
