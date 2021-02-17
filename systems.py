from flask import request, jsonify
from elasticsearch import Elasticsearch
import json
import os
import ast



class Ranker(object):

    def __init__(self):
        self.INDEX = 'test-index'
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.PATH = './basedata/livivo/documents'


    def test(self):
        return self.es.info(), 200

    def index(self):
        docs = 0

        for file in os.listdir(self.PATH):
            if file.endswith(".jsonl"):
                with open(os.path.join(self.PATH, file), 'r', encoding="utf-8") as f:
                    for line in f:
                        try:
                            doc = ast.literal_eval(line)
                            self.es.index(index=self.INDEX, doc_type=doc['type'], id=doc['id'], body=doc)
                            docs += 1
                            print(doc['id'])
                        except:
                            pass

        return 'Index built with ' + str(docs) + ' docs', 200

    def rank_publications(self, query, page, rpp):

        itemlist = []
        start = page * rpp

        if (query is not None):

            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

            result = es.search(index=self.INDEX,
                               from_=start,
                               size=rpp,
                               body={"query": {"query_string": {"query": query, "default_field": "*"}}})

            for res in result["hits"]["hits"]:
                try:
                    itemlist.append(res['_source']['id'])
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
