from flask import request, jsonify
from elasticsearch import Elasticsearch
import json
import jsonlines
import os
import ast


class Ranker(object):

    def __init__(self):
        self.INDEX = 'idx'
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.PATH = './data/livivo/documents'

    def test(self):
        return self.es.info(), 200

    def index(self):
        docs = 0
        for file in os.listdir("./data/livivo/documents/"):
            if file.endswith(".jsonl"):
                with jsonlines.open(os.path.join("./data/livivo/documents", file)) as reader:
                    for obj in reader:
                        try:
                            _id = obj.get('DBRECORDID')
                            self.es.index(index=self.INDEX, doc_type='PUB', id=_id, body=obj)
                            docs += 1
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
