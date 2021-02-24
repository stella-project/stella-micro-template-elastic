import jsonlines
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Ranker(object):

    def __init__(self):
        self.INDEX = 'idx'
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.PATH = './data/livivo/documents'

    def test(self):
        return self.es.info(), 200

    def load_json(self, directory):
        for path, subdir, file in os.walk(directory):
            extensions = tuple([".jsonl"])
            files = [f for f in file if f.endswith(extensions)]
            for f in files:
                with jsonlines.open(os.path.join(path, f), 'r') as reader:
                    for obj in reader:
                        yield {
                            '_op_type': 'index',
                            '_index': self.INDEX,
                            '_type': 'record',
                            '_id': obj['DBRECORDID'],
                            '_source': obj,
                        }

    def index(self):
        docs = 0
        path = "./data/livivo/documents/"

        for success, info in helpers.parallel_bulk(self.es, self.load_json(path), index=self.INDEX):
            if not success:
                print('A document failed:', info)

        return 'Index built with ' + str(docs) + ' docs', 200

    def rank_publications(self, query, page, rpp):

        itemlist = []
        start = page * rpp

        if (query is not None):

            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

            result = es.search(index=self.INDEX,
                               from_=start,
                               size=rpp,
                               body={"query": {"multi_match": {"query": query, "fields": ["TITLE", 'ABSTRACT']}}})

            for res in result["hits"]["hits"]:
                try:
                    itemlist.append(res['_source']['DBRECORDID'])
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
