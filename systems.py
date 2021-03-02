import os
import json
import jsonlines
from elasticsearch import Elasticsearch, helpers


class Ranker(object):

    def __init__(self):
        self.INDEX = 'idx'
        self.index_settings_path = os.path.join('index_settings', 'settings_default.json')
        self.index_mappings_path = os.path.join('index_settings', 'mapping_default.json')
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.documents_path = './data/livivo/documents'

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
                            '_id': obj['DBRECORDID'],
                            '_source': obj}

    def index(self):
        with open(self.index_settings_path) as json_file:
            index_settings = json.load(json_file)

        with open(self.index_mappings_path) as json_file:
            index_mappings = json.load(json_file)

        body = {"settings": index_settings, "mappings": index_mappings}

        self.es.indices.create(index=self.INDEX, body=body)

        for success, info in helpers.parallel_bulk(self.es, self.load_json(self.documents_path), index=self.INDEX):
            if not success:
                return 'A document failed: ' + info, 400

        return 'Index built with ' + ' docs', 200

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
