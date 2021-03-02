import os
import json
import jsonlines
from elasticsearch import Elasticsearch, helpers


def load_json(directory, id_field):
    for path, subdir, file in os.walk(directory):
        extensions = tuple([".jsonl"])
        files = [f for f in file if f.endswith(extensions)]
        for f in files:
            with jsonlines.open(os.path.join(path, f), 'r') as reader:
                for obj in reader:
                    yield {
                        '_op_type': 'index',
                        '_id': obj[id_field],
                        '_source': obj}


class Ranker(object):

    def __init__(self):
        self.INDEX = 'idx'
        self.index_settings_path = os.path.join('index_settings', 'livivo_settings.json')
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.documents_path = './data/livivo/documents'

    def test(self):
        return self.es.info(), 200

    def index(self):
        with open(self.index_settings_path) as json_file:
            index_settings = json.load(json_file)

        self.es.indices.create(index=self.INDEX, body=index_settings)

        for success, info in helpers.parallel_bulk(self.es, load_json(self.documents_path, 'DBRECORDID'),
                                                   index=self.INDEX):
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
        self.index_documents = 'documents'
        self.index_documents_settings_path = os.path.join('index_settings', 'gesis-search_documents_settings.json')
        self.documents_path = './data/gesis-search/documents'

        self.index_datasets = 'datasets'
        self.index_datasets_settings_path = os.path.join('index_settings', 'gesis-search_datasets_settings.json')
        self.datasets_path = './data/gesis-search/datasets'

        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def index(self):
        with open(self.index_documents_settings_path) as json_file:
            index_settings = json.load(json_file)
        self.es.indices.create(index=self.index_documents, body=index_settings)

        with open(self.index_datasets_settings_path) as json_file:
            index_settings = json.load(json_file)
        self.es.indices.create(index=self.index_datasets, body=index_settings)

        for success, info in helpers.parallel_bulk(self.es,
                                                   load_json(self.documents_path,
                                                             'id'), index=self.index_documents):
            if not success:
                return 'A document failed: ' + info, 400

        for success, info in helpers.parallel_bulk(self.es,
                                                   load_json(self.documents_path,
                                                             'id'), index=self.index_datasets):
            if not success:
                return 'A document failed: ' + info, 400

        return 'Index built with ' + ' docs', 200

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
