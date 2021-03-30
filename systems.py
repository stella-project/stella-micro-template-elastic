import os
import json
import jsonlines
from elasticsearch import Elasticsearch, helpers
import re
from google_trans_new import google_translator
import tokens_functions as tf
from elasticsearch.client.ingest import IngestClient


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


def load_settings(settings_path):
    with open(settings_path) as json_file:
        return json.load(json_file)


def load_query_settings(query_settings_path, operator, query_tokenized_ori,
                        query_tokenized_eng, query_tokenized_german):
    f = open("query_settings\query_settings.txt", "r+", encoding="utf-8")
    content = f.read()
    content = content.replace('"query": "query_tokenized_ori",', '"query": "' + query_tokenized_ori + '",')
    content = content.replace('"query": "query_tokenized_eng",', '"query": "' + query_tokenized_eng + '",')
    content = content.replace('"query": "query_tokenized_german",', '"query": "' + query_tokenized_german + '",')
    content = content.replace('"default_operator": "operator",', '"default_operator": "' + operator + '",')
    #print(content)
    # query_pattern = '"query": ' + query + ","

    # strings = ""
    # for i in f:
    #    result = re.sub(r'^"query": "query_tokenized_ori",', query_pattern, i)
    #    strings = strings + result
    # return json.dumps(content,separators=None)
    return content


def load_ingest_pipeline_settings(es, pipeline_settings_path):
    p = IngestClient(es)
    #content = json.loads(pipeline_settings_path)
    #print(content.read())
    with open(pipeline_settings_path) as json_file:
        content = json.load(json_file)
    p.put_pipeline(id='attachment', body=content)

    return p

class Ranker(object):
    def __init__(self):
        self.INDEX = 'idx'
        self.index_settings_path = os.path.join('index_settings', 'test_settings_multilang.json')
        self.query_settings_path = os.path.join('query_settings', 'query_settings.txt')
        self.pipeline_settings_path = os.path.join("pipeline_settings", "pipeline_settings.json")

        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        # self.documents_path = './data/livivo/documents'
        self.documents_path = './data/livivo/test'

    def test(self):
        return self.es.info(), 200

    def index(self):
        if self.es.indices.exists(self.INDEX):
            self.es.indices.delete(index=self.INDEX)

        load_ingest_pipeline_settings(self.es, self.pipeline_settings_path)

        self.es.indices.create(index=self.INDEX, body=load_settings(self.index_settings_path))

        for success, info in helpers.parallel_bulk(self.es, load_json(self.documents_path, 'DBRECORDID'),
                                                   index=self.INDEX, pipeline="attachment"):
            if not success:
                return 'A document failed: ' + info, 400
        '''
        for filename in os.listdir(self.documents_path):
            if filename.endswith('.jsonl'):
                fullpath=os.path.join(self.documents_path, filename)
                print("Inserting file ->", filename)
                with open(fullpath, "r", encoding="utf8") as open_file:
                    reader = jsonlines.Reader(open_file)
                    #json_docs.append(jsonlines.Reader(open_file))
                    helpers.bulk(self.es, reader, ignore = 400,index=self.INDEX, raise_on_error=False, stats_only=False)
        '''
        return 'Index built with ' + ' docs', 200

    def rank_publications(self, query, page, rpp):

        itemlist = []
        start = page * rpp
        if query is not None:
            translator = google_translator()
            operator = 'OR'  ### Kritisch weil User potentiell mehrere operatoren kombiniren könnte
            if ' AND ' in query and not ' OR ' in query:  # wahrscheinlich nur query_string besser
                operator = 'AND'

            query_eng = translator.translate(query, lang_tgt='en')
            query_de = translator.translate(query, lang_tgt='de')

            if type(query_de) == list:
                query_de = query_de[1]

            query_tokenized_german = tf.tokenize_string_german(query_de)
            query_tokenized_ori = tf.tokenize_string_sci(query)
            query_tokenized_eng = tf.tokenize_string_sci(query_eng)

            #print(query_tokenized_eng)
            #print(query_tokenized_german)
            #print(query_tokenized_ori)
            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

            result = es.search(index=self.INDEX,
                               from_=start,
                               size=rpp,
                               body=load_query_settings(self.query_settings_path, operator, query_tokenized_ori,
                                                        query_tokenized_eng, query_tokenized_german))

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
        self.es.indices.create(index=self.index_documents, body=load_settings(self.index_documents_settings_path))
        self.es.indices.create(index=self.index_datasets, body=load_settings(self.index_documents_settings_path))

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

        return 'Indices built', 200

    def recommend_datasets(self, item_id, page, rpp):
        itemlist = []

        start = page * rpp

        if item_id is not None:

            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

            result = es.search(index=self.index_documents,
                               from_=start,
                               size=rpp,
                               body={"query": {"multi_match": {"query": item_id, "fields": ["id"]}}})

            if result["hits"]["hits"]:
                title = result["hits"]["hits"][0]['_source']['title']
                es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

                result = es.search(index=self.index_datasets,
                                   from_=start,
                                   size=rpp,
                                   body={"query": {"multi_match": {"query": title, "fields": ["title", 'abstract']}}})

            for res in result["hits"]["hits"]:
                try:
                    itemlist.append(res['_source']['id'])
                except:
                    pass

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
