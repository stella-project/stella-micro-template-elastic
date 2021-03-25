from elasticsearch import Elasticsearch
from pprint import pprint as pp
from google_trans_new import google_translator
import pandas as pd
import numpy as np
import time


import tokens_functions as tf

from pprint import pprint as pp

# Variante 1
# topicFile = "C:/Users/Fabian/Desktop/elastic/cord-metadata_qrels_topics/topics-rnd5_covid-complete.xml"
run = "filler_final_run"

root = pd.read_json("C:/Users/Fabian/PycharmProjects/Livivo/data/livivo/candidates/livivo_hq_1000.jsonl", lines=True)
client = Elasticsearch([{'host': 'localhost'}, {'port': 9200}])

path = "C:/Users/Fabian/PycharmProjects/Livivo/git/livivo_project/"

candidates = pd.read_json(path_or_buf="data/livivo/candidates/livivo_hq_100_candidates.jsonl", lines=True)
#
#
# rel_docs = candidates.candidates.values
#
# rel_docs_ids = []
# for l in rel_docs:
#     for x in l:
#         rel_docs_ids.append(x)
#
# rel_docs_ids = list(set(rel_docs_ids))



translator = google_translator()

# %%
df = pd.DataFrame(columns=['qid', 'query', 'new_query', 'cnt_results', 'relevant_found'])

p = 0

with open(path + run + '.txt', 'w') as f:
    for i_topic in range(618, len(root)):
        time.sleep(1.5)

        print(p)
        p += 1
        query = root.iloc[i_topic, 1]


        topic_id = root.iloc[i_topic, 0]

        rel_candidates_qid = candidates[candidates['qid:'] == topic_id]['candidates'].values[0]
        operator = 'or'
        if ' AND ' in query and not ' OR ' in query:
            operator = 'and'

        query_lan = query.replace(" OR", "").replace(" AND", "")
        query_eng = translator.translate(query_lan, lang_tgt='en')
        query_de = translator.translate(query_lan, lang_tgt='de')

        if type(query_de) == list:
            query_de = query_de[1]

        query_tokenized_german = tf.tokenize_string_german(query_de)
        query_tokenized_ori = tf.tokenize_string_sci(query)
        query_tokenized_eng = tf.tokenize_string_sci(query_eng)


        search = client.search(body={
            "query": {
                "bool": {
                    "should": [{
                        "query_string": {
                            "query": query_tokenized_ori,
                            "default_operator": operator,
                            "fields": ["TITLE_TOKENZ_SCI", "ABSTRACT_TOKENZ_SCI", "MESH_TOKENZ^5", "CHEM_TOKENZ^5",
                                       "KEYWORDS_TOKENZ^5"],
                            "analyzer": "comma"
                        }
                    },

                        {
                            "query_string": {
                                "query": query_tokenized_eng,
                                "default_operator": operator,
                                "fields": ["TITLE_TOKENZ_SCI", "ABSTRACT_TOKENZ_SCI", "MESH_TOKENZ^5", "CHEM_TOKENZ^5",
                                           "KEYWORDS_TOKENZ^5"],
                                "analyzer": "comma"
                            }
                        },
                        {
                            "query_string": {
                                "query": query_tokenized_german,
                                "default_operator": operator,
                                "fields": ["TITLE_TOKENZ_GERMAN", "ABSTRACT_TOKENZ_GERMAN"],
                                "analyzer": "comma"

                            }}
                    ]
                }
            }

        }, index="livivo_ger_sci", size="1000")

        counter = 0

        result_len = len(search['hits'].get('hits'))

        candidates_query = candidates[candidates['qid:'] == topic_id]['candidates'].values[0]
        relevant_found = []
        for result in search['hits'].get('hits'):
            result_id = result['_source'].get('DBRECORDID')
            if result_id in candidates_query:
                relevant_found.append(result_id)

        relevant_found = list(set(relevant_found))

        # print(relevant_found)

        df = df.append({'qid': topic_id, 'query': query, 'new_query': query_tokenized_ori,
                        'cnt_results': result_len,
                        'relevant_found': len(relevant_found)}, ignore_index=True)

        #if result_len > 0:
        for i, hit in enumerate(search['hits'].get('hits'), 1):
            if counter == 99:
                break
            id = hit['_source'].get('DBRECORDID')
            if id in rel_candidates_qid:
                line = " ".join(
                    [str(topic_id), "0", str(hit['_source'].get('DBRECORDID')), str(counter),
                     str(hit.get('_score')),
                     str(run), str('\n')])
                f.write(line)
                counter += 1
                del rel_candidates_qid[rel_candidates_qid.index(id)]

        rel_candidates_qid_reversed = rel_candidates_qid[::-1]
        pseudo_score = 1
        while counter <= 99:
            line = " ".join(
                [str(topic_id), "0", str(rel_candidates_qid_reversed.pop()), str(counter),
                 str(pseudo_score),
                 str(run), str('\n')])
            f.write(line)
            pseudo_score -= 0.001
            counter += 1

# %%
df['cnt_results'].replace(0, np.nan, inplace=True)
df['cnt_results'].fillna(1, inplace=True)
df['share'] = df['relevant_found'] / df['cnt_results']
df.sort_values(by='share', ascending=False, inplace=True)
df.reset_index(inplace=True, drop=True)
# %%


df.sort_values(by='relevant_found', ascending=False, inplace=True)
# if len(set(ids)) < len(ids):
#     i = [id_ for id_ in ids if ids.count(id_) > 1]
#     print(topic_id,  i)

# %%

df.to_csv("result_evaluation.csv", index=False)
# %%
from elasticsearch import Elasticsearch
import elasticsearch_dsl
from pprint import pprint as pp
import os
import urllib.request
import urllib.parse
import json
import xml.etree.ElementTree as ET

# %%

# root = ET.parse(topicFile).getroot()
client = Elasticsearch([{'host': 'localhost'}, {'port': 9200}])

os.chdir("C:/Users/Fabian/PycharmProjects/Livivo")

# %%
body_index = {
    "settings": {
        "similarity":
            {
                "my_similarity": {
                    "type": "DFR",
                    "basic_model": "g",
                    "after_effect": "b",
                    "normalization": "z"
                }
            },
        "analysis": {
            "tokenizer": {
                "comma": {
                    "type": "pattern",
                    "pattern": ","
                }
            },
            "analyzer": {
                "comma": {
                    "type": "custom",
                    "tokenizer": "comma"
                },
                "whitespace": {
                    "type": "custom",
                    "tokenizer": "whitespace"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "DBRECORDID": {
                "type": "text",
                "index": False,
            },
            "TITLE": {
                "type": "text",
                "index": False
            },
            "ABSTRACT": {
                "type": "text",
                "index": False
            },
            "LANGUAGE": {
                "type": "text",
                "index": False
            },
            "MESH": {
                "type": "text",
                "index": False
            },
            "CHEM": {
                "type": "text",
                "index": False
            },
            "KEYWORDS": {
                "type": "text",
                "index": False
            },
            "TITLE_TOKENZ_GERMAN": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"

            }, "TITLE_TOKENZ_SCI": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"

            },
            "ABSTRACT_TOKENZ_GERMAN": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"

            },
            "ABSTRACT_TOKENZ_SCI": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"

            },
            "KEYWORDS_TOKENZ": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"

            },
            "MESH_TOKENZ": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"
            },
            "CHEM_TOKENZ": {
                "type": "text",
                "index": True,
                "analyzer": "comma",
                "similarity": "my_similarity"
            }
        }
    }
}

# make an API call to the Elasticsearch cluster
# and have it return a response:
response = client.indices.create(
    index="livivo_ger_sci",
    body=body_index,
    ignore=400  # ignore 400 already exists code
)

# print out the response:
print('response:', response)
#%%
l = [1,2,3]

re = l[::-1]

p = re.pop()
#%%&
for x in range(1,5):
    print(x)

