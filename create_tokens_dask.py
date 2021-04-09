import pandas as pd
import numpy as np
import dask.dataframe as dd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
# from elasticsearch import Elasticsearch
import en_core_sci_lg
import de_core_news_lg

import os

# client = Elasticsearch([{'host': 'localhost'}, {'port': 9200}])

nlp_german = de_core_news_lg.load()

nlp_sci = en_core_sci_lg.load()


# UNIVERSAL
def is_token_allowed_german(token):
    '''
         Only allow valid tokens which are not stop words
         and punctuation symbols.
    '''
    if not token or not token.text.strip() or token.is_stop or token.is_punct:
        return False
    return True


def preprocesstoken_german(token):
    # Reduce token to its lowercase lemma form
    return token.lemma_.strip().lower()


def tokenize_string_german(x):
    return ",".join(
        [preprocesstoken_german(token) for token in nlp_german(prettify(x)) if is_token_allowed_german(token)])


# SCIENTIFIC
def is_token_allowed_sci(token):
    '''
         Only allow valid tokens which are not stop words
         and punctuation symbols.
    '''
    if not token or not token.text.strip() or token.is_stop or token.is_punct:
        return False
    return True


def preprocesstoken_sci(token):
    # Reduce token to its lowercase lemma form
    return token.lemma_.strip().lower()


def tokenize_string_sci(x):
    return ",".join([preprocesstoken_sci(token) for token in nlp_sci(prettify(x)) if is_token_allowed_sci(token)])


def prettify(x):
    if type(x) == list:
        x = x[0]
    return x.replace("[", "").replace("]", "").lstrip("'").rstrip("'").lstrip('"').rstrip('"')


def prettify_v2(x):
    if type(x) == list:
        x = x[0]
    return x.replace("[", "").replace("]", "").replace("'", "").replace('"', "")


for f in os.listdir("./data/filtered_documents/"):
    path = "./data/filtered_documents/" + f + "/data-0.jsonl"
    #if filename.endswith('.jsonl'):
    df = dd.read_json(path, lines=True, orient="records", encoding="UTF-8")
    df.fillna('')

    # print(df[0:5])

    german_mask = df['LANGUAGE'] == 'ger'
    else_mask = (df['LANGUAGE'] != 'ger') | (df['LANGUAGE'] == '')

    # TITLE
    df['TITLE_TOKENZ_GERMAN'] = df.loc[german_mask, 'TITLE']
    df.loc[german_mask, 'TITLE_TOKENZ_GERMAN'] = df.loc[german_mask, 'TITLE_TOKENZ_GERMAN'].apply(
        tokenize_string_german, meta=("TITLE_TOKENZ_GERMAN","str"))
    print("title_tokenz_german")

    df['TITLE_TOKENZ_SCI'] = df.loc[else_mask, 'TITLE']
    df.loc[else_mask, 'TITLE_TOKENZ_SCI'] = df.loc[else_mask, 'TITLE_TOKENZ_SCI'].apply(tokenize_string_sci, meta=("TITLE_TOKENZ_SCI","str"))
    print("title_tokenz_sci")

    # ABSTRACT
    df['ABSTRACT_TOKENZ_GERMAN'] = df.loc[german_mask, 'ABSTRACT']
    df.loc[german_mask, 'ABSTRACT_TOKENZ_GERMAN'] = df.loc[german_mask, 'ABSTRACT_TOKENZ_GERMAN'].apply(
        tokenize_string_german, meta=("ABSTRACT_TOKENZ_GERMAN","str"))
    print("abstract_tokenz_german")

    df['ABSTRACT_TOKENZ_SCI'] = df.loc[else_mask, 'ABSTRACT']
    df.loc[else_mask, 'ABSTRACT_TOKENZ_SCI'] = df.loc[else_mask, 'ABSTRACT_TOKENZ_SCI'].apply(tokenize_string_sci, meta=("ABSTRACT_TOKENZ_SCI","str"))
    print("abstract_tokenz_sci")

    df['KEYWORDS_TOKENZ'] = df['KEYWORDS'].apply(tokenize_string_sci, meta=("KEYWORDS_TOKENZ","str"))
    print("keywords")

    df['MESH_TOKENZ'] = df['MESH'].apply(tokenize_string_sci, meta=("MESH_TOKENZ","str"))
    print("mesh_to")

    df['CHEM_TOKENZ'] = df['CHEM'].apply(tokenize_string_sci, meta=("CHEM_TOKENZ","str"))
    print("chem_to")
    df.fillna('')

    #df.to_json(f"/home/joshth22/filtered_docs/{f}/prep_data-*.jsonl", orient="records", index=True, lines=True, force_ascii=False)
    df.to_json(f"data/preprocessed_docs/{f}/prep_data-*.jsonl", orient="records", index=True, lines=True, force_ascii=False)
    # df.to_csv(f"tokenz_german_and_sci.csv", index=False)
