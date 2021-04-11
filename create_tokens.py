import pandas as pd
import numpy as np

# from elasticsearch import Elasticsearch
import en_core_sci_lg
import de_core_news_lg
import os
import datetime
import string

# %%


# client = Elasticsearch([{'host': 'localhost'}, {'port': 9200}])

nlp_german = de_core_news_lg.load(exclude=["parser", "ner", "tok2vec", "textcat"])

nlp_sci = en_core_sci_lg.load(exclude=["parser", "ner", "tok2vec", "textcat"])


# %%
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


# def tokenize_german(x):
#     try:
#         return str([preprocesstoken_german(token) for token in nlp_german(x) if is_token_allowed_german(token)])
#     except:
#         return str([])


def tokenize_string_german(x):
    return ",".join([preprocesstoken_german(token) for token in nlp_german(x) if is_token_allowed_german(token)])


def tokenize_german_numpy(x):
    return np.array(
        [[",".join([preprocesstoken_german(token) for token in nlp_german(i) if is_token_allowed_german(token)])] for i
         in x], dtype=str).reshape((len(x),))


def tokenize_sci_numpy(x):
    return np.array(
        [[",".join([preprocesstoken_sci(token) for token in nlp_sci(i) if is_token_allowed_sci(token)])] for i in x],
        dtype=str).reshape((len(x),))


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


# def tokenize_sci(x):
#     try:
#         return str([preprocesstoken_sci(token) for token in nlp_sci(x) if is_token_allowed_sci(token)])
#     except:
#         return str([])


# def tokenize_string_sci(x):
#     return ",".join([preprocesstoken_sci(token) for token in nlp_sci(x) if is_token_allowed_sci(token)])


def tokenize_string_sci(x):
    string = ""
    for token in nlp_sci(x):
        if is_token_allowed_sci(token):
            string = string + "," + preprocesstoken_sci(token)
    return string


def prettify(x):
    if type(x) == list:
        x = x[0]

    # Test
    return x.translate(x.maketrans("", "", "[]'\""))
    # return x.replace("[", "").replace("]", "").lstrip("'").rstrip("'").lstrip('"').rstrip('"')


def prettify_v2(x):
    if type(x) == list:
        x = x[0]
    # Test
    return x.translate(x.maketrans("", "", "[]'\""))
    # return x.replace("[", "").replace("]", "").replace("'", "").replace('"', "")

def main():

    for f in os.listdir("data/livivo/documents"):
        df_chunks = pd.read_json(path_or_buf=f"data/livivo/documents/{f}", lines=True, chunksize=10000) # increase Chunksize to atleast 100 000 on the VM
        
        for df in df_chunks:
            cols = ['DBRECORDID', 'TITLE', 'ABSTRACT', 'LANGUAGE', 'MESH', 'CHEM', 'KEYWORDS']
            if 'MESH' not in df.columns:
                df['MESH'] = ""
            if 'CHEM' not in df.columns:
                df['CHEM'] = ""
            if 'KEYWORDS' not in df.columns:
                df['KEYWORDS'] = ""

            df = df[cols]
            df.fillna('', inplace=True)

            df['TITLE'] = df['TITLE'].apply(prettify)
            df['ABSTRACT'] = df['ABSTRACT'].apply(prettify)
            df['LANGUAGE'] = df['LANGUAGE'].apply(prettify)

            df['MESH'] = df['MESH'].apply(prettify_v2)
            df['CHEM'] = df['CHEM'].apply(prettify_v2)
            df['KEYWORDS'] = df['KEYWORDS'].apply(prettify_v2)

            #
            # df = df.iloc[:10000]

            # TITLE
            df['TITLE_TOKENZ_GERMAN'] = ""
            df['TITLE_TOKENZ_SCI'] = ""
            df['ABSTRACT_TOKENZ_GERMAN'] = ""
            df['ABSTRACT_TOKENZ_SCI'] = ""
            df['KEYWORDS_TOKENZ'] = ""
            df['MESH_TOKENZ'] = ""
            df['CHEM_TOKENZ'] = ""

            numpy_df = df.to_numpy()

            german_mask_numpy = numpy_df[:, 3] == "ger"

            else_mask_numpy = (numpy_df[:, 3] != "ger") | (numpy_df[:, 3] == "")

            # TITLE
            numpy_df[german_mask_numpy, 7] = np.apply_along_axis(tokenize_german_numpy, 0, numpy_df[german_mask_numpy, 1])
            print(datetime.datetime.now(), "  german title")
            numpy_df[else_mask_numpy, 8] = np.apply_along_axis(tokenize_sci_numpy, 0, numpy_df[else_mask_numpy, 1])
            print(datetime.datetime.now(), "  sci title")

            # ABSTRACT
            numpy_df[german_mask_numpy, 9] = np.apply_along_axis(tokenize_german_numpy, 0, numpy_df[german_mask_numpy, 2])
            print(datetime.datetime.now(), "  german abstract")

            numpy_df[else_mask_numpy, 10] = np.apply_along_axis(tokenize_sci_numpy, 0, numpy_df[else_mask_numpy, 2])
            print(datetime.datetime.now(), "  sci abstract")

            # KEYWORDS_TOKENZ
            numpy_df[:, 11] = np.apply_along_axis(tokenize_sci_numpy, 0, numpy_df[:, 6])
            print(datetime.datetime.now(), "  keywords")

            # MESH_TOKENZ
            numpy_df[:, 12] = np.apply_along_axis(tokenize_sci_numpy, 0, numpy_df[:, 4])
            print(datetime.datetime.now(), "  mesh")

            # CHEM_TOKENZ
            numpy_df[:, 13] = np.apply_along_axis(tokenize_sci_numpy, 0, numpy_df[:, 5])
            print(datetime.datetime.now(), "  chem")

            new_df = pd.DataFrame(numpy_df, columns=df.columns)
            #print(df.columns)
            #new_df = new_df.drop(["AUTHOR", "INSTITUTION", "PUBLISHER", "SOURCE", "PUBLDATE", "PUBLYEAR", "PUBLPLACE", "PUBLCOUNTRY","IDENTIFIER", "EISSN", "PISSN", "DOI", "VOLUME", "ISSUE", "PAGES", "ISSN", "DATABASE", "DOCUMENTURL"], axis=1)
        # FOR TEST ONLY REMOVE THE BREAK!!
        #break
        # if file does not exist write header
            f = f.replace(".jsonl", ".csv")
            if os.path.isfile(f"prep_data/filtered_documents/{f}"):
                new_df.to_csv(f"prep_data/filtered_documents/{f}", mode='a', header=False, index=False)
            else:  # else it exists so append without writing the header
                new_df.to_csv(f"prep_data/filtered_documents/{f}", header=df.columns, index=False)

if __name__ == '__main__':
    main()