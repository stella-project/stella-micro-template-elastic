import pandas as pd
import os
import dask.dataframe as dd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


#data = ["livivo_medline_00.jsonl", "livivo_medline_02.jsonl", "livivo_medline_06.jsonl", "livivo_agris.jsonl"]

def prettify(x):
    typ = type(x)
    if typ == list:
        x = " ".join(x)
    return x.replace("[", "").replace("]", "").replace("'", "").replace('"', "")


def prettify_v2(x):
    typ = type(x)
    if typ == list:
        x = " ".join(x)
    return x.replace("[", "").replace("]", "").replace("'", "").replace('"', "")


for f in os.listdir("./data/livivo/documents"):
    print(f)
   
    df_local = dd.read_json("./data/livivo/documents/" + f, lines=True, orient="records", encoding="utf-8",
                            engine=pd.read_json)
    df_local = df_local.drop(["AUTHOR","SOURCE","PUBLCOUNTRY","IDENTIFIER","EISSN","PISSN","DOI","VOLUME","ISSUE","PAGES","ISSN","DATABASE","DOCUMENTURL"], axis=1)
    df_local.fillna('') #inplace=True)

    if 'MESH' not in df_local.columns:
        df_local['MESH'] = ""
    print("first if")
    if 'CHEM' not in df_local.columns:
        df_local['CHEM'] = ""
    print("second if")
    if 'KEYWORDS' not in df_local.columns:
        df_local['KEYWORDS'] = ""
    print("third if")

    df_local['DBRECORDID'] = df_local.DBRECORDID.astype(str)
    df_local['TITLE'] = df_local.TITLE.astype(str)
    df_local['ABSTRACT'] = df_local.ABSTRACT.astype(str)
    df_local['LANGUAGE'] = df_local.LANGUAGE.astype(str)
    df_local['MESH'] = df_local.MESH.astype(str)
    df_local['CHEM'] = df_local.CHEM.astype(str)
    df_local['KEYWORDS'] = df_local.KEYWORDS.astype(str)
    df_local['PUBLDATE'] = df_local.PUBLDATE.astype(str)
    df_local['PUBLYEAR'] = df_local.PUBLYEAR.astype(str)

    df_local['TITLE'] = df_local['TITLE'].apply(prettify, meta=("TITLE","str"))#, meta="string")
    df_local['ABSTRACT'] = df_local['ABSTRACT'].apply(prettify, meta=("ABSTRACT","str"))#, meta="string")
    df_local['LANGUAGE'] = df_local['LANGUAGE'].apply(prettify, meta=("LANGUAGE","str"))#, meta="string")
    print("added three cols")
    df_local['MESH'] = df_local['MESH'].apply(prettify_v2, meta=("MESH","str"))#, meta="string")
    df_local['CHEM'] = df_local['CHEM'].apply(prettify_v2, meta=("CHEM","str"))#, meta="string")
    df_local['KEYWORDS'] = df_local['KEYWORDS'].apply(prettify_v2, meta=("KEYWORDS","str"))#, meta="string")
    df_local['PUBLDATE'] = df_local['PUBLDATE'].apply(prettify_v2, meta=("PUBLDATE","str"))#, meta="string")
    df_local['PUBLYEAR'] = df_local['PUBLYEAR'].apply(prettify_v2, meta=("PUBLYEAR","str"))#, meta="string")

    f = f.replace(".jsonl","")
    #df_local.to_json(f"/home/joshth22/filtered_docs/{f}/data-*.jsonl", orient="records", index=True, lines=True, force_ascii=False)
    df_local.to_json(f"data/filtered_documents/{f}/data-*.jsonl", orient="records", index=True, lines=True, force_ascii=False)