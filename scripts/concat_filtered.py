import pandas as pd
import os

df = pd.DataFrame()


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


for f in os.listdir("filtered_docs/"):
    print(f)
    df_local = pd.read_csv(f"filtered_docs/{f}")
    df_local.fillna('', inplace=True)
    cols = ['DBRECORDID', 'TITLE', 'ABSTRACT', 'LANGUAGE', 'MESH', 'CHEM', 'KEYWORDS']
    if 'MESH' not in df_local.columns:
        df_local['MESH'] = ""
    if 'CHEM' not in df_local.columns:
        df_local['CHEM'] = ""
    if 'KEYWORDS' not in df_local.columns:
        df_local['KEYWORDS'] = ""
        
    df_local['TITLE'] = df_local['TITLE'].apply(prettify)
    df_local['ABSTRACT'] = df_local['ABSTRACT'].apply(prettify)
    df_local['LANGUAGE'] = df_local['LANGUAGE'].apply(prettify)

    df_local['MESH'] = df_local['MESH'].apply(prettify_v2)
    df_local['CHEM'] = df_local['CHEM'].apply(prettify_v2)
    df_local['KEYWORDS'] = df_local['KEYWORDS'].apply(prettify_v2)


    df = df.append(df_local.loc[:, cols], ignore_index=True)

df.reset_index(drop=True, inplace=True)
df.to_csv("all_filtered.csv", index=False)
