import pandas as pd
import os

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

def main():
    for f in os.listdir("data/livivo/documents"):
        print(f)
        df_local = pd.read_json("data/livivo/documents/" + f, lines=True, orient="values", encoding="utf-8")
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

        #df = df.append(df_local.loc[:, cols], ignore_index=True)
        #df.reset_index(drop=True, inplace=True)
        df_local.to_json(f"data/filtered_documents/{f}", orient="records",  index=True, lines=True, force_ascii=False)

if __name__ == '__main__':
    main()

