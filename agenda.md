#Agenda:
- Elasticsearch csv einlesen / CSV to JSON transform
###Index
- Multilanguage ingestion 
	-https://towardsdatascience.com/designing-an-optimal-multi-language-search-engine-with-elasticsearch-3d2de1b9636d
	- Approach #3
###Query
- Mesh.ipynb > Apply on Query > Expand with meshterms + meshterm synonyms in systems.py>Ranker>rank_publication
	- Disorder Term adds unnecesary synonyms
### Translation of Query in English & german from elasticSearch_queries.py to systems.py > Ranker >rank_publication


___________________________________________________________________________________________________________________________
## Notes
- Preprocessing of data
	- Tokenizers with sci
	- implement in query too
- Translation of Query in English & German
	- 600 Requests per Minute
	- Size of Requests relevant
	- Character limit
	- if failure, use original query
- Mesh terms
	- Synonyms of Meshterms
	- Query Expansion
	- Search for Meshterms in Mesh field
		- delimiter (,)
___________________


create_tokens.py(preprocessing) >  SCI Token
elasticsearch_queries.py(query) > not in scripts
	- also contains mapping
tokens_functions.py > scientific
concat_filtered > csv formatting correction 
Mesh.ipynb > Mesh term recognition + synonym expansion

___________________

Data > Preprocessing > token as csv > insert into index(only , delimiter tokenizer)
									> Tokenize Title for EN,DE within Elasticsearch
									> Take Title_SCI Token as is


After Preprocessing:
	Create CSV with: Title, Title_SCI, etc++++

Title_EN, Title_DE, Title_SCI, Title_keyword



	
	

