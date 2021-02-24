# stella-micro-template-elastic

This repository provides interested experimenters with a template for integrating their ranking and recommendation systems into the [STELLA infrastructure](https://stella-project.org/) with [Elasticsearch](https://www.elastic.co). It is based on the [stella-micro-template](https://github.com/stella-project/stella-micro-template).

## Vision

Tech-savvy participants should be able to integrate their own search systems with the help of Docker. However, less technically adept users can simply rely on this repository by configuring Elasticsearch. This means they do not have to fiddle around with code or the data and **can participate by contributing a config file for Elasticsearch only!**

## Todos

- [x] Rankings based on Elasticsearch
- [ ] Config-Template for different search configurations (cf. [Similarity module](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html))
- [ ] Bulk indexing (maybe [this](https://github.com/irgroup/trec-covid/blob/master/scripts/core/elastic.py) helps as starting point)
- [ ] Recommendations based on Elasticsearch (cf. to [this](https://github.com/stella-project/gesis_rec_pyserini/blob/master/systems.py))

## Want to contribute? Here's a 'How to set it up'

0. Install python and docker
1. Clone this repository
2. Install all requirements (there's an additional `requirements.txt` in the `test/` folder)
3. Download the data (e.g. [this file](https://th-koeln.sciebo.de/s/OBm0NLEwz1RYl9N/download?path=%2Flivivo%2Fdocuments&files=livivo_testset.jsonl) and place it in `data/livivo/documents/`)
4. Run `test/docker_build_run.py`
5. Index the data by calling `http://0.0.0.0:5000/index`
6. Query the system by `http://0.0.0.0:5000/search?query=agriculture`
