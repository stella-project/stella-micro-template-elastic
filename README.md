<a href="https://stella-project.org/"><img align="right" width="100" src="doc/img/logo-st.JPG" /></a>
# Solr micro template of the STELLA infrastructure 

This repository provides interested experimenters with a template for integrating their ranking and recommendation systems into the [STELLA infrastructure](https://stella-project.org/) with [Solr](https://solr.apache.org/). It is based on the [stella-micro-template](https://github.com/stella-project/stella-micro-template).

## Vision

Tech-savvy participants should be able to integrate their own search systems with the help of Docker. However, less technically adept users can simply rely on this repository by configuring Solr. This means they do not have to fiddle around with code or the data and **can participate by contributing config files for Solr only!**

## Want to contribute? Here's a 'How to set it up'

0. Install python and docker
1. Clone this repository
2. Install all requirements (there's an additional `requirements.txt` in the `test/` folder)
3. Decide on the Task ([Ad-hoc retrieval of scientific documents](https://clef-lilas.github.io/tasks#task-1-ad-hoc-search-ranking) or [Research dataset recommendation](https://clef-lilas.github.io/tasks#task-2-research-data-recommendations)) you want to use this template for and commend out either the `Ranker` or `Recommender` from [app.py](https://github.com/stella-project/stella-micro-template-solr/blob/main/app.py)
4. Download the data (e.g. [this file](https://th-koeln.sciebo.de/s/OBm0NLEwz1RYl9N/download?path=%2Flivivo%2Fdocuments&files=livivo_testset.jsonl) and place it in `data/livivo/documents/`)
5. Specify the `index` and `data` directories in [docker_build_run.py](https://github.com/stella-project/stella-micro-template-solr/blob/test/docker_build_run.py)
6. Configure your Solr system. In the [index_settings/](https://github.com/stella-project/stella-micro-template-solr/tree/main/index_settings) folder are basic configurations for each task.
7. Run `test/docker_build_run.py`
8. Index the data by calling `http://0.0.0.0:5000/index`
9. Query the system by `http://0.0.0.0:5000/search?query=agriculture`
