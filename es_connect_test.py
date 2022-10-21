import pprint
import json
# pip install elasticsearch
from elasticsearch import Elasticsearch
import pandas as pd

def _search_engine(keyword):
    es = Elasticsearch("http://{ip}:9200", http_auth=("es-id", "es-pw"))
    # 기업명으로 검색
    resp2 = es.search(index="corp_total_info", query={"match_phrase": {"corp_nm": keyword}})
    search_list = resp2['hits']['hits']

    return search_list

# 검색결과는 10개 제한(디폴트설정)
# 필요시 늘릴수 있음
if __name__ == "__main__":
    a = _search_engine('')
    pprint.pprint(a)

