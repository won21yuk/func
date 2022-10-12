from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search, Q
from django.http import JsonResponse

def elastic_client_set():
    # elasticsearch와 연결
    client = connections.create_connection(hosts=['http://{ip}:9200'], http_auth=('es-id', 'es-pw'))
    s = Search(using=client)

    # 상세정보 확인한 기업명 기준으로 grouping
    body = {"size": 0, "aggs": {"by_corp": {"terms": {"field": "result_corp_nm.keyword", "size": 5},
                                            "aggs": {"addrs": {"top_hits": {"_source": ['result_corp_addr']}}}
                                            }}}

    # 1주일 간의 데이터로 시간 범위 설정
    s = Search.from_dict(body).filter("range", **{"@timestamp": {"gte": "now-7d", "lte": "now"}})
    s = s.index("targetlog-*")
    s = s.doc_type("target_log")

    return s

# 1. elasticsearch와 연결
s = elastic_client_set()
q_by_job = Q("bool", must=[Q("match", user_like_job="IT·웹·통신업")])
s = s.query(q_by_job)
t = s.execute()
favorite_job_lst = []
for item in t.aggregations.by_corp.buckets:
    dic = {}
    dic['corp_nm'] = item.key
    addr_split = item.addrs.hits.hits[0]._source.result_corp_addr.split(' ')
    dic['addr'] = addr_split[0] + " " + addr_split[1]
    favorite_job_lst.append(dic)

print(favorite_job_lst)


# 1. elasticsearch와 연결
client = connections.create_connection(hosts=['http://220.86.100.9:9200'], http_auth=('elastic', 'votmdnjem'))
s = Search(using=client)

# 2. job과 일치하는 값들만 필터링
## job 변수에 들어갈 값은 reqeust로 유저의 선호직업군을 받아서 입력
q_by_job = Q("bool", must=[Q("match", user_like_job="IT·웹·통신업")])

# 3. result_corp_nm로 그룹 짓기(size: 상위 5개 출력)
body = {"size": 0, "aggs": {"by_corp": {"terms": {"field": "result_corp_nm.keyword", "size": 5},
                                        "aggs": {"addrs" : {"top_hits": {"_source": ['result_corp_addr']}}}
                                        }}}

# 4. 시간 범위 필터 걸어 주기( now-7d(현재 시간 기준 7일전) <= ... <= now(현재 시간) )
s = Search.from_dict(body).filter("range", **{"@timestamp": {"gte": "now-7d", "lte": "now"}})
s = s.query(q_by_job)
s = s.index("targetlog-*")
s = s.doc_type("target_log")

t = s.execute()
# 5. 반복문 걸어서 필요 필드 가져 오기
favorite_job_lst = []
for item in t.aggregations.by_corp.buckets:
    dic = {}
    dic['corp_nm'] = item.key
    addr_split = item.addrs.hits.hits[0]._source.result_corp_addr.split(' ')
    dic['addr'] = addr_split[0] + " " + addr_split[1]
    favorite_job_lst.append(dic)

print(favorite_job_lst)