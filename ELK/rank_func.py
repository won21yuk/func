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

# 일주일 간 이용자(회원+비회원)이 가장 많이 검색한 기업
# 기업 상세 정보 확인(클릭)까지 이루어 졌을 경우를 유효값으로 카운팅
def search_corp_rank(reqeust):
    t = elastic_client_set.execute()

    corp_rank_list = []
    for item in t.aggregations.by_corp.buckets:
        dic = {}
        dic['corp_nm'] = item.key
        addr_split = item.addrs.hits.hits[0]._source.result_corp_addr.split(' ')
        dic['addr'] = addr_split[0] + " " + addr_split[1]
        corp_rank_list.append(dic)

    return corp_rank_list


# 일주일 간 희망업종이 같은 지원자(회원)들이 가장 많이 검색한 기업
# 기업 상세 정보 확인까지 이루어진 경우를 유효값으로 카운팅
def rank_by_job(request):
    # 1. elasticsearch와 연결
    client = connections.create_connection(hosts=['http://{ip}:9200'], http_auth=('es-id', 'es-pw'))
    s = Search(using=client)

    # 2. job과 일치하는 값들만 필터링
    ## job 변수에 들어갈 값은 reqeust로 유저의 선호직업군을 받아서 입력
    q_by_job = Q("bool", must=[Q("match", user_like_job=job)])

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

    return favorite_job_lst


# 일주일 간 희망근무지가 같은 지원자(회원)이 가장 많이 검색한 기업
# 기업 상세 정보 확인까지 이루어진 경우를 유효값으로 카운팅
def rank_by_region(region):
    # 1. elasticsearch와 연결
    client = connections.create_connection(hosts=['http://{ip}:9200'], http_auth=('es-ip', 'es-pw'))
    s = Search(using=client, index="targetlog-*", doc_type="target_log")

    # 2. job과 일치하는 값들만 필터링
    ## region 변수에 들어갈 값은 reqeust로 유저의 선호근무지역을 받아서 입력
    q_by_region = Q("bool", must=[Q("match", user_like_workzone=region)])

    # 3. result_corp_nm로 그룹 짓기(size: 상위 5개 출력)
    body = {"size": 0, "aggs": {"by_corp": {"terms": {"field": "result_corp_nm.keyword", "size": 5},
                                            "aggs": {"addrs" : {"top_hits": {"_source": ['result_corp_addr']}}}
                                            }}}

    # 4. 시간 범위 필터 걸어주기( now-7d(현재 시간 기준 7일전) <= ... <= now(현재 시간) )
    s = Search.from_dict(body).filter("range", **{"@timestamp": {"gte": "now-7d", "lte": "now"}})
    s = s.query(q_by_region)
    s = s.index("targetlog-*")
    s = s.doc_type("target_log")

    t = s.execute()

    # 5. 반복문 걸어서 필요한 필드 가져오기
    favorite_region_lst = []
    for item in t.aggregations.by_corp.buckets:
        dic = {}
        dic['corp_nm'] = item.key
        addr_split = item.addrs.hits.hits[0]._source.result_corp_addr.split(' ')
        dic['addr'] = addr_split[0] + " " + addr_split[1]
        favorite_region_lst.append(dic)

    return favorite_region_lst



# 일주일간 이용자들(회원, 비회원)이 가장 많이 검색한 키워드
# 검색창에 실제로 어떤 검색어들이 입력되었는지 집계
# 용도는 실시간 검색어 혹은 검색엔진의 검색기능 향상을 위한 데이터 수집 정도
def keyword_rank(request):
    client = connections.create_connection(hosts=['http://{ip}:9200'], http_auth=('es-id', 'es-pw'))
    s = Search(using=client)

    # 검색어(keyword) 기준 grouping
    body = {"size": 0, "aggs": {"by_keyword": {"terms": {"field": "keyword.keyword", "size": 5}}}}

    s = Search.from_dict(body).filter("range", **{"@timestamp": {"gte": "now-7d", "lte": "now"}})
    s = s.index("searchlog-*")
    s = s.doc_type("search_log")

    # 유저(s1)/비유저(s2) 검색어 구분
    q1 = Q("bool", must_not=[Q("match", request_user="no_user")])
    q2 = Q("bool", must=[Q("match", request_user="no_user")])
    s1 = s.query(q1)
    s2 = s.query(q2)

    t1 = s1.execute()
    t2 = s2.execute()

    member_keyword_rank = []
    non_member_keyword_rank = []
    for item in t1.aggregations.by_keyword.buckets:
        member_keyword_rank.append(item.key)

    for item in t2.aggregations.by_keyword.buckets:
        non_member_keyword_rank.append(item.key)

    return member_keyword_rank, non_member_keyword_rank