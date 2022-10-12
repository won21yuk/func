from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search, Q


def search_company(request):
    context = {}
    if request.method == "GET":
        context['message'] = "search company"
        context['user_email'] = request.session.get('user_email', False)

        logging_click(request)
        return render(request, 'search.html', {'context': context})
    elif request.method == "POST":
        req_corp_nm = request.POST.get('corp_nm', False)
        logging_search(request, req_corp_nm)

        client = connections.create_connection(hosts=['http://220.86.100.9:9200'], http_auth=('elastic', 'votmdnjem'))
        s = Search(using=client)
        s = s.index("corp_total_info")
        q = Q("multi_match", query=keyword, fields=["corp_nm", "corp_nm_eng"])
        s = s.query(q)

        t = s.execute()

        search_list = []
        for item in t:
            del item['@timestamp'], item['@version']
            search_list.append(item.to_dict())

        context['corp_result'] = search_list
        context['corp_num'] = len(search_list)
        logging_click(request)

        return JsonResponse(context, status=200)


