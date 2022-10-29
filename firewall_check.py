"""
pip install oauth2client
pip install google-api-python-client
pip install tabulate
pip install pandas
"""

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import requests, json
import pandas as pd


credentials = GoogleCredentials.get_application_default()

service = discovery.build('compute', 'v1', credentials=credentials)

# Project ID for this request.
project = '자신의 프로젝트명을 입력하세요'  # TODO: Update placeholder value.

request = service.firewalls().list(project=project)
lst = []
while request is not None:
    response = request.execute()

    for firewall in response['items']:
        # TODO: Change code below to process each `firewall` resource:
        sourceRanges = firewall['sourceRanges']
        # all open 방화벽 여부 체크
        if '0.0.0.0/0' not in sourceRanges:
            continue
        # json 형태로 만들기
        firewall_id = firewall['id']
        firewall_name = firewall['name']
        firewall_network = firewall['network'].split('/')[-1]
        traffic_direction = firewall['direction']
        creation_date = firewall['creationTimestamp']

        dic = {}
        dic['firewall_id'] = firewall_id
        dic['firewall_name'] = firewall_name
        dic['firewall_network'] = firewall_network
        dic['traffic_direction'] = traffic_direction
        dic['creation_date'] = creation_date
        lst.append(dic)

    request = service.firewalls().list_next(previous_request=request, previous_response=response)


# 데이터 프레임 만들기
df = pd.DataFrame(lst)

# 슬랙에 연결 및 사용할 메세지 세팅
slack_webhook_url = "자신의 webhook URL 입력하세요"

# 슬랙 메세지는 마크다운 언어를 지원하기 때문에 마크다운 언어로 구성되도록 작성해도 됨.
message_result = ("GCP 인스턴스 방화벽에 0.0.0.0/0 으로 오픈 된 방화벽 정책이 있습니다.\n\n"
                  + "```"
                  + df.to_markdown()
                  + "```"
                  + "\n")
slack_message = ":bell:" + " *방화벽 모니터링* \n" + message_result

# 슬랙에 메세지 보내기
payload = {"text": slack_message,
           "icon_emoji": "false"}
requests.post(slack_webhook_url,
              data=json.dumps(payload),
              headers={"Content-Type": "application/json"})