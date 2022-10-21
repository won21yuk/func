import requests
import json
import pandas as pd


# 데이터 개수만큼 반복하기 - dict에 붙이기

df = pd.read_csv('./BIKE_STATION.csv', encoding='euc-kr')

row_num = int(len(df))
for i in range(row_num):
    location = df['주소1'][i]
    url = f"https://dapi.kakao.com/v2/local/search/keyword.json?query={location}"
    kakao_key = "#"
    result = requests.get(url, headers={"Authorization": f"KakaoAK {kakao_key}"})
    json_obj = result.json()

    if df.loc[i, '위도'] == 0:
        try:
            x = json_obj['documents'][0]['x']
            y = json_obj['documents'][0]['y']
            df.loc[i, '경도'] = x
            df.loc[i, '위도'] = y
        except:
            pass
    # x : 경도 / y : 위도
    # print(x, y)
print(len(df.loc[df['위도'] == 0]))

## 동코드 열 추가
df['동코드'] = ""

# 동코드 삽입
dong_cd = list()
for x, y in zip(df['경도'], df['위도']):
    kakao_key = "#"
    url = f'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?x={x}&y={y}'
    try:
        resp = requests.get(url, headers={"Authorization" : f"KakaoAK {kakao_key}"})
        result = resp.text
        json_req = json.loads(result)['documents'][1]
        # 행정동 코드 8자리 가져오기
        dong_code = json_req.get('code')[:8]
    except:
        dong_code = 'NaN'
    dong_cd.append(dong_code)

# 대여소별 행정동 정보 리스트
df['동코드'] = dong_cd

print(df)
print(len(df.loc[df['위도'] == 0]))

df.to_csv('BIKE_STATION2.csv')