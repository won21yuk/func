from django.shortcuts import render, redirect
from folium import plugins
import folium
import geocoder
# import geojson
from .models import MyBoard, MyMembers, MentalServiceLocation
import pandas as pd
from geopy.distance import great_circle
from jinja2 import Template
#import requests

# 마우스 클릭으로 생성된 마커의 위경도값을 value값으로 전달하는 클래스
class ClickForOneMarker(folium.ClickForMarker):
    _template = Template(u"""
            {% macro script(this, kwargs) %}
                var new_mark = L.marker();
                function newMarker(e){
                    new_mark.setLatLng(e.latlng).addTo({{this._parent.get_name()}});
                    new_mark.dragging.enable();
                    new_mark.on('dblclick', function(e){ {{this._parent.get_name()}}.removeLayer(e.target)})
                    var lat = e.latlng.lat.toFixed(4),
                       lng = e.latlng.lng.toFixed(4);
                    new_mark.bindPopup({{ this.popup }});
                    parent.document.getElementById("latitude").value = lat;
                    parent.document.getElementById("longitude").value =lng;
                    };
                {{this._parent.get_name()}}.on('click', newMarker);
            {% endmacro %}
            """)  # noqa

    def __init__(self, popup=None):
        super(ClickForOneMarker, self).__init__(popup)


# 반경안에 마커 그리는 클래스
class MappingByCoord:

    def __init__(self, df, lat, lng, dist=5):

        self.df = df
        self.lat = lat
        self.lng = lng
        self.dist = dist

    # 사각형 내 좌표 필터링
    def setRectangler(self):

        lat_min = self.lat - 0.01 * self.dist
        lat_max = self.lat + 0.01 * self.dist

        lng_min = self.lng - 0.015 * self.dist
        lng_max = self.lng + 0.015 * self.dist

        self.points = [[lat_min, lng_min], [lat_max, lng_max]]

        result = self.df.loc[
            (self.df['latitude'] > lat_min) &
            (self.df['latitude'] < lat_max) &
            (self.df['longitude'] > lng_min) &
            (self.df['longitude'] < lng_max)
            ]
        result.index = range(len(result))

        return result

    # 원 내 좌표 필터링
    def setCircle(self):

        tmp = self.setRectangler()

        center = (self.lat, self.lng)

        result = pd.DataFrame()

        for index, row in tmp.iterrows():
            point = (row['latitude'], row['longitude'])
            d = great_circle(center, point).kilometers
            if d <= self.dist:
                result = pd.concat([result, tmp.iloc[index, :].to_frame().T])

        result.index = range(len(result))

        return result

    # 사각형 내 좌표 찍기
    def MappingInRectangler(self, df):

        m = folium.Map(location=[self.lat, self.lng], zoom_start=14)

        for idx, row in df.iterrows():
            lat_now = row['latitude']
            lng_now = row['longitude']

            folium.Marker(location=[lat_now, lng_now],
                          radius=15,
                          tooltip=row['agency']).add_to(m)

        folium.Rectangle(bounds=self.points,
                         color='#ff7800',
                         fill=True,
                         fill_color='#ffff00',
                         fill_opacity=0.2).add_to(m)

        return m

    # 원 내 좌표찍기
    def MappingInCircle(self, df):

        m = folium.Map(location=[self.lat, self.lng], width='70%', height='100%', zoom_start=13, tiles=None)
        folium.TileLayer('openstreetmap', name='구분').add_to(m)

        mcg = folium.plugins.MarkerCluster(control=False)
        m.add_child(mcg)
        sangdam = folium.plugins.FeatureGroupSubGroup(mcg, "상담소")
        center_ = folium.plugins.FeatureGroupSubGroup(mcg, "센터")
        ins = folium.plugins.FeatureGroupSubGroup(mcg, "시설")
        hos = folium.plugins.FeatureGroupSubGroup(mcg, "병원")
        bogun = folium.plugins.FeatureGroupSubGroup(mcg, "보건소")

        m.add_child(sangdam)
        m.add_child(center_)
        m.add_child(ins)
        m.add_child(hos)
        m.add_child(bogun)

        for idx, row in df.iterrows():

            lat_now = row['latitude']
            lng_now = row['longitude']

            div = ['상담소', '센터', '시설', '병원', '보건소']

            if row['categories'] == div[0]:
                sangdam.add_child(
                    folium.Marker([lat_now, lng_now], icon=folium.Icon(color='pink'), radius=15, tooltip=row['agency']))

            elif row['categories'] == div[1]:
                center_.add_child(
                    folium.Marker([lat_now, lng_now], icon=folium.Icon(color='green'), radius=15,
                                  tooltip=row['agency']))

            elif row['categories'] == div[2]:
                ins.add_child(
                    folium.Marker([lat_now, lng_now], icon=folium.Icon(color='blue'), radius=15, tooltip=row['agency']))

            elif row['categories'] == div[3]:
                hos.add_child(
                    folium.Marker([lat_now, lng_now], icon=folium.Icon(color='purple'), radius=15,
                                  tooltip=row['agency']))

            else:
                bogun.add_child(
                    folium.Marker([lat_now, lng_now], icon=folium.Icon(color='orange'), radius=15,
                                  tooltip=row['agency']))

        folium.Circle(radius=self.dist * 1000,
                      location=[self.lat, self.lng],
                      color="#ff7800",
                      fill_color='#ffff00',
                      fill_opacity=0.2
                      ).add_to(m)

        return m

# 맵을 그리는 메서드(iframe 형태로 template에 전달)
def makeMap(request):
    user_loc = geocoder.ip('me')
    mylocation = user_loc.latlng

    cfom = ClickForOneMarker()

    lat = mylocation[0]
    lng = mylocation[1]
    dist = 5

    df = pd.DataFrame(list(MentalServiceLocation.objects.all().values()))

    mbc = MappingByCoord(df, lat, lng, dist)

    result_radius = mbc.setCircle()

    mymap = mbc.MappingInCircle(result_radius)
    folium.Marker(location=mylocation, popup='현재 나의 위치', icon=folium.Icon(color='red', icon='star')).add_to(mymap)
    plugins.LocateControl().add_to(mymap)
    plugins.Geocoder(position='bottomright', collapsed=True, add_marker=True).add_to(mymap)

    folium.LayerControl(collapsed=True, position='bottomright').add_to(mymap)
    mymap.add_child(cfom)

    mymap.layer_name = '구분'
    mymap

    maps = mymap._repr_html_()
    return render(request, 'map_show.html', {'mymap': maps})




