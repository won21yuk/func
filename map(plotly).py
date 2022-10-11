from django.shortcuts import render
from .models import *
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from plotly.offline import plot


# km -> mile 변환
def km_to_mile(km):
    mile = km * 0.621371
    return float(mile)


# coords = [lon, lat]
def _get_map(collection, coords, distance):
    client = MongoClient("mongodb://team01:1234@54.248.183.216", 27017)
    db = client['pjt2']
    coll = db[collection]
    dist = km_to_mile(distance) / 3963.2
    collection_list = []
    cursor = coll.find({
        'location': {
            '$geoWithin': {
                '$centerSphere': [coords, dist]
            }
        }
    }, {'_id': 0})
    for doc in cursor:
        # if len(doc['location']['coordinates'][0])
        doc['lon'] = doc['location']['coordinates'][0]
        doc['lat'] = doc['location']['coordinates'][1]
        del doc['location']
        collection_list.append(doc)
    df = pd.DataFrame(collection_list)

    return df


def ranking():
    client = MongoClient("mongodb://team01:1234@54.248.183.216", 27017)
    db = client['pjt2']
    bike_station = db['BIKE_STATION']
    id_list = ['207', '502', '2715']
    info_list = []
    for id in id_list:
        info = bike_station.find({'bike_station_id': f'{id}'}, {'_id': 0})[0]
        info['coords'] = [info['location']['coordinates'][0], info['location']['coordinates'][1]]
        info['lon'] = info['location']['coordinates'][0]
        info['lat'] = info['location']['coordinates'][1]
        del info['location']
        info_list.append(info)

    return info_list


def transportation_facility(request):
    # 교통시설 : BUS_STATION / SUBWAY_STATION / BIKE_ROAD
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("C:\workspaces\project_2\Project_Engineering\seoul_bike\seoul_bike\mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_bus1 = _get_map('BUS_STATION', center[0], 2)
    df_sub1 = _get_map('SUBWAY_STATION', center[0], 2)
    # df_road1 = _get_map('BIKE_ROAD', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_bus2 = _get_map('BUS_STATION', center[1], 2)
    df_sub2 = _get_map('SUBWAY_STATION', center[1], 2)
    # df_road2 = _get_map('BIKE_ROAD', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_bus3 = _get_map('BUS_STATION', center[2], 2)
    df_sub3 = _get_map('SUBWAY_STATION', center[2], 2)
    # df_road3 = _get_map('BIKE_ROAD', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_bus1, lat='lat', lon='lon', hover_name='bus_station_name', width=300, height=300, color_discrete_sequence=['purple'])
    fig_first.add_trace(px.scatter_mapbox(df_sub1, lat='lat', lon='lon', hover_name='subway_station_name', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_first.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_bus2, lat='lat', lon='lon', hover_name='bus_station_name', width=300, height=300, color_discrete_sequence=['purple'])
    fig_second.add_trace(px.scatter_mapbox(df_sub2, lat='lat', lon='lon', hover_name='subway_station_name', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_second.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))
    # 3등 대여소
    fig_third = px.scatter_mapbox(df_bus3, lat='lat', lon='lon', hover_name='bus_station_name', width=300, height=300, color_discrete_sequence=['purple'])
    fig_third.add_trace(px.scatter_mapbox(df_sub3, lat='lat', lon='lon', hover_name='subway_station_name', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_third.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    return render(request, 'map_show.html', {'TF_mymap1': plot_div1, 'TF_mymap2': plot_div2, 'TF_mymap3': plot_div3})

def neighborhood_facility(request):
    # 근린시설 : PARK / MALL
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("C:\workspaces\project_2\Project_Engineering\seoul_bike\seoul_bike\mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_park1 = _get_map('PARK', center[0], 2)
    df_mall1 = _get_map('MALL', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_park2 = _get_map('PARK', center[1], 2)
    df_mall2 = _get_map('MALL', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_park3 = _get_map('PARK', center[2], 2)
    df_mall3 = _get_map('MALL', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_park1, lat='lat', lon='lon', hover_name='park_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_first.add_trace(px.scatter_mapbox(df_mall1, lat='lat', lon='lon', hover_name='mall_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_first.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_park2, lat='lat', lon='lon', hover_name='park_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_second.add_trace(px.scatter_mapbox(df_mall2, lat='lat', lon='lon', hover_name='mall_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_second.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))
    # 3등 대여소
    fig_third = px.scatter_mapbox(df_park3, lat='lat', lon='lon', hover_name='park_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_third.add_trace(px.scatter_mapbox(df_mall3, lat='lat', lon='lon', hover_name='mall_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])
    fig_third.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    return render(request, 'map_show2.html', {'NF_mymap1': plot_div1, 'NF_mymap2': plot_div2, 'NF_mymap3': plot_div3})

def education_facility(request):
    # 교육/문화 : SCHOOL / TOUR_PLACE / CULTURE_PLACE / EVENT_PLACE
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("C:\workspaces\project_2\Project_Engineering\seoul_bike\seoul_bike\mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_school1 = _get_map('SCHOOL', center[0], 2)
    df_tour1 = _get_map('TOUR_PLACE', center[0], 2)
    df_culture1 = _get_map('CULTURE_PLACE', center[0], 2)
    df_event1 = _get_map('EVENT_PLACE', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_school2 = _get_map('SCHOOL', center[1], 2)
    df_tour2 = _get_map('TOUR_PLACE', center[1], 2)
    df_culture2 = _get_map('CULTURE_PLACE', center[1], 2)
    df_event2 = _get_map('EVENT_PLACE', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_school3 = _get_map('SCHOOL', center[2], 2)
    df_tour3 = _get_map('TOUR_PLACE', center[2], 2)
    df_culture3 = _get_map('CULTURE_PLACE', center[2], 2)
    df_event3 = _get_map('EVENT_PLACE', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_school1, lat='lat', lon='lon', hover_name='school_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_first.add_trace(px.scatter_mapbox(df_tour1, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_culture1, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['green']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_event1, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['red']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])

    fig_first.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_school2, lat='lat', lon='lon', hover_name='school_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_second.add_trace(px.scatter_mapbox(df_tour2, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_culture2, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['green']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_event2, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['red']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])

    fig_second.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))

    # 3등 대여소
    fig_third = px.scatter_mapbox(df_school3, lat='lat', lon='lon', hover_name='school_nm', width=300, height=300, color_discrete_sequence=['purple'])
    fig_third.add_trace(px.scatter_mapbox(df_tour3, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['blue']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_culture3, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['green']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_event3, lat='lat', lon='lon', hover_name='place_nm', width=300, height=300, color_discrete_sequence=['red']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['yellow']).data[0])

    fig_third.update_layout(autosize=False, hovermode='closest', width=300, height=300, margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    return render(request, 'map_show3.html', {'EF_mymap1': plot_div1, 'EF_mymap2': plot_div2, 'EF_mymap3': plot_div3})


