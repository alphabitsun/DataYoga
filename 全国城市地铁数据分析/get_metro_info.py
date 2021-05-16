# encoding:utf-8
# Date:     2021/4/14 19:32
# Description:
import json
import random
import time

import pandas as pd
import numpy as np
import warnings

import requests
from bs4 import BeautifulSoup

from craw_tools.get_ua import get_ua

warnings.filterwarnings('ignore')

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


def get_city_list():
    """
    获取拥有地铁的所有城市
    @return:
    """
    url = 'http://map.amap.com/subway/index.html'
    res = requests.get(url, headers={'User-Agent': get_ua()})
    res.encoding = res.apparent_encoding
    soup = BeautifulSoup(res.text, 'html.parser')

    name_dict = []
    # 获取显示出的城市列表
    for soup_a in soup.find('div', class_='city-list fl').find_all('a'):
        city_name_py = soup_a['cityname']
        city_id = soup_a['id']
        city_name_ch = soup_a.get_text()
        name_dict.append({'name_py': city_name_py, 'id': city_id, 'name_ch': city_name_ch})
    # 获取未显示出来的城市列表
    for soup_a in soup.find('div', class_='more-city-list').find_all('a'):
        city_name_py = soup_a['cityname']
        city_id = soup_a['id']
        city_name_ch = soup_a.get_text()
        name_dict.append({'name_py': city_name_py, 'id': city_id, 'name_ch': city_name_ch})

    df_name = pd.DataFrame(name_dict)

    return df_name


def get_metro_info(id, cityname, name):
    """
    地铁线路信息获取
    """
    url = "http://map.amap.com/service/subway?_1618387860087&srhdata=" + id + '_drw_' + cityname + '.json'
    res = requests.get(url, headers={'User-Agent': get_ua()})
    data = json.loads(res.text)

    df_data_city = pd.DataFrame()
    if data['l']:
        # 遍历每一条地铁线路
        for data_line in data['l']:
            df_per_zd = pd.DataFrame(data_line['st'])
            df_per_zd = df_per_zd[['n', 'sl', 'poiid', 'sp']]
            df_per_zd['gd经度'] = df_per_zd['sl'].apply(lambda x: x.split(',')[0])
            df_per_zd['gd纬度'] = df_per_zd['sl'].apply(lambda x: x.split(',')[1])
            df_per_zd.drop('sl', axis=1, inplace=True)
            df_per_zd['路线名称'] = data_line['ln']
            df_per_zd['城市名称'] = name

            df_per_zd.rename(columns={'n': '站点名称', 'sp': '拼音名称', 'poiid': 'POI编号'}, inplace=True)
            df_data_city = df_data_city.append(df_per_zd, ignore_index=True)

    return df_data_city


if __name__ == '__main__':
    df_city = pd.DataFrame()
    """获取有地铁站点的城市名"""
    df_name = get_city_list()
    print(df_name.head(5))
    for row_index, data_row in df_name.iterrows():
        print('正在爬取第 {0}/{1} 个城市 {2} 的数据中...'.format(row_index + 1, len(df_name), data_row['name_ch']))
        """遍历每个城市获取地铁站点信息"""
        df_per_city = get_metro_info(data_row['id'], data_row['name_py'], data_row['name_ch'])
        df_city = df_city.append(df_per_city, ignore_index=True)
        # 爬虫休眠
        time.sleep(random.randint(3, 5))

    # print(df_city)
    df_city.to_csv(r'全国城市地铁站点信息V202104.csv', encoding='gbk', index=False)