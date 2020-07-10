# -*- encoding:utf8 -*-

# '''
# 目标：获取某个时间区间内的所有「事件」所对应的「事件概述」与「事件影响」。
# step1、获取某个时间区间内的所有 cluster（事件） 的信息（包括 cluster_id、title、publish_time、keywords等）
# step2、利用 step1 结果中的 cluster_id 获取 每个簇内的所有特征（“事件概述”、“事件影响”）
# step3、根据需求，组合 step1 和 step2 的数据，进行适当变换，按照一定格式存储到文件中。
# '''
import io
import os
import json
import requests
import logging.config
import codecs
import time_utils
import hashlib


# step1
def get_cluster_info_from_api(start_time, end_time):
    cluster_ids = []
    cluster_infos = []
    url = "http://information-doc-service:31001/cluster/search"

    totalCount = 0

    try:
        cp = 1
        while True:
            params = dict()
            params["cp"] = cp
            params["ps"] = 50
            params["clusterTypes"] = ['热点事件']
            params["delFlag"] = 0
            params["timeField"] = 'publishAt'
            params["startAt"] = start_time
            params["endAt"] = end_time

            r = requests.get(url, params)
            result_json = r.json()

            # print(result_json)
            # break
            totalCount = result_json['data']['totalCount']
            if len(result_json['data']['list']) == 0:
                break
            for item in result_json['data']['list']:
                cluster_id = item['id']  # 簇的id
                keywords = item['keywords']  # 簇的关键词（由簇内的所有资讯标题统计出的）
                createAt = item['createAt']  # 簇的创建时间
                publishAt = item['publishAt']  # 簇（事件）内最新（近）的一篇资讯的发布时间
                title = item['title']  # 簇（事件）的标题 目前为簇内某（按相关程度）篇资讯的一个标题
                hot = item['hot']  # 簇内的资讯数量 即热度

                cluster_ids.append(cluster_id)
                cluster_infos.append({'cluster_id': cluster_id,
                                      'keywords': keywords,
                                      'createAt': createAt,
                                      'publishAt': publishAt,
                                      'title': title,
                                      'hot': hot})
            cp += 1
    except Exception as e:
        # logger.exception("Exception: {}".format(e))
        print("Exception: {}".format(e))
    # logger.info("get_cluster_info_from_api count: {}".format(totalCount))
    print("get_cluster_info_from_api count: {}".format(totalCount))
    return cluster_ids, cluster_infos


# step2
def get_cluster_infoids_feature_from_api(clusterIds):
    url = 'http://index-information-service:31001/information/relation/search'

    totalCount = 0

    # 由于 clusterIds 可能很多，分批取
    batch_size = 50
    epoch = int(len(clusterIds) / batch_size) + 1
    begin = 0

    # 遍历取数据
    for i in range(0, epoch):

        clusterIds_ = clusterIds[begin: begin + batch_size]
        begin += batch_size

        try:
            cp = 1
            while True:
                params = dict()
                params["cp"] = cp
                params["ps"] = 50
                params["clusterIds"] = clusterIds_
                params["delFlag"] = 0
                params["relationTypes"] = "事件概述,事件影响"
                params["human"] = 0
                # params["startAt"] = start_time
                # params["endAt"] = end_time
                params["relationMethods"] = 'CLUSTER'

                r = requests.get(url, params)
                result_json = r.json()

                # print(result_json)
                # break
                totalCount = result_json['data']['totalCount']
                if len(result_json['data']['list']) == 0:
                    break
                for item in result_json['data']['list']:
                    # contentId
                    # id
                    # editAt
                    # mediaFrom
                    # 'relationType': ['事件影响']
                    # title
                    cluster_id = item['clusterId']
                    content = item['content']
                    createAt = item['createAt']
                    publishAt = item['publishAt']
                    relationType = item['relationType']
                    # if '事件影响' in relationType:
                    #   print(content)
                    #  print(relationType)
                cp += 1
        except Exception as e:
            # logger.exception("Exception: {}".format(e))
            print("Exception: {}".format(e))
    # logger.info("get_cluster_infoids_feature_from_api count: {}".format(totalCount))
    print("get_cluster_infoids_feature_from_api count: {}".format(totalCount))


if __name__ == '__main__':
    start_time = 1547395200000  # 1.14.00
    end_time = start_time + int((1 * 24 * 60 * 60 * 1000))

    cluster_ids, cluster_infos = get_cluster_info_from_api(start_time, end_time)
    get_cluster_infoids_feature_from_api(cluster_ids)










