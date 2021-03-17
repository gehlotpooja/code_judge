# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.http import HttpResponse
#from django.shortcuts import render
from rest_framework.response import Response
from rest_framework import status
import requests
from datetime import datetime,timedelta
from rest_framework.decorators import api_view
# Create your views here.
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

def index(requests):
    return HttpResponse("Hello, world. You're at Rest.")


def download_file(url):
    try:
        response_obj = requests.get(url)
    except Exception as e:
        print(e.args)
    return (url, response_obj.text)


def get_urls(url_list,processing_count):
    url_count_dic = {}
    url_response_dic = {}
    if processing_count>2:
        threads = []
        with ThreadPoolExecutor(max_workers=processing_count) as executor:
            for url in url_list:
                if url in url_count_dic:
                    url_count_dic[url] +=1
                else:
                    url_count_dic[url]=1
                    threads.append(executor.submit(download_file, url))

        for task in as_completed(threads):
            url_response_dic[task.result()[0]]=task.result()[1]
    else:
        for url in url_list:
            if url in url_count_dic:
                url_count_dic[url] += 1
            else:
                url_count_dic[url] = 1
                result = download_file(url)
                url_response_dic[result[0]] = result[1]

    return url_count_dic,url_response_dic

def get_time(timestamp_in_milli_sec,flag):
    response_obj_list = timestamp_in_milli_sec.split()
    start_time_interval_obj1 = datetime.min + timedelta(milliseconds=int(response_obj_list[1]))
    nearest_15 = start_time_interval_obj1.minute % 15
    if flag=='ST':
        hour=12
    elif flag=='NST':
        hour = 0
    start_time = start_time_interval_obj1 - timedelta(minutes=nearest_15,
                                                      seconds=start_time_interval_obj1.second) + timedelta(hours=hour)

    return start_time

def create_intervals(start_time_interval_obj1,end_time_interval_obj1):
    # import pdb
    # pdb.set_trace()
    response_list = []
    nearest_15_start = start_time_interval_obj1.minute % 15
    nearest_15_end = end_time_interval_obj1.minute % 15
    start = start_time_interval_obj1 - timedelta(minutes=nearest_15_start, seconds=start_time_interval_obj1.second,
                                                 microseconds=start_time_interval_obj1.microsecond)
    end = end_time_interval_obj1 - timedelta(minutes=nearest_15_end, seconds=end_time_interval_obj1.second)
    while start<end:
        time_interval=[start,start+timedelta(minutes=15)]
        # time_interval = start.strftime('%H:%M-') +(start+timedelta(minutes=15)).strftime('%H:%M')
        val = {"timestamp": time_interval, "logs": []}

        response_list.append(val)
        start = start+timedelta(minutes=15)
    return response_list


def get_start_and_end_time(url_response_dic):
    start_time = 0
    end_time = 0
    for k, v in url_response_dic.items():

        url_response_dic[k] = []
        for i in v.split('\r\n'):
            temp = i.split()
            url_response_dic[k].append([datetime.min + timedelta(milliseconds=int(temp[1])), temp[2]])
        if start_time == 0:
            start_time = url_response_dic[k][0][0]
            end_time = url_response_dic[k][-1][0]
        else:
            start_time_obj = url_response_dic[k][0][0]
            end_time_obj = url_response_dic[k][-1][0]
            if start_time_obj < start_time:
                start_time = start_time_obj
            if end_time_obj > end_time:
                end_time = end_time_obj
    return start_time,end_time,url_response_dic

@api_view(['POST'])
def get_process(request):
    try:
        start=time()
        data_list=[]
        data_obj = request.data
        list_of_logfiles = data_obj.get("logFiles")
        processing_count = data_obj.get("parallelFileProcessingCount")
        if list_of_logfiles and processing_count>0:
            url_count_dic,url_response_dic=get_urls(list_of_logfiles,processing_count)

            start_time,end_time,url_response_dic= get_start_and_end_time(url_response_dic)
            response_list=create_intervals(start_time,end_time)

            for k,v in url_response_dic.items():
                # import pdb
                # pdb.set_trace()
                timestamp_ind = 0
                url_text_repetition = url_count_dic[k]
                for x in v:
                    flag = False
                    while not flag:
                        # import pdb
                        # pdb.set_trace()
                        response_obj_timestamp = response_list[timestamp_ind]
                        if (x[0]>response_obj_timestamp["timestamp"][0]) and (x[0]<response_obj_timestamp["timestamp"][1]):
                            exception_value = next((item for item in response_obj_timestamp["logs"] if
                                                    item["exception"] == x[1]), False)
                            if exception_value:
                                exception_value["count"] += url_text_repetition

                            else:
                                exception_value = {"exception": x[1], "count": url_text_repetition}
                                response_obj_timestamp["logs"].append(exception_value)
                            flag = True
                        else:
                            timestamp_ind+=1

            data_list.append({"response": []})
            # print("data list #### ",data_list[0]["response"])
            for z in range(len(response_list)):
                # import pdb
                # pdb.set_trace()
                temp_list = response_list[z]
                if len(temp_list["logs"]) > 0:
                    response_list[z]["timestamp"]=temp_list["timestamp"][0].strftime("%H:%M-")+\
                                                  temp_list["timestamp"][1].strftime("%H:%M")
                    temp_list["logs"] = sorted(temp_list["logs"], key=lambda x: x["exception"])
                    data_list[0]["response"].append(temp_list)
            print(f'Time taken: {time() - start}')

            return Response(data=data_list[0], status=status.HTTP_200_OK)
        return Response(data={"status": "failure", "reason":
            "Parallel File Processing count must be greater than zero!"}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        print(e.args)
