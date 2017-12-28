#!/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Author : Zhilong Zheng 
# @Email : zhengzl0715@163.com
# @Date : 2017-12-28 22:03
# @Description :

import queue
import threading
import datetime
import json
import os

class ReducerThread(threading.Thread):
    def __init__(self, file_path, with_decompress, thread_id, finish_reducing):
        super().__init__()
        self.file_path = file_path
        self.with_decompress = with_decompress
        self.thread_id = thread_id
        self.finish_reducing = finish_reducing

    def run(self):
        self.preprocess()

        print("Reducer %d starts processing........" % self.thread_id)
        flows = {}
        for line in self.lines:
            split_str = line.split('\t')
            timestamp = int(split_str[0])
            src_ip = split_str[2]
            dst_ip = split_str[3]
            src_port = split_str[4]
            dst_port = split_str[5]
            protocol = split_str[6]
            five_tuple = src_ip + ',' + dst_ip + ',' + src_port + ',' + dst_port + ',' + protocol
            if five_tuple not in flows:
                flows[five_tuple] = []
            flows[five_tuple].append(timestamp)

        concatenated_flows = []
        for (k, v) in flows.items():
            #如果只有一条记录，FIN = SYN+2
            if len(v) == 1:
                property = {"flow_key": k, "syn_timestamp": v[0], "mid_timestamp": v[0] + 1,
                            "fin_timestamp": v[0] + 2}
            else:
                _min = min(v)
                _max = max(v)
                _mid = int((_min + _max) / 2)
                property = {"flow_key": k, "syn_timestamp": _min, "mid_timestamp": _mid,
                            "fin_timestamp": _max}
            concatenated_flows.append(property)

        self.postprocess(concatenated_flows)


    def postprocess(self, concatenated_flows):
        # 按syn_timestamp排序
        concatenated_flows.sort(key=lambda flow: (flow.get('syn_timestamp', 0)))
        # 写到一个单独的文件
        out_filename = 'out_data/'
        if not os.path.exists(out_filename):
            os.mkdir(out_filename)
        split_str = self.file_name.split('/')
        out_filename = out_filename + split_str[len(split_str)-1] + '_json.out'
        json_file = open(out_filename, 'w')
        # for flow in concatenated_flows:
        _json = json.dumps(concatenated_flows)
        json_file.write(_json)
        json_file.close()

        self.finish_reducing(self.thread_id, concatenated_flows)

    def preprocess(self):
        if self.with_decompress:  #如果需要解压
            print("Decompress bz2")
            cmd = 'bzip2 -d ' + self.file_path
            os.system(cmd)
            self.file_name = self.file_path[0:len(self.file_path)-4]
        else:
            self.file_name = self.file_path

        with open(self.file_name, 'r') as file:
            self.lines = file.readlines()


