#!/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Author : Zhilong Zheng 
# @Email : zhengzl0715@163.com
# @Date : 2017-12-28 21:35
# @Description : 

import sys
import os
from reducer_thread import ReducerThread

def get_files_path(dir_name, with_decompress):
    paths = []
    for root, dir, files in os.walk(dir_name):
        for file_name in files:
            if file_name.split('.')[0][0:5] == '10000': # 只要1000开头的文件
                if with_decompress:
                    if 'bz2' in file_name:
                        path = dir_name + file_name
                        paths.append(path)
                else:
                    if 'bz2' not in file_name:
                        path = dir_name + file_name
                        paths.append(path)

    return paths

def finish_reducing(thread_id, flows):
    print("Reducer %d completed" % thread_id)
    for flow in flows:
        print(flow)

def main(dir_name, with_decompress):
    print('Magic Starts........')
    files_path = get_files_path(dir_name, with_decompress)
    reducer_threads = []
    thread_id = 0
    # map
    for path in files_path:
        reducer = ReducerThread(path, with_decompress, thread_id, finish_reducing)
        reducer_threads.append(reducer)
    for i in range(0, len(reducer_threads)):
        print("Reducer %d starts" % i)
        reducer_threads[i].start()
    for i in range(0, len(reducer_threads)):
        print("Reducer %d ends" % i)
        reducer_threads[i].join()

if __name__ == '__main__':
    with_decompress = False
    if len(sys.argv) == 1:
        with_decompress = False
    elif len(sys.argv) == 2 and sys.argv[1] == '-d':
        with_decompress = True
    else:
        print('Bad input!!!')
    dir_name = 'data/'
    main(dir_name, with_decompress)