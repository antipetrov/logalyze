#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import unittest
from datetime import date
import time
from log_analyzer import *


class MainCase(unittest.TestCase):
    
    def test_target_files(self):
        target_files = get_target_files(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],0)
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 2)
        print(target_files)

    def test_target_files_filter(self):
        target_files = get_target_files(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],int(time.mktime(date(2017,01,01).timetuple())))
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 1)

    def test_file_order(self):
        pass

    def test_line_parse(self):
        line = '1.138.198.128 -  - [30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1" 200 1261 "-" "python-requests/2.8.1" "-" "1498782502-440360380-4707-10488740" "4e9627334" 0.863'
        pattern = '([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* ([0-9.]+)$'
        m = re.match(pattern, line)
        print(m.group(5))
        print(m.group(6))
        print(m.group(7))






if __name__ == '__main__':
    unittest.main()
