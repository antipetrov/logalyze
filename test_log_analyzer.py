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

    def test_target_files_filter(self):

        target_files = get_target_files(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],int(time.mktime(date(2017,01,01).timetuple())))
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 1)
        



if __name__ == '__main__':
    unittest.main()
