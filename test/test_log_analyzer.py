#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';



import unittest
import sys
import os
import time
from datetime import datetime, date, time


# sys.path.insert(0,'..')
sys.path.insert(0,(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from log_analyzer import *

class LogAnalyzerCase(unittest.TestCase):
    
    def test_last_file(self):
        target_file, filedate = get_last_log(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],datetime(year=2000, month=1, day=1), 'nginx-access-ui.log-(\d+).(gz|log)')

        self.assertEqual(target_file, 'nginx-access-ui.log-20170102.gz')
        self.assertEqual(filedate.year, 2017)
        self.assertEqual(filedate.month, 01)
        self.assertEqual(filedate.day, 02)


    def test_line_parse(self):
        line = '1.138.198.128 -  - [30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1" 200 1261 "-" "python-requests/2.8.1" "-" "1498782502-440360380-4707-10488740" "4e9627334" 0.863'
        parsed = parse_log_line(line)
        print(parsed)
        self.assertEqual(parsed[0], '/api/v2/banner/25949683')
        self.assertEqual(parsed[1], '0.863')
        # self.assertIsNone(err)


        # bad line
        line = '[30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1"'
        with self.assertRaises(Exception):
            parsed_line = parse_log_line(line)

    def test_stat(self):
        stat = [{'count': 1, 'time_avg': 1.631, 'time_max': 0.0, 'time_sum': 1.631, 'url': '/banners/26362895/switch_status/?status=delete&_=1498748952071', 'time_med': 1.631, 'time_perc': 8.461522660694406e-07, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 0.046, 'time_max': 0.0, 'time_sum': 0.046, 'url': '/accounts/login/?next=/agency/campaigns/%3Fsearch%3D%25D1%2581%25D0%25BE%25D1%2582%25D0%25B0%26activity%3Dactive', 'time_med': 0.046, 'time_perc': 2.386450290569851e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 13, 'time_avg': 0.043461538461538454, 'time_max': 0.0, 'time_sum': 0.565, 'url': '/api/v2/banner/26751035/statistic/?date_from=2017-06-29&date_to=2017-06-29', 'time_med': 0.043, 'time_perc': 2.9311835090694906e-07, 'count_perc': 4.973818584175758e-06}, {'count': 1, 'time_avg': 0.072, 'time_max': 0.0, 'time_sum': 0.072, 'url': '/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28', 'time_med': 0.072, 'time_perc': 3.7353134982832446e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 2, 'time_avg': 0.5065, 'time_max': 0.0, 'time_sum': 1.013, 'url': '/api/v2/banner/17096340/', 'time_med': 0.5065, 'time_perc': 5.255378574667954e-07, 'count_perc': 7.652028591039628e-07}, {'count': 1, 'time_avg': 0.076, 'time_max': 0.0, 'time_sum': 0.076, 'url': '/api/v2/internal/banner/24324264/info', 'time_med': 0.076, 'time_perc': 3.942830914854536e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 3.724, 'time_max': 0.0, 'time_sum': 3.724, 'url': '/ads/campaigns/7863032/gpmd/event_statistic/?date1=29-06-2017&date2=29-06-2017&date_type=day&puid1=&puid2=&puid3=', 'time_med': 3.724, 'time_perc': 1.931987148278723e-06, 'count_perc': 3.826014295519814e-07}]


        import operator
        stat.sort(key=operator.itemgetter('time_avg'), reverse=True)
        

    def test_report_render(self):
        stat = [{'count': 1, 'time_avg': 1.631, 'time_max': 0.0, 'time_sum': 1.631, 'url': '/banners/26362895/switch_status/?status=delete&_=1498748952071', 'time_med': 1.631, 'time_perc': 8.461522660694406e-07, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 0.046, 'time_max': 0.0, 'time_sum': 0.046, 'url': '/accounts/login/?next=/agency/campaigns/%3Fsearch%3D%25D1%2581%25D0%25BE%25D1%2582%25D0%25B0%26activity%3Dactive', 'time_med': 0.046, 'time_perc': 2.386450290569851e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 13, 'time_avg': 0.043461538461538454, 'time_max': 0.0, 'time_sum': 0.565, 'url': '/api/v2/banner/26751035/statistic/?date_from=2017-06-29&date_to=2017-06-29', 'time_med': 0.043, 'time_perc': 2.9311835090694906e-07, 'count_perc': 4.973818584175758e-06}, {'count': 1, 'time_avg': 0.072, 'time_max': 0.0, 'time_sum': 0.072, 'url': '/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28', 'time_med': 0.072, 'time_perc': 3.7353134982832446e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 2, 'time_avg': 0.5065, 'time_max': 0.0, 'time_sum': 1.013, 'url': '/api/v2/banner/17096340/', 'time_med': 0.5065, 'time_perc': 5.255378574667954e-07, 'count_perc': 7.652028591039628e-07}, {'count': 1, 'time_avg': 0.076, 'time_max': 0.0, 'time_sum': 0.076, 'url': '/api/v2/internal/banner/24324264/info', 'time_med': 0.076, 'time_perc': 3.942830914854536e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 3.724, 'time_max': 0.0, 'time_sum': 3.724, 'url': '/ads/campaigns/7863032/gpmd/event_statistic/?date1=29-06-2017&date2=29-06-2017&date_type=day&puid1=&puid2=&puid3=', 'time_med': 3.724, 'time_perc': 1.931987148278723e-06, 'count_perc': 3.826014295519814e-07}]
  
        report_str = render_report(stat, './report.html')
        pos = report_str.find('/banners/26362895/switch_status/?status=delete&_=1498748952071')
        self.assertNotEqual(pos, -1)

    def test_last_processed(self):
        ts_filename = './test_last_processed.ts'
        time_now = datetime.now()
        updated = update_last_processed(ts_filename, time_now)
        self.assertTrue(updated)

        loaded_time = load_last_processed(ts_filename)
        self.assertEqual(time_now, time_now)
        os.remove(ts_filename)

    def test_full_process(self):
        config = {
            "REPORT_SIZE": 1000,
            "REPORT_DIR": "./test/reports",
            "REPORT_TEMPLATE": "./report.html",
            "PROCESS_LOG": "./test/test.log",
            "TS_FILE": "./test/test.ts",
            "LOG_DIR": "./test/log",
            "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d+).(gz|log)",
            "LAST_PROCESSED_FILE": "./test/last_processed.ts",
        }

        try:
            os.remove(config['LAST_PROCESSED_FILE'])
        except OSError as e:
            pass

        # run full process
        process(config)

        with open(config['TS_FILE']) as ts_file:
            tstamp = int(ts_file.readline())

        mtime = os.path.getmtime(config['TS_FILE'])

        self.assertEqual(mtime, tstamp)

if __name__ == '__main__':
    unittest.main()
