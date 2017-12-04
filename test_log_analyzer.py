#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import unittest
import os
import time
from datetime import datetime, date

from logger import log
from log_analyzer import *

class LogProcessorCase(unittest.TestCase):
    
    def test_last_file(self):
        target_files = get_last_file(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],datetime(year=2000, month=1, day=1), 'nginx-access-ui.log-(\d+).(gz|log)')
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 2)



    def test_line_parse(self):
        line = '1.138.198.128 -  - [30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1" 200 1261 "-" "python-requests/2.8.1" "-" "1498782502-440360380-4707-10488740" "4e9627334" 0.863'
        parsed_url, parsed_time = parse_log_line(line)
        self.assertEqual(parsed_url, '/api/v2/banner/25949683')
        self.assertEqual(parsed_time, '0.863')

        # bad line
        line = '[30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1"'
        parsed_url, parsed_time = parse_log_line(line)
        self.assertRaises(Exception)
    

    def test_stat(self):
        stat = [{'count': 1, 'time_avg': 1.631, 'time_max': 0.0, 'time_sum': 1.631, 'url': '/banners/26362895/switch_status/?status=delete&_=1498748952071', 'time_med': 1.631, 'time_perc': 8.461522660694406e-07, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 0.046, 'time_max': 0.0, 'time_sum': 0.046, 'url': '/accounts/login/?next=/agency/campaigns/%3Fsearch%3D%25D1%2581%25D0%25BE%25D1%2582%25D0%25B0%26activity%3Dactive', 'time_med': 0.046, 'time_perc': 2.386450290569851e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 13, 'time_avg': 0.043461538461538454, 'time_max': 0.0, 'time_sum': 0.565, 'url': '/api/v2/banner/26751035/statistic/?date_from=2017-06-29&date_to=2017-06-29', 'time_med': 0.043, 'time_perc': 2.9311835090694906e-07, 'count_perc': 4.973818584175758e-06}, {'count': 1, 'time_avg': 0.072, 'time_max': 0.0, 'time_sum': 0.072, 'url': '/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28', 'time_med': 0.072, 'time_perc': 3.7353134982832446e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 2, 'time_avg': 0.5065, 'time_max': 0.0, 'time_sum': 1.013, 'url': '/api/v2/banner/17096340/', 'time_med': 0.5065, 'time_perc': 5.255378574667954e-07, 'count_perc': 7.652028591039628e-07}, {'count': 1, 'time_avg': 0.076, 'time_max': 0.0, 'time_sum': 0.076, 'url': '/api/v2/internal/banner/24324264/info', 'time_med': 0.076, 'time_perc': 3.942830914854536e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 3.724, 'time_max': 0.0, 'time_sum': 3.724, 'url': '/ads/campaigns/7863032/gpmd/event_statistic/?date1=29-06-2017&date2=29-06-2017&date_type=day&puid1=&puid2=&puid3=', 'time_med': 3.724, 'time_perc': 1.931987148278723e-06, 'count_perc': 3.826014295519814e-07}]


        import operator
        stat.sort(key=operator.itemgetter('time_avg'), reverse=True)
        

    def test_report_render(self):
        processor = LogProcessor()
        stat = [{'count': 1, 'time_avg': 1.631, 'time_max': 0.0, 'time_sum': 1.631, 'url': '/banners/26362895/switch_status/?status=delete&_=1498748952071', 'time_med': 1.631, 'time_perc': 8.461522660694406e-07, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 0.046, 'time_max': 0.0, 'time_sum': 0.046, 'url': '/accounts/login/?next=/agency/campaigns/%3Fsearch%3D%25D1%2581%25D0%25BE%25D1%2582%25D0%25B0%26activity%3Dactive', 'time_med': 0.046, 'time_perc': 2.386450290569851e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 13, 'time_avg': 0.043461538461538454, 'time_max': 0.0, 'time_sum': 0.565, 'url': '/api/v2/banner/26751035/statistic/?date_from=2017-06-29&date_to=2017-06-29', 'time_med': 0.043, 'time_perc': 2.9311835090694906e-07, 'count_perc': 4.973818584175758e-06}, {'count': 1, 'time_avg': 0.072, 'time_max': 0.0, 'time_sum': 0.072, 'url': '/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28', 'time_med': 0.072, 'time_perc': 3.7353134982832446e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 2, 'time_avg': 0.5065, 'time_max': 0.0, 'time_sum': 1.013, 'url': '/api/v2/banner/17096340/', 'time_med': 0.5065, 'time_perc': 5.255378574667954e-07, 'count_perc': 7.652028591039628e-07}, {'count': 1, 'time_avg': 0.076, 'time_max': 0.0, 'time_sum': 0.076, 'url': '/api/v2/internal/banner/24324264/info', 'time_med': 0.076, 'time_perc': 3.942830914854536e-08, 'count_perc': 3.826014295519814e-07}, 
                {'count': 1, 'time_avg': 3.724, 'time_max': 0.0, 'time_sum': 3.724, 'url': '/ads/campaigns/7863032/gpmd/event_statistic/?date1=29-06-2017&date2=29-06-2017&date_type=day&puid1=&puid2=&puid3=', 'time_med': 3.724, 'time_perc': 1.931987148278723e-06, 'count_perc': 3.826014295519814e-07}]
  
        report = processor.render_report(stat, datetime.today())
        pos = report.find('/banners/26362895/switch_status/?status=delete&_=1498748952071')
        self.assertNotEqual(pos, -1)

    def test_ts_file_save(self):

        ts_filename = '.test.ts'

        start_time = datetime.now()
        start_timestamp = int(time.mktime(start_time.timetuple()))

        save_last_processed(ts_filename, start_time)

        with open(ts_filename) as ts_loaded:
            ts_loaded = fp.readline().strip()

        os.remove(ts_filename)

        loaded_timestamp = int(ts_loaded)

        self.assertEqual(start_timestamp, loaded_timestamp)

    def test_ts_file_load(self):
        ts_filename = '.test_load.ts'
        processor = LogProcessor()
        time_now = datetime.now()
        processor.save_last_processed(ts_filename, time_now)
        loaded_time = processor.load_last_processed(ts_filename)

        self.assertEqual(time_now, time_now)

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

        processor = LogProcessor(config)
        processor.process()



if __name__ == '__main__':
    unittest.main()
