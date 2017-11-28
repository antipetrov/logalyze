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


from log_processor import LogProcessor


class LogProcessorCase(unittest.TestCase):
    
    def test_target_files(self):
        processor = LogProcessor()
        target_files = processor.get_target_files(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],0)
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 2)

    def test_target_files_filter(self):
        processor = LogProcessor()

        target_files = processor.get_target_files(['nginx-access-ui.log-20170101.gz','nginx-access-ui.log-20170102.gz'],int(time.mktime(date(2017,01,01).timetuple())))
        self.assertIsInstance(target_files, list)
        self.assertEqual(len(target_files), 1)

    def test_file_order(self):
        #TODO: test order of processing
        pass

    def test_line_parse(self):
        line = '1.138.198.128 -  - [30/Jun/2017:03:28:23 +0300] "GET /api/v2/banner/25949683 HTTP/1.1" 200 1261 "-" "python-requests/2.8.1" "-" "1498782502-440360380-4707-10488740" "4e9627334" 0.863'
        processor = LogProcessor()
        parsed_url, parsed_time = processor.parse_log_line(line)
        self.assertEqual(parsed_url, '/api/v2/banner/25949683')
        self.assertEqual(parsed_time, '0.863')

    # pattern = '([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* ([0-9.]+)$'
    # m = re.match(pattern, line)
    # print(m.group(5))
    # print(m.group(6))
    # print(m.group(7))
    def test_stat(self):
        pass

    def test_report_render(self):
        processor = LogProcessor()
        
        stat = {'/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28': 
                    {'count': 1, 'time_avg': 0.072, 'time_list': [0.072], 'time_max': 0.072, 'time_sum': 0.072, 'time_med': 0.072, 'time_perc': 3.7353134982832446e-08, 'count_perc': 0}, '/api/v2/banner/17096340/': {'count': 2, 'time_avg': 0.5065, 'time_list': [0.596, 0.417], 'time_max': 1.013, 'time_sum': 1.013, 'time_med': 0.5065, 'time_perc': 5.255378574667954e-07, 'count_perc': 0}, '/api/v2/internal/banner/24324264/info': {'count': 1, 'time_avg': 0.076, 'time_list': [0.076], 'time_max': 0.076, 'time_sum': 0.076, 'time_med': 0.076, 'time_perc': 3.942830914854536e-08, 'count_perc': 0}, 
                '/ads/campaigns/7863032/gpmd/event_statistic/?date1=29-06-2017&date2=29-06-2017&date_type=day&puid1=&puid2=&puid3=': 
                    {'count': 1, 'time_avg': 3.724, 'time_list': [3.724], 'time_max': 3.724, 'time_sum': 3.724, 'time_med': 3.724, 'time_perc': 1.931987148278723e-06, 'count_perc': 0}}
        report = processor.render_report(stat, datetime.today())
        pos = report.find('/api/v2/banner/25918447/statistic/outgoings/?date_from=2017-06-28&date_to=2017-06-28')
        self.assertNotEqual(pos, -1)

    def test_ts_file_save(self):

        ts_filename = '.test.ts'

        processor = LogProcessor()
        processor.save_last_processed(ts_filename)

        time_val = str(int(time.time()))
        fp = open(ts_filename)
        ts_value = fp.readline().strip()
        fp.close()

        os.remove(ts_filename)

        self.assertEqual(time_val, ts_value)

    def test_ts_file_load(self):
        ts_filename = '.test_load.ts'
        processor = LogProcessor()
        time_val = str(int(time.time()))
        processor.save_last_processed(ts_filename)
        loaded_val = processor.load_last_processed(ts_filename)

        self.assertLess(int(loaded_val) - int(time_val), 2)




if __name__ == '__main__':
    unittest.main()
