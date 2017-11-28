#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import re
from logger import log
import json
from time import time
from datetime import datetime
from decimal import Decimal


def median(lst):
    n = len(lst)
    if n < 1:
        return None
    if n % 2 == 1:
        return sorted(lst)[n//2]
    else:
        return sum(sorted(lst)[n//2-1:n//2+1])/2.0


class LogProcessor(object):

    default_config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "REPORT_TEMPLATE": "./report.html",
        "PROCESS_LOG": "./log_analyzer.log",
        "TS_FILE": "./logalize.ts",
        "LOG_DIR": "./log",
        "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d+).(gz|log)"
    }


    def __init__(self, config=None):
        if not config:
            self.config = LogProcessor.default_config
        else:
            self.config = config

    def parse_log_line(self, line):
        """
        process single log line

        return tuple(url, response_time)
        """
        pattern = '([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* ([0-9.]+)$'
        try:
            log_match = re.match(pattern, line)
        except Exception as e:
            log.error("error parsing line:'%s' message:%s", line, e.message)
            raise    

        try:
            result = (log_match.group(6), log_match.group(7))
        except Exception as e:
            log.error("error parsing line (no groups):'%s' message:%s", line, e.message)
            raise

        return result


    def process_log_file(self, filename):
        """
        process logfile
        """

        log.info("Start processing %s", filename)

        # get date part of file
        try:
            fname_match = re.match(self.config['LOG_FILE_PATTERN'], filename)
            date_processed = datetime.strptime(fname_match.group(1), "%Y%m%d")

        except Exception as e:
            log.error('Could not parse log date %s for with pattern %s. Error: %s', filename, self.config['LOG_FILE_PATTERN'], e.message)
            return None, None

        filepath = os.path.join(self.config['LOG_DIR'], filename)

        # open plain or gzip
        import gzip
        try:
            if filename.endswith('.gz'):
                fp = gzip.open(filepath)
            else:
                fp = open(filepath)
        except Exception as e:
            log.error('Unable to open file %s', filepath)
            return None, None

        # init values
        stat = {}

        total_count = 0
        total_time = 0.0
        line_num = 0

        # go parsing
        for line in fp:
            line_num += 1
            try:
                uri, rtime = self.parse_log_line(line.rstrip())
            except Exception as e:
                log.info("skipped line  %d", line_num)
                continue

            responce_time = float(rtime)
            
            current_stat = stat.get(uri, {'count':0, 'time_sum': 0.0, 'time_max':0.0, 'time_list':[]})
        
            current_stat['count'] = current_stat['count'] + 1
            current_stat['time_sum'] = current_stat['time_sum'] + responce_time
            current_stat['time_max'] = current_stat['time_max'] if responce_time <= current_stat['time_sum'] else responce_time
            current_stat['time_list'].append(responce_time)
            
            stat[uri] = current_stat
            
            total_count += 1
            total_time += responce_time

            # log progress
            if line_num % 100000 == 0:
                log.info("%d lines processed", line_num)

        fp.close()

        # pass 2 - calculate aggregates & convert
        log.info("Calculating aggregates on total %d lines",line_num)

        stat_list = []
        for url,  data in stat.iteritems():
            stat_list.append({
                'url':url,
                'count':data['count'],
                'time_max':data['time_max'],
                'time_sum':data['time_sum'],
                'time_avg':data['time_sum']/data['count'],
                'time_med':median(data['time_list']), 
                'time_perc':data['time_sum']/total_time,
                'count_perc':float(data['count'])/total_count,
                })

        # sort it
        import operator
        stat_list.sort(key=operator.itemgetter('time_avg'), reverse=True)

        max_lines = self.config.get('REPORT_SIZE', 1000)

        return stat_list[:int(max_lines)], date_processed
            

    def render_report(self, stat_list, processed_date):
        """
        Render stat into html file.
        Uses REPORT_TEMPLATE from config as page-template

        :param stat - stat dict, where keys - uri`s
        :param processed_date - inital date - used in resulting filename

        result row: {"count": 2767, "time_avg": 62.994999999999997, "time_max": 9843.5689999999995, 
                     "time_sum": 174306.35200000001, "url": "/api/v2/internal/html5/phantomjs/queue/?wait=1m", 
                     "time_med": 60.073, "time_perc": 9.0429999999999993, "count_perc": 0.106}
        """

        # re-format stat to list
        stat_rows = []
        for data in stat_list:
            stat_rows.append({
                'url':data['url'],
                'count':data['count'],
                'time_avg':'{0:.10f}'.format(data['time_avg']),
                'time_max':'{0:.10f}'.format(data['time_max']),
                'time_sum':'{0:.10f}'.format(data['time_sum']),
                'time_med':'{0:.10f}'.format(data['time_med']),
                'time_perc':'{0:.10f}'.format(data['time_perc']),
                'count_perc':'{0:.10f}'.format(data['count_perc']),
                })

        stat_json = json.dumps(stat_rows)

        report_template_filename = self.config.get("REPORT_TEMPLATE", None)    
        
        try:
            ftemp = open(report_template_filename, "r")
            template = "".join(ftemp.readlines())
        except Exception as e:
            log.error('Failed to read report template file "%s": %s', (report_template_filename, e.message))
            return None

        report_str = template.replace('$table_json', stat_json)
        ftemp.close()

        try:
            report_filename = os.path.join(self.config['REPORT_DIR'], "report_%s.html"%datetime.strftime(processed_date, "%Y.%m.%d"))
            frep=open(report_filename, 'w')
            frep.write(report_str)
            frep.close()
        except Exception as e:
            log.error('Failed to write report file: %s', e.message)
            return None

        return report_str    


    def get_target_files(self, file_list, from_datetime):
        """
        Get files with date-part of in filename greater than date of from_timestamp
        Timezones are believed to match 
        """
        target_files = []

        start_date_mark = from_datetime.strftime("%Y%m%d")
        
        for filename in file_list:
            fname_match = re.match(self.config['LOG_FILE_PATTERN'], filename)
            if fname_match:
                fname_date_part = fname_match.group(1)
                if int(fname_date_part) > int(start_date_mark):
                    target_files.append(filename)

        sorted(target_files, reverse=True)
        return target_files

    def load_last_processed(self, ts_file_path):
        try:
            ts_file = open(ts_file_path)
            return self.timestamp_to_datetime(int(ts_file.readline()))
        except Exception as e:
            log.error('Unable to load last processed timestamp. Using 0')
            return self.timestamp_to_datetime(0)

    def save_last_processed(self, ts_filename, processed_date):
        from time import time
        try: 
            fp = open(ts_filename, 'w')
            fp.write(str(self.datetime_to_timestamp(processed_date)))
            fp.close()
        except Exception as e:
            log.error('Unable to write processing timestamp. Error: %s', e.message)
            return False

        return True

    def datetime_to_timestamp(self, dt):
        return time.mktime(d.timetuple())

    def timestamp_to_datetime(self, ts):
        return datetime.fromtimestamp(ts)



    def process(self):
        last_processed = self.load_last_processed(self.config['TS_FILE'])
        logfile_list = os.listdir(self.config['LOG_DIR'])
        
        target_files = self.get_target_files(logfile_list, last_processed)
        if not target_files:
            log.info('No files newer than %d found', last_processed)
            print('No files newer than %d found' % last_processed)


        log.info("Target files: %s", ",".join(target_files))
        for tfilename in target_files:
            stat, processed_date = self.process_log_file(tfilename)

            if stat:
                log.info('processed %d lines', len(stat))
                report = self.render_report(stat, processed_date)
            else:
                log.info('failed to process file %s', tfilename)
                continue

            if stat and report:
                ts_filename = self.config.get('TS_FILE', './log_analyzer.ts')
                self.save_last_processed(ts_filename, datetime.now())
