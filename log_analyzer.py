#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import os
import re
import logging
import json
from datetime import datetime
from decimal import Decimal

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "REPORT_TEMPLATE": "./report.html",
    # "PROCESS_LOG": "./log_analyzer.log"
    "TS_FILE": "./logalize.ts",
    "LOG_DIR": "./log",
    "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d+).(gz|log)"
}

def load_config():
    # TODO: add load from file
    return config

def load_last_processed(ts_file_path):
    try:
        ts_file = open(ts_file_path)
        return int(ts_file.readline())
    except Exception as e:
        return 0


def median(lst):
    n = len(lst)
    if n < 1:
        return None
    if n % 2 == 1:
        return sorted(lst)[n//2]
    else:
        return sum(sorted(lst)[n//2-1:n//2+1])/2.0

def parse_log_line(line):
    """
    process single log line

    return tuple(url, response_time)
    """
    pattern = '([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* ([0-9.]+)$'
    try:
        log_match = re.match(pattern, line)
    except Exception as e:
        logging.error("error parsing line:'%s' message:%s", line, e.message)
        raise    

    try:
        result = (log_match.group(6), float(log_match.group(7)))
    except Exception as e:
        logging.error("error parsing line (no groups):'%s' message:%s", line, e.message)
        raise

    return result


def process_log_file(filename):
    """
    process logfile
    """

    logging.info("Start processing %s", filename)

    # get date part of file
    try:
        fname_match = re.match(config['LOG_FILE_PATTERN'], filename)
        date_processed = datetime.strptime(fname_match.group(1), "%Y%m%d")

    except Exception as e:
        logging.error('Could not parse log date %s for with pattern %s. Error: %s'%(filename, config['LOG_FILE_PATTERN'], e.message))
        exit()

    filepath = os.path.join(config['LOG_DIR'], filename)

    # open plain or gzip
    import gzip
    if filename.endswith('.gz'):
        fp = gzip.open(filepath)
    else:
        fp = open(filepath)

    # init values
    stat = {}

    total_count = 0
    total_time = 0.0
    line_num = 0

    # go parsing
    for line in fp:
        line_num += 1
        try:
            uri, rtime = parse_log_line(line.rstrip())
        except Exception as e:
            logging.info("skipped line  %d", line_num)
            continue
        
        current_stat = stat.get(uri, {'count':0, 'time_sum': 0.0, 'time_max':0.0, 'time_list':[]})
    
        current_stat['count'] = current_stat['count'] + 1
        current_stat['time_sum'] = current_stat['time_sum'] + rtime
        current_stat['time_max'] = current_stat['time_max'] if rtime <= current_stat['time_sum'] else rtime
        current_stat['time_list'].append(rtime)
        
        stat[uri]['time_list'] = current_stat
        
        total_count += 1
        total_time += rtime

        # log progress
        if line_num % 100000 == 0:
            logging.info("%d lines processed, %d uri found", (line_num, len(stat.keys())))

    fp.close()

    # pass 2 - calculate aggregates
    logging.info("Calculating aggregates on total %d lines",line_num)

    for uri, data in stat.iteritems():
        stat[uri]['count_perc'] = float(data['count'])/total_count
        stat[uri]['time_perc'] = data['time_sum']/total_time
        stat[uri]['time_perc'] = data['time_sum']/total_time
        stat[uri]['time_avg'] = data['time_sum']/data['count']
        stat[uri]['time_med'] = median(data['time_list'])
        del stat[uri]['time_list']

    return stat, date_processed
        

def render_report(stat, processed_date):
    """
    result row: {"count": 2767, "time_avg": 62.994999999999997, "time_max": 9843.5689999999995, 
                 "time_sum": 174306.35200000001, "url": "/api/v2/internal/html5/phantomjs/queue/?wait=1m", 
                 "time_med": 60.073, "time_perc": 9.0429999999999993, "count_perc": 0.106}
    """

    # re-format stat to list
    stat_rows = []
    for url, data in stat.iteritems():
        stat_rows.append({
            'url':url,
            'count':data['count'],
            'time_avg':'{0:.10f}'.format(data['time_avg']),
            'time_max':'{0:.10f}'.format(data['time_max']),
            'time_sum':'{0:.10f}'.format(data['time_sum']),
            'time_med':'{0:.10f}'.format(data['time_med']),
            'time_perc':'{0:.10f}'.format(data['time_perc']),
            'count_perc':'{0:.10f}'.format(data['count_perc']),
            })

    stat_json = json.dumps(stat_rows)

    report_template_filename = config.get("REPORT_TEMPLATE", None)    
    
    try:
        ftemp = open(report_template_filename, "r")
        template = "".join(ftemp.readlines())
    except Exception as e:
        logging.error('Failed to read report template file "%s": %s', (report_template_filename, e.message))
        return None

    report_str = template.replace('$table_json', stat_json)
    ftemp.close()

    try:
        report_filename = os.path.join(config['REPORT_DIR'], "report_%s.html"%datetime.strftime(processed_date, "%Y.%m.%d"))
        frep=open(report_filename, 'w')
        frep.write(report_str)
        frep.close()
    except Exception as e:
        logging.error('Failed to write report file: %s', e.message)
        return None

    return report_str    


def get_target_files(file_list, from_timestamp):
    """
    Get files with date-part of in filename greater than date of from_timestamp
    Timezones are believed to match 
    """
    target_files = []

    from_datetime = datetime.fromtimestamp(from_timestamp)
    start_date_mark = from_datetime.strftime("%Y%m%d")
    
    for filename in file_list:
        fname_match = re.match(config['LOG_FILE_PATTERN'], filename)
        if fname_match:
            fname_date_part = fname_match.group(1)
            if int(fname_date_part) > int(start_date_mark):
                target_files.append(filename)

    sorted(target_files, reverse=True)
    return target_files

    

def save_last_processed():
    pass



def main():
    config = load_config()

    log_path = config.get("PROCESS_LOG", None)
    logging_params = {'format':'[%(asctime)s] %(levelname).1s %(message)s', 'level':logging.INFO}
    if log_path:
        logging_params['filename'] = log_path, 
    
    logging.basicConfig(**logging_params)

    logfile_list = os.listdir(config['LOG_DIR'])
    last_processed = load_last_processed(config['TS_FILE'])

    target_files = get_target_files(logfile_list, last_processed)
    logging.info("Target files: %s", ",".join(target_files))
    for tfilename in target_files:
        stat, processed_date = process_log_file(tfilename)
    
    render_report(stat, processed_date)
    save_last_processed()

if __name__ == "__main__":
    main()
