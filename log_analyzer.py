#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import os
import re
import logging
from datetime import datetime

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
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


def parse_log_line(line):
    """
    process single log line

    return tuple(url, response_time)
    """
    pattern = '([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* ([0-9.]+)$'
    try:
        log_match = re.match(pattern, line)
    except Exception as e:
        #TODO: Add logging
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

    import gzip
    if filename.endswith('.gz'):
        fp = gzip.open(filename)
    else:
        fp = open(filename)


    stat = {}

    total_count = 0
    total_rtime = 0
    line_num = 0

    for line in fp:
        line_num += 1
        try:
            uri, rtime = parse_log_line(line)
        except Exception as e:
            logging.info("skipped line  %d", line_num)
            continue
        
        try:
            prev_stat = stat[uri]
        except Exception as e:
            prev_stat = stat[uri] = {'count':0, 'time_sum': 0, 'time_max':0}

        stat[uri]['count'] = prev_stat['count'] + 1
        stat[uri]['time_sum'] = prev_stat['time_sum'] + rtime
        stat[uri]['time_max'] = prev_stat['time_sum'] if rtime <= prev_stat['time_sum'] else rtime
        total_count += 1
        total_rtime += rtime

        if line_num % 100000 == 0:
            logging.info("%d lines processed", line_num)
            


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



def main():
    config = load_config()

    log_path = config.get("PROCESS_LOG", None)
    logging_params = {'format':'[%(asctime)s] %(levelname).1s %(message)s', 'level':logging.INFO}
    if log_path:
        logging_params['filename'] = log_path, 
    
    logging.basicConfig(**logging_params)


    dirfiles = os.listdir(config['LOG_DIR'])
    last_processed = load_last_processed(config['TS_FILE'])

    target_files = get_target_files(dirfiles, last_processed)
    print(target_files)

    stat = process_log_file(os.path.join(config['LOG_DIR'], target_files[0]))
    print(stat)

if __name__ == "__main__":
    main()
