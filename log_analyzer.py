#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import os
import re
from datetime import datetime

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
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
    """
    pass

def process_log_file(filename):
    """
    process logfile
    """
    pass

def get_target_files(file_list, from_timestamp):
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

    dirfiles = os.listdir(config['LOG_DIR'])
    last_processed = load_last_processed(config['TS_FILE'])

    target_files = get_target_files(dirfiles, last_processed)
    print(target_files)

if __name__ == "__main__":
    main()
