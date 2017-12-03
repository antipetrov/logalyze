#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import argparse
import os
import ConfigParser
import logging
from datetime import datetime

from log_processor import LogProcessor

defaut_config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "REPORT_TEMPLATE": "./report.html",
        "PROCESS_LOG": "./log_analyzer.log",
        "TS_FILE": "./logalize.ts",
        "LAST_PROCESSED_FILE": "./last_processed.ts",
        "LOG_DIR": "./log",
        "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d+).(gz|log)"
    }
 

def load_config():
    pass

def read_logfile():
    pass


def get_last_log():
    pass



def main():
    start_time = datetime.now()
    
    # options 
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', dest='config_file', help='path to config-file')
    args = arg_parser.parse_args()

    if not args.config_file:
        print("Starting with default config on %s"% start_time.isoformat())
    else:
        print("Starting with config '%s' on %s" % (args.config_file, start_time.isoformat()))

    if args.config_file:
        fileconfig = ConfigParser.ConfigParser()
        fileconfig.optionxform = str
        fileconfig.read('default.conf')
        config = dict(fileconfig.defaults())
    else:
        config = default_config

    # loggers
    log_path = config.get("PROCESS_LOG", None)
    if log_path:
        # to file
        print('process log: %s'%os.path.abspath(log_path))
        formatter = logging.Formatter('[%(asctime)s] %(levelname).1s %(message)s')
        file_handler = logging.FileHandler(os.path.abspath(log_path))
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)        
        log.addHandler(file_handler)
    else:
        # to stdout
        print('logging into stoout')
        log.addHandler(stdout_handler)

    # process
    processor = LogProcessor(config)
    processor.process()

    print("Finished %s" % datetime.now().isoformat())

if __name__ == "__main__":
    main()
