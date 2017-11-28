#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import argparse
import logging
import ConfigParser

from log_processor import LogProcessor

# config default

def main():


    # options 
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', dest='config_file', help='path to config-file')
    args = arg_parser.parse_args()

    if not args.config_file:
        print("Starting with default config\n")
    else:
        print("Starting with config '%s'\n" % args.config_file)

    if args.config_file:
        fileconfig = ConfigParser.ConfigParser()
        fileconfig.optionxform = str
        fileconfig.read('default.conf')
        config = dict(fileconfig.defaults())
    else:
        config = LogProcessor.default_config

    # logging 
    log_path = config.get("PROCESS_LOG", None)
    logging_params = {'format':'[%(asctime)s] %(levelname).1s %(message)s', 'level':logging.INFO}
    if log_path:
        logging_params['filename'] = log_path
    
    logging.basicConfig(**logging_params)

    # init processor
    processor = LogProcessor(config)
    processor.process()


if __name__ == "__main__":
    main()
