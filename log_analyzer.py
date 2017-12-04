#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import os
import argparse
import ConfigParser
import logging
import re
from datetime import datetime
from collections import namedtuple


from log_processor import LogProcessor

default_config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "REPORT_TEMPLATE": "./report.html",
    "PROCESS_LOG": "./log_analyzer.log",
    "TS_FILE": "./logalize.ts",
    "LAST_PROCESSED_FILE": "./last_processed.ts",
    "LOG_DIR": "./log",
    "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d+).(gz|log)"
}

ParsedLine = namedtuple('ParsedLine', 'url','response_time', 'parse_error')


def load_config():
    pass

def read_logfile():
    pass


def get_last_processed(ts_file_path):

    try:
        with open(ts_file_path) as ts_file: 
            last_timestamp = int(ts_file.readline())
            last_datetime = datetime.fromtimestamp(last_timestamp)

    except Exception as e:
        logging.info('Unable to load last processed timestamp. Using 0')
        last_datetime = datetime.fromtimestamp(0)
    

    return last_datetime

def get_last_log(file_list, from_datetime, filename_pattern):
    """
    Выбираем все файлы в списке file_list с подходящим форматом имени и у которых 
    дата в имени файла больше чем from_datetime
    """
    max_date_mark = int(from_datetime.strftime("%Y%m%d"))
    last_filename = None

    for filename in file_list:
        fname_match = re.match(filename_pattern, filename)
        if fname_match:
            fname_date_part = fname_match.group(1)
            if int(fname_date_part) > int(max_date_mark):
                last_filename = filename
                max_date_mark = int(fname_date_part)

    return last_filename

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
        result = (log_match.group(6), log_match.group(7))
    except Exception as e:
        logging.error("error parsing line (no groups):'%s' message:%s", line, e.message)
        raise

    return result


def xread_logline(file):
    # open plain or gzip
    import gzip
    try:
        if filename.endswith('.gz'):
            logfile = gzip.open(filepath)
        else:
            logfile = open(filepath)
    except Exception as e:
        logging.error('Unable to open file %s', file)
        raise

    for line in fp:
        try:
            parsed = parse_log_line(line.rstrip())
            yield ParsedLine(*parsed, None)
        except Exception as e:
            yield ParsedLine(None, None, e)

    logfile.close()

def median(lst):
    n = len(lst)
    if n < 1:
        return None
    if n % 2 == 1:
        return sorted(lst)[n//2]
    else:
        return sum(sorted(lst)[n//2-1:n//2+1])/2.0


def calc_stat(file):
    total_count = 0
    total_time = 0.0
    line_num = 0

    stat = {}
    for line in xread_loglines(file):
        line_num += 1
        if line.parse_error:
            continue

        response_time = float(line.response_time)        
        current_stat = stat.get(uri, {'count':0, 'time_sum': 0.0, 'time_max':0.0, 'time_list':[]})

        current_stat['count'] = current_stat['count'] + 1
        current_stat['time_sum'] = current_stat['time_sum'] + response_time
        current_stat['time_max'] = current_stat['time_max'] if response_time <= current_stat['time_sum'] else response_time
        current_stat['time_list'].append(response_time)
        
        stat[uri] = current_stat
        
        total_count += 1
        total_time += response_time

    # TODO: catch if all lines unparsed
    if total_count == 0:
        logging.error('No lines parsed from %s', file)
        raise Exception('Wrong format')

    # pass 2 - calculate aggregates & convert
    loging.info("Calculating aggregates on total %d lines",line_num)

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

    max_lines = int(self.config.get('REPORT_SIZE', 1000))

    return stat_list[:max_lines], date_processed


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


def process(config):
    # get log files
    from_datetime = get_last_processed(config['LAST_PROCESSED_FILE'])
    logging.info('Trying to fing log-files newer than: %s' % from_datetime.isoformat())
    try:
        log_files_list = os.listdir(config['LOG_DIR'])
    except Exception as e:
        logging.error('Could not open log-dir %s. message: %s', config['LOG_DIR'], e.message)

    # get last log file
    target_file = get_last_log(log_files_list, from_datetime, config['LOG_FILE_PATTERN'])
    if not target_file:
        logging.info('No logfile newer than %s found. Exiting', from_datetime.isoformat())
        exit()
    
    logging.info("Processing logfile: %s" % target_file)
    stat = calc_stat(target_file) # can have exception

    



def main():
    start_time = datetime.now()

    
    # options 
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', dest='config_file', help='path to config-file')
    args = arg_parser.parse_args()

    if not args.config_file:
        print("Starting with default config %s"% start_time.isoformat())
    else:
        print("Starting with config '%s' %s" % (args.config_file, start_time.isoformat()))

    config = default_config

    if args.config_file:
        fileconfig = ConfigParser.ConfigParser()
        fileconfig.optionxform = str
        fileconfig.read('default.conf')
        config_loaded = dict(fileconfig.defaults())
        config.update(config_loaded)

    # logging
    logging_params = {
        'format':'[%(asctime)s] %(levelname).1s %(message)s', 
        'datefmt':'%Y.%m.%d %H:%M:%S',
        'level':logging.INFO
        }

    log_path = config.get("PROCESS_LOG", None)
    if log_path:
        print('Process log: %s'%os.path.abspath(log_path))
        logging_params['filename'] = log_path
    else:
        print('Process log: stdout' )
    logging.basicConfig(**logging_params)
    logging.info('Processing started')

    # process
    process(conig)
    
    print("Finished %s" % datetime.now().isoformat())

if __name__ == "__main__":
    main()
