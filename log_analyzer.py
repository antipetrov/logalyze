#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


import os
import shutil
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

LogFile = namedtuple('LogFile', 'filename', 'date')
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


    max_datetime = datetime.datetime.strptime(max_date_mark, '%Y%m%d')
    return last_filename, max_datetime

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


def process_logfile(file):
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

def render_report(stat_list, template_file):
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


        return report_str  

def save_report(report_str, report_dir, report_date):
    report_tmp_filename = "./report.tmp"
    try:
        with open(report_tmp_filename, 'w') as freport:
            frep.write(report_str)
    except Exception as e:
        logging.error('Failed to write report to file: %s', e.message)
        return None

    try:
        report_filename = os.path.join(report_dir, "report_%s.html"%datetime.strftime(report_date, "%Y.%m.%d"))
        shutils.copyfile(report_tmp_filename, report_filename)
    except Exception as e:
        logging.error('Failed to write report to destination: %s', e.message)
        return None        

    return report_filename


def update_ts_file(ts_filename):
    timestamp = int(time.mktime(datetime.now().timetuple()))
    
    try:
        fp = open(ts_filename, 'w')
        fp.write(str(timestamp))
        fp.close()
    except Exception as e:
        log.error('Unable to update ts file "%s": %s', ts_filename, e.message)
        return False

    return True


def process(config):
    # get log files
    from_datetime = get_last_processed(config['LAST_PROCESSED_FILE'])
    logging.info('Trying to fing log-files newer than: %s' % from_datetime.isoformat())
    try:
        log_files_list = os.listdir(config['LOG_DIR'])
    except Exception as e:
        logging.error('Could not open log-dir %s. message: %s', config['LOG_DIR'], e.message)

    # get last log file
    target_file, target_date = get_last_log(log_files_list, from_datetime, config['LOG_FILE_PATTERN'])
    if not target_file:
        logging.info('No logfile newer than %s found. Exiting', from_datetime.isoformat())
        exit()
    
    logging.info("Processing logfile: %s" % target_file)
    stat = process_logfile(target_file) # can have exception

    report_str = render_report(stat, config['TEMPLATE_FILE'])
    saved_filename = save_report(report_str, config['REPORT_DIR'], target_date)
    print("report file: %s" % saved_filename)

    if saved_filename:
        update_last_processed(config['LAST_PROCESSED_FILE'], target_date)
        update_ts(config['TS_FILE'], datetime.now())






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
