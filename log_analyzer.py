#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import json
import gzip
import shutil
import logging
import re
import time
import operator
import ConfigParser

from datetime import datetime
from collections import namedtuple

ParsedLine = namedtuple('ParsedLine', ('url', 'response_time'))
LogfileData = namedtuple('LogfileData', ('filename', 'date'))


def init_config(config_filename):

    # default config
    config = {
        "REPORT_SIZE": 1000,
        "REPORT_DIR": "./reports",
        "REPORT_TEMPLATE": "./report.html",
        "PROCESS_LOG": "./log_analyzer.log",
        "TS_FILE": "./log_analyzer.ts",
        "LAST_PROCESSED_FILE": "./last_processed.ts",
        "LOG_DIR": "./log",
        "LOG_FILE_PATTERN": "nginx-access-ui.log-(\d{8}).(gz|log)",
        "PARSE_ERROR_PERC_MAX": 0.2
    }

    # check config file
    if not os.path.isfile(config_filename):
        print('Could not find config file %s. Exiting' % config_filename)
        return False

    # read-update config
    fileconfig = ConfigParser.ConfigParser()
    fileconfig.optionxform = str
    try:
        fileconfig.read(config_filename)
        config_loaded = dict(fileconfig.defaults())
        config.update(config_loaded)
    except ConfigParser.Error as e:
        print('Could parse config file %s. Error: %s. Exiting' %
              (config_filename, e.message))
        return False

    return config


def get_last_log(file_list, filename_pattern):
    """
    Выбираем самый поздний файл в списке file_list с подходящим форматом имени
    Дату берем из имени файла
    """
    max_date_mark = 0
    last_filename = None

    for filename in file_list:
        fname_match = re.match(filename_pattern, filename)
        if fname_match:
            fname_date_part = fname_match.group(1)
            if int(fname_date_part) > int(max_date_mark):
                last_filename = filename
                max_date_mark = fname_date_part

    max_datetime = datetime.strptime(max_date_mark, '%Y%m%d')
    return LogfileData(filename=last_filename, date=max_datetime)


def parse_log_line(line):
    """
    process single log line

    return tuple(url, response_time)
    """
    pattern = ('([0-9.]+) (.*) (.*) \[(.*)\] \"(\S+) ([^"]+) HTTP[^"]*\" .* '
               '([0-9.]+)$')
    try:
        log_match = re.match(pattern, line)
    except Exception as e:
        logging.error("error parsing line:'%s' message:%s", line, e.message)
        raise

    try:
        result = ParsedLine(url=log_match.group(
            6), response_time=log_match.group(7))
    except Exception as e:
        logging.error(
            "error parsing line (no groups):'%s' message:%s", line, e.message)
        raise

    return result


def xread_loglines(filename):
    # open plain or gzip
    try:
        if filename.endswith('.gz'):
            logfile = gzip.open(filename)
        else:
            logfile = open(filename)
    except Exception as e:
        logging.error('Unable to open file %s', filename)
        raise

    for line in logfile:
        try:
            parsed = parse_log_line(line.decode('utf-8').rstrip())
            yield parsed, None
        except UnicodeDecodeError as e:
            yield None, e
        except Exception as e:
            yield None, e

    logfile.close()


def median(lst):
    n = len(lst)
    if n < 1:
        return None
    if n % 2 == 1:
        return sorted(lst)[n // 2]
    else:
        return sum(sorted(lst)[n // 2 - 1:n // 2 + 1]) / 2.0


def process_logfile(log_lines, report_size=1000, parse_error_perc_max=0.0):
    line_count = 0
    parsed_count = 0
    total_time = 0.0

    stat = {}
    for line, parse_error in log_lines:
        line_count += 1
        if parse_error:
            continue

        response_time = float(line.response_time)
        current_stat = stat.get(
            line.url, {'count': 0, 'time_sum': 0.0,
                       'time_max': 0.0, 'time_list': []})

        current_stat['count'] = current_stat['count'] + 1
        current_stat['time_sum'] = current_stat['time_sum'] + response_time
        current_stat['time_max'] = (
            current_stat['time_max']
            if response_time <= current_stat['time_sum']
            else response_time)

        current_stat['time_list'].append(response_time)

        stat[line.url] = current_stat
        parsed_count += 1
        total_time += response_time

    # проверяем чтобы процент ощибочных строк был не больше максимума
    #
    if float(parsed_count) / line_count < (1.0 - parse_error_perc_max):
        logging.error('Wrong format. %d of %d lines parsed. '
                      'More than %d%% of errors - failed parsing',
                      parsed_count, line_count,
                      int(parse_error_perc_max * 100))
        raise Exception('Wrong format')

    # pass 2 - calculate aggregates & convert
    logging.info("Calculating aggregates on total %d lines", parsed_count)

    stat_list = []
    for url, data in stat.iteritems():
        stat_list.append({
            'url': url,
            'count': data['count'],
            'time_max': data['time_max'],
            'time_sum': data['time_sum'],
            'time_avg': data['time_sum'] / data['count'],
            'time_med': median(data['time_list']),
            'time_perc': data['time_sum'] / total_time,
            'count_perc': float(data['count']) / parsed_count,
        })

    # sort it
    stat_list.sort(key=operator.itemgetter('time_avg'), reverse=True)

    return stat_list[:report_size]


def render_report(stat_list, template_filename):
    """
    Render stat into html file.
    Uses REPORT_TEMPLATE from config as page-template

    :param stat - stat dict, where keys - uri`s
    :param processed_date - inital date - used in resulting filename

    result row: {"count": 2767,
                 "time_avg": 62.994999999999997,
                 "time_max": 9843.5689999999995,
                 "time_sum": 174306.35200000001,
                 "url": "/api/v2/internal/html5/phantomjs/queue/?wait=1m",
                 "time_med": 60.073,
                 "time_perc": 9.0429999999999993,
                 "count_perc": 0.106}
    """

    # re-format stat to list
    stat_rows = []
    for data in stat_list:
        stat_rows.append({
            'url': data['url'],
            'count': data['count'],
            'time_avg': '{0:.10f}'.format(data['time_avg']),
            'time_max': '{0:.10f}'.format(data['time_max']),
            'time_sum': '{0:.10f}'.format(data['time_sum']),
            'time_med': '{0:.10f}'.format(data['time_med']),
            'time_perc': '{0:.10f}'.format(data['time_perc']),
            'count_perc': '{0:.10f}'.format(data['count_perc']),
        })

    stat_json = json.dumps(stat_rows)

    try:
        with open(template_filename, "r") as ftemp:
            template = "".join(ftemp.readlines())
    except Exception as e:
        logging.error('Failed to read report template file "%s": %s',
                      template_filename, e.message)
        return None

    report_str = template.replace('$table_json', stat_json)
    return report_str


def get_report_filename(report_dir, report_datetime):
    return os.path.join(report_dir, "report_%s.html" %
                        datetime.strftime(report_datetime, "%Y.%m.%d"))


def save_report(report_str, report_filename):
    report_tmp_filename = "./report.tmp"
    try:
        with open(report_tmp_filename, 'w') as freport:
            freport.write(report_str)
    except Exception as e:
        logging.error('Failed to write report to tmp-file: %s', e.message)
        return None

    try:
        shutil.copyfile(report_tmp_filename, report_filename)
    except Exception as e:
        logging.error('Failed to write report to destination: %s', e.message)
        return None

    os.remove(report_tmp_filename)
    return True


def update_ts_file(ts_filename):
    timestamp = int(time.mktime(datetime.now().timetuple()))

    try:
        with open(ts_filename, 'w') as ts_file:
            ts_file.write(str(timestamp))
    except Exception as e:
        logging.error('Unable to update ts file "%s": %s',
                      ts_filename, e.message)
        return False

    return True


def process(config):
    # get last logfile
    try:
        log_files_list = os.listdir(config['LOG_DIR'])
    except Exception as e:
        logging.error('Could not open log-dir %s. message: %s',
                      config['LOG_DIR'], e.message)
        update_ts_file(config['TS_FILE'])
        return False

    target_logfile_data = get_last_log(
        log_files_list, config['LOG_FILE_PATTERN'])
    
    if not target_logfile_data:
        logging.info('No logfile found. Exiting')
        return True

    # create report filename
    report_filename = get_report_filename(config['REPORT_DIR'],
                                          target_logfile_data.date)

    # check if report exists
    if os.path.isfile(report_filename):
        logging.info("Report for %s already exists. Exiting",
                     target_logfile_data.date.isoformat())
        return True

    logging.info("Processing logfile: %s" % target_logfile_data.filename)
    log_path = os.path.join(config['LOG_DIR'], target_logfile_data.filename)
    try:
        stat = process_logfile(
            log_lines=xread_loglines(log_path),
            report_size=int(config['REPORT_SIZE']),
            parse_error_perc_max=config.get('PARSE_ERROR_PERC_MAX', 0.2)
        )
    except Exception as e:
        logging.error('Processing failed: %s', e.message)
        return False

    report_str = render_report(stat, config['REPORT_TEMPLATE'])
    saved = save_report(report_str, report_filename)
    logging.info("Report generated: %s", report_filename)

    return True


def main():

    # options
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config',
                            dest='config_file',
                            default='/usr/local/etc/log_analyzer.conf',
                            help='path to config-file')
    args = arg_parser.parse_args()

    config = init_config(args.config_file)
    if not config:
        sys.exit(1)

    print("Started %s" % datetime.now().isoformat())

    # logging
    logging.basicConfig(
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        level=logging.INFO,
        filename=config.get("PROCESS_LOG", None)
    )

    logging.info('Processing started')
    # process
    try:
        processed = process(config)
    except Exception:
        logging.exception('Processing error')
        sys.exit(1)

    if processed:
        update_ts_file(config['TS_FILE'])


    logging.info('Processing finished')
    print("Finished %s" % datetime.now().isoformat())


if __name__ == "__main__":
    main()
