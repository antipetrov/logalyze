# logalyze
Анализатор access-логов nginx-а определенного формата.

Разбирает логи из папки и создает суточные html-отчеты со списком URI и временных метрик их обработки, отсортированных по убыванию среднего времени отдачи.

## Конфиг
Конфиг берется из файла вида

```
[DEFAULT]
REPORT_SIZE=1000
REPORT_DIR=./reports
REPORT_TEMPLATE=./report.html
TS_FILE=./logalize.ts
LOG_DIR=./log
LOG_FILE_PATTERN=nginx-access-ui.log-(\d+).(gz|log)
PROCESS_LOG = ./process.log
```

Где 
* REPORT_SIZE - число строк отчета
* REPORT_DIR - папка для создаваемых отчетов
* REPORT_TEMPLATE - файл шаблон для отчета
* TS_FILE - путь к служебному файлу с записью времени последней обрботки
* LOG_DIR - папка с логами для обрабртки
* LOG_FILE_PATTERN - паттерн для имени файла лога - первая группа = дата файла в формате %Y%m%d
* PROCESS_LOG - файл лога скрипта (если не указан - лог выводится в stdout)

Настройки должны всегда находиться в секции [DEFAULT]

## Запуск скрипта
`python log_analyzer.py [--config CONFIG_FILE]`

* --config CONFIG_FILE  - Путь к файлу конфига

## Запуск тестов
python -m unittest test_log_analyzer.py

