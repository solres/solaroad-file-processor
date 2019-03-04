import json
import os
import re
from pathlib import Path
import pandas as pd
import urllib.request
import logging
from datetime import datetime as dt
from apscheduler.schedulers.blocking import BlockingScheduler

URL = 'http://192.168.1.110'
DEFAULT_PATH = '/api/data?begintime=1970-01-01T00%3A00%3A00.000%2B00%3A00'
# URL = 'https://raw.githubusercontent.com/solres/sol-resources/master/test.json'
# DEFAULT_PATH = ''
FLIR_LOG_PATH = 'C:\\Users\\ra-solaroadzwaarver\\Downloads\\Arun\\Flir\\'
# FLIR_LOG_PATH = ' '
DB_PATH = 'db'
DB_FILE = 'processedPaths.data'
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s - [%(name)s] %(message)s')
logger = logging.getLogger('flirDataLogger')
logger.setLevel(logging.DEBUG)


def processPath(df, path):
    with urllib.request.urlopen(path) as response:
        logger.debug('ResponseCode = ' + str(response.getcode()))
        if response.getcode() != 200:
            return {}, ''

        for line in response:
            f = line.decode('utf-8')

    logger.debug('Going to load json')
    y = json.loads(f)

    for data in y['data']:
        for zone in data['zone']:
            for clazz in zone['class']:
                index = int(clazz['classNr'])
                if index not in df:
                    df[index] = pd.DataFrame({data['time']: clazz}).transpose()
                else:
                    df[index] = pd.concat([df[index], pd.DataFrame({data['time']: clazz}).transpose()])

    logger.debug('Going to re-index')
    for d in df.values():
        d.index = pd.to_datetime(d.index, utc=True)
        d.index.name = 'Timestamp'

    if 'nextDataUrl' in y:
        logger.debug('Next step' + y['nextDataUrl'])
        return df, y['nextDataUrl']
    else:
        print('No Next step')
        return df, ''


def getNextPath(path):
    with urllib.request.urlopen(path) as response:
        logger.debug('ResponseCode = ' + str(response.getcode()))
        if response.getcode() != 200:
            return ''

        for line in response:
            f = line.decode('utf-8')

    y = json.loads(f)
    if 'nextDataUrl' in y:
        return y['nextDataUrl']
    else:
        return ''


def publishToCSV(df, date):
    for classNr in df.keys():
        filename = FLIR_LOG_PATH + "class" + str(classNr) + "_" + date + ".csv"
        file = Path(filename)
        if file.is_file():
            args = {"mode": "a", "header": False}
        else:
            args = {"mode": "a"}
        df[classNr].to_csv(filename, **args)


def doProcessing():
    logfile = os.path.join('log', dt.now().strftime('log_%d_%m_%Y.log'))
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    lastProcessedPath = ''

    dbFile = os.path.join(DB_PATH, DB_FILE)
    with open(dbFile, 'r') as fileHandle:
        for processedPath in fileHandle.readlines():
            lastProcessedPath = processedPath
        fileHandle.close()

    if lastProcessedPath is not '':
        next_path = getNextPath(URL + lastProcessedPath)
    else:
        next_path = URL + DEFAULT_PATH

    if next_path is '':
        logger.debug('No more paths to process! Skipping this round!')
    else:
        while next_path is not '':
            date = re.match('.*=(.*)T.*', next_path)[1]
            lastProcessedPath = next_path
            df, next_path = processPath(URL + next_path)
            if df:
                logger.debug('Writing to file!')
                publishToCSV(df, date)

            logger.debug('Processed until %s !!', lastProcessedPath)
            with open(dbFile, 'w') as fileHandle:
                fileHandle.write(lastProcessedPath)
                fileHandle.close()

    fh.close()


doProcessing()
scheduler = BlockingScheduler()
scheduler.add_job(doProcessing, 'interval', hours=1)
scheduler.start()
