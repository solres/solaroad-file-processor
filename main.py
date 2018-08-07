import configparser
import os
import logging
import multiprocessing
from datetime import datetime as dt
from apscheduler.schedulers.background import BlockingScheduler

PATH = r'\\app-solaroad01\data\Setup\Data'
DB_PATH = 'db'
DB_FILE = 'processedFiles.data'
LOG_PATH = 'log'
CONFIG_FILE = 'config.ini'

formatter = logging.Formatter('%(asctime)s: %(levelname)-8s - [%(name)s] %(message)s')
logger = logging.getLogger('solaroad')
uploadTime = multiprocessing.Value('uploadTime', 2)  # 2 AM everyday
scheduler = BlockingScheduler()

def doProcessing():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    currentFiles = []
    processedFiles = []

    logfile = os.path.join(LOG_PATH, dt.now().strftime('log_%d_%m_%Y.log'))
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    dbFile = os.path.join(DB_PATH, DB_FILE)

    with open(dbFile, 'r') as fileHandle:
        for processedFile in fileHandle.readlines():
            processedFiles.append(processedFile)
        fileHandle.close()

    for dirPath, dirnames, fileNames in os.walk(PATH):
        for fileName in fileNames:
            line = os.path.join(dirPath, fileName)
            currentFiles.append(line)

    for file in [f for f in currentFiles if f not in processedFiles]:
        logger.debug("%s needs to be processed!", file)
        # process(file)
        with open(dbFile, 'w') as fileHandle:
            fileHandle.write(file + "\n")
            fileHandle.close()

    if 'upload_time' in config['SensorCloud']:
        try:
            rescheduledTime = int(config['SensorCloud']['upload_time'])
            if 0 <= rescheduledTime <= 23 and rescheduledTime != uploadTime.value:
                logging.debug('Rescheduling processingJob to %s:00 hours from %s:00 hours everyday!', rescheduledTime,
                              uploadTime.value)
                scheduler.reschedule_job('processing_job', 'cron', hour=rescheduledTime)
                uploadTime.value = rescheduledTime
        except ValueError:
            logger.error('%s is not a valid upload time. Expecting value in the range (0-23).',
                          config['SensorCloud']['upload_time'])

    logger.debug('End of processing for the day!')
    logger.removeHandler(fh)
    fh.close()

scheduler.add_job(doProcessing, 'cron', hour=str(uploadTime.value), id='processing_job')
scheduler.start()