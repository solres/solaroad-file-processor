import configparser
import os
import logging
import multiprocessing
from datetime import datetime as dt
from apscheduler.schedulers.background import BlockingScheduler
from itertools import chain
import agilentFileProcessor as agilent
import apsFileProcessor as aps
import gillFileProcessor as gill
import flir_bicycleFileProcessor as flirBicycle
import legrandFileProcessor as legrand
import aps_datalogger as apsDL
import flir_datalogger as flirDL


PATHS = [
    'C:\\Users\\Public\\Documents\\Measure Software\\CSV\\Energies',
    'C:\\Users\\ra-solaroadzwaarver\\Documents\\APS',
    'C:\\Users\\ra-solaroadzwaarver\\Documents\\Flir',
    'C:\\Users\\ra-solaroadzwaarver\\Documents\\Gill_logdata',
    'C:\\Users\\ra-solaroadzwaarver\\Documents\\TNO_Agilent'
]
DB_PATH = 'db'
DB_FILE = 'processedFiles.data'
LOG_PATH = 'log'
CONFIG_FILE = 'config.ini'

formatter = logging.Formatter('%(asctime)s: %(levelname)-8s - [%(name)s] %(message)s')
logger = logging.getLogger('solaroad')
logger.setLevel(logging.DEBUG)
uploadTime = multiprocessing.Value('i', 2)  # 2 AM everyday
scheduler = BlockingScheduler()

# Start the Flir and APS data loggers
apsDL.start()
flirDL.start()


def doProcessing():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    currentFiles = []
    processedFiles = []

    if 'fh' in locals():
        logger.removeHandler(fh)
        fh.close()

    logfile = os.path.join(LOG_PATH, dt.now().strftime('log_%d_%m_%Y_%H_%M_%S.log'))
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    dbFile = os.path.join(DB_PATH, DB_FILE)

    with open(dbFile, 'r') as fileHandle:
        for processedFile in fileHandle.readlines():
            processedFiles.append(processedFile[:-1])
        fileHandle.close()

    for dirPath, dirs, fileNames in chain.from_iterable(os.walk(path) for path in PATHS):
        for fileName in fileNames:
            line = os.path.join(dirPath, fileName)
            currentFiles.append(line)

    filesToBeProcessed = [f for f in currentFiles if f not in processedFiles]
    logger.debug('Going to process %d files : %s', len(filesToBeProcessed), filesToBeProcessed)

    for file in filesToBeProcessed:
        try:
            if 'Agilent' in file:
                agilent.processAgilentFile(file)
            elif 'Aps' in file:
                aps.processAPSFile(file)
            elif 'Gill' in file:
                gill.processGillFile(file)
            elif 'Flir' in file:
                flirBicycle.processFlirBicycleFile(file)
            elif 'Energies' in file:
                legrand.processLeGrandFile(file)
            else:
                logger.error('Unknown sensor datatype: %s', file)
        except:
            logger.error('An error occurred trying to process %s', file)

        with open(dbFile, 'a') as fileHandle:
            fileHandle.write(file + '\n')
            fileHandle.close()

    logger.debug('Done processing files!')

    if 'upload_time' in config['SensorCloud']:
        try:
            rescheduledTime = int(config['SensorCloud']['upload_time'])
            if 0 <= rescheduledTime <= 23 and rescheduledTime != uploadTime.value:
                logging.debug('Rescheduling processingJob to %s:00 hours from %s:00 hours everyday!', rescheduledTime,
                              uploadTime.value)
                scheduler.reschedule_job('processing_job', 'cron', hour=str(rescheduledTime))
                uploadTime.value = rescheduledTime
        except ValueError:
            logger.error('%s is not a valid upload time. Expecting value in the range (0-23).',
                         config['SensorCloud']['upload_time'])

    logger.debug('End of processing for the day!')


# doProcessing()

scheduler.add_job(doProcessing, 'cron', hour=str(uploadTime.value), id='processing_job', misfire_grace_time=900, coalesce=True)
scheduler.start()
