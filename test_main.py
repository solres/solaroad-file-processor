import configparser
import os
import logging
from datetime import datetime as dt
import agilentFileProcessor as agilent
import apsFileProcessor as aps
import autarcoFileProcessor as autarco
import flir_bicycleFileProcessor as flirBicycle
import flir_presenceFileProcessor as flirPresence
import kratos_mithrasFileProcessor as kratosMithras
import legrandFileProcessor as legrand

#PATH = r'\\app-solaroad01\data\Setup\Data'
#PATH = 'C:\Data\Setup\Data'
PATH = r'\\app-solaroad01\data\Setup\Data\LeGrand'
DB_PATH = 'db'
DB_FILE = 'processedFiles.data'
LOG_PATH = 'log'
CONFIG_FILE = 'config.ini'

formatter = logging.Formatter('%(asctime)s: %(levelname)-8s - [%(name)s] %(message)s')
logger = logging.getLogger('solaroad')
logger.setLevel(logging.DEBUG)

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
            processedFiles.append(processedFile[:-1])
        fileHandle.close()

    for dirPath, dirnames, fileNames in os.walk(PATH):
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
            elif 'Autarco' in file:
                autarco.processAutarcoFile(file)
            elif 'FlirBicycle' in file:
                flirBicycle.processFlirBicycleFile(file)
            elif 'FlirOther' in file:
                flirPresence.processFlirPresenceFile(file)
            elif 'kratos' in file:
                kratosMithras.processKratosFile(file)
            elif 'mithras' in file:
                kratosMithras.processMithrasFile(file)
            elif 'LeGrand' in file:
                legrand.processLeGrandFile(file)
            else:
                logger.error('Unknown sensor datatype: %s', file)
        except:
            logger.error('An error occurred trying to process %s', file)

        # with open(dbFile, 'a') as fileHandle:
        #     fileHandle.write(file + '\n')
        #     fileHandle.close()

    logger.debug('Done processing files!')

    logger.debug('End of processing for the day!')
    logger.removeHandler(fh)
    fh.close()

doProcessing()