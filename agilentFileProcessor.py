import xdrlib
from builtins import len, str, int, float
import sensorcloud as sc
import pandas as pd
import numpy as np
import logging

SENSOR_NAME = 'Agilent'


def processAgilentFile(file):
    logger = logging.getLogger('solaroad.agilent')
    numSkipRows = 0
    with open(file, encoding="utf-16") as csvFile:
        line = csvFile.readline()
        while line[:5] != "Scan,":
            numSkipRows += 1
            line = csvFile.readline()
    x = pd.read_csv(file, delimiter=',', skip_blank_lines=True, skiprows=numSkipRows, encoding="utf-16")
    index_time = pd.to_datetime(x['Time'], format="%m/%d/%Y %H:%M:%S:%f")
    x.index = index_time
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    x = x.dropna()
    del (x['Time'])
    del (x['Scan'])

    server, auth_token = sc.authenticate()
    deviceId = sc.getDeviceId()

    # Add Sensor
    logger.debug('======================== Now processing Agilent ========================')
    sc.addSensor(server, auth_token, deviceId, SENSOR_NAME, SENSOR_NAME, SENSOR_NAME, SENSOR_NAME)

    # Pre-processing
    y = x.resample('120S').mean().interpolate(method='linear')

    # Break DataFrame into chunks of 100k
    ctr = 0
    total_steps = np.round(len(x) / sc.MAX_POINTS) + 1
    while ctr < total_steps:
        sp = ctr * sc.MAX_POINTS
        tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
        logger.debug('--------------------- RECORD %s/%s ------------------------------', str(ctr + 1),
                     str(total_steps))
        for key in tmp.keys():
            channel = '_'.join(key.replace('(', '').replace(')', '').replace('<', '').replace('>', '').split(' '))
            packer = xdrlib.Packer()
            packer.pack_int(1)  # version 1

            packer.pack_enum(sc.SECONDS)
            packer.pack_int(120)

            POINTS = len(tmp[key])
            packer.pack_int(POINTS)

            logger.debug('Now uploading %s', key)

            if ctr == 0:
                sc.addChannel(server, auth_token, deviceId, SENSOR_NAME, channel, channel, channel)

            for item in tmp[key].iteritems():
                val = item[1]
                timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                packer.pack_hyper(int(timestamp))
                packer.pack_float(float(val))

            data = packer.get_buffer()
            sc.uploadData(server, auth_token, deviceId, SENSOR_NAME, channel, data)
        ctr = ctr + 1
