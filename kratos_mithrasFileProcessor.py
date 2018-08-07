import logging
import re
import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc

REGEX = '^.*\((.*)\).log'

def processKratosFile(file):
    y = pd.read_table(file, skip_blank_lines=True)
    index_time = pd.to_datetime(y['Date'] + ' ' + y[' Time'], format='%m/%d/%Y %I:%M:%S %p')
    y.index = index_time
    del (y['Date'])
    del (y[' Time'])
    del (y['Unnamed: 10'])
    del (y['State'])

    m = re.search(REGEX, file)
    sensorId = m.group(1)
    processDataFrame(y, 'kratos', sensorId)

def processMithrasFile(file):
    y = pd.read_table(file, skip_blank_lines=True, skiprows=1)
    index_time = pd.to_datetime(y['Date'] + " " + y['Time'], format="%m/%d/%Y %I:%M:%S %p")
    y.index = index_time
    del (y['Date'])
    del (y['Time'])
    del (y['State'])
    del (y['Pwm Duty'])
    del (y['Panel Voltage'])
    del (y['Panel Current'])
    del (y['Panel Power'])
    del (y['Femto 48V'])
    del (y['Temp msp'])

    m = re.search(REGEX, file)
    sensorId = m.group(1)
    processDataFrame(y, 'mithras', sensorId)

def processDataFrame(y, sensor, sensorId):
    logger = logging.getLogger('solaroad.' + sensor)

    # Pre-processing
    x = y[np.abs(y['Energy (Wh)'] - y['Energy (Wh)'].mean()) <= (2 * y['Energy (Wh)'].std())]
    x = x[(x['Energy (Wh)'] != 0)]
    x = x.dropna()
    x = x.resample('10S').mean().interpolate(method='linear')

    # Break DataFrame into chunks of 100k
    ctr = 0
    total_steps = np.round(len(x) / sc.MAX_POINTS) + 1
    while ctr < total_steps:
        # Authenticate
        server, auth_token = sc.authenticate()
        deviceId = sc.getDeviceId()

        sp = ctr * sc.MAX_POINTS
        tmp = x.iloc[sp:sp + sc.MAX_POINTS - 1, :]
        logger.debug('--------------------- RECORD %s/%s of %s ------------------------------', ctr + 1, total_steps,
                     sensorId)
        for key in tmp.keys():
            channel = '_'.join(key.replace('(', '').replace(')', '').split(' '))
            packer = xdrlib.Packer()
            packer.pack_int(1)  # version 1

            packer.pack_enum(sc.SECONDS)
            packer.pack_int(10)

            POINTS = len(tmp[key])
            packer.pack_int(POINTS)

            logger.debug('Now uploading %s', key)

            if ctr == 0:
                sc.addChannel(server, auth_token, deviceId, sensorId, channel, channel, channel)

            for item in tmp[key].iteritems():
                val = item[1]
                timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                packer.pack_hyper(int(timestamp))
                packer.pack_float(float(val))

            data = packer.get_buffer()
            sc.uploadData(server, auth_token, deviceId, sensorId, channel, data)
        logger.debug('--------------------- FINISHED RECORD %s/%s of %s ------------------------------', ctr + 1,
                     total_steps, sensorId)
        ctr = ctr + 1
