import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc
import logging


def processGillFile(file):

    logger = logging.getLogger('solaroad.gill')
    x = pd.read_csv(file, delimiter=',', skiprows=5, header=None, skip_blank_lines=True)
    x.columns = ['Node', 'Pressure', 'Relative Humidity', 'Temperature', 'Dew Point', 'Solar Radiation', 'Measured Time', 'Voltage', 'Status', 'EndChar', 'SlNo', 'Reporting Time']
    x.index = pd.to_datetime(x['Reporting Time'], format="%d/%m/%Y %H:%M:%S")
    x.index.name = 'Timestamp'
    del(x['SlNo'])
    del(x['Node'])
    del(x['Reporting Time'])
    del(x['Measured Time'])
    del(x['Voltage'])
    del(x['Status'])
    del(x['EndChar'])
    x = x.dropna()

    sensorName = 'GILL_UVW_THREE_AXIS'
    logger.debug('======================== Now processing %s ========================', sensorName)

    server, auth_token = sc.authenticate()
    deviceId = sc.getDeviceId()

    ctr = 0
    total_steps = np.round(len(x) / sc.MAX_POINTS) + 1
    while ctr < total_steps:
        sp = ctr * sc.MAX_POINTS
        tmp = x.iloc[sp:sp + sc.MAX_POINTS - 1, :]
        logger.debug('--------------------- RECORD %s/%s ------------------------------', ctr + 1, total_steps)

        sc.addSensor(server, auth_token, deviceId, sensorName, sensorName, sensorName, sensorName)

        packer = xdrlib.Packer()
        packer.pack_int(1)  # version 1

        packer.pack_enum(sc.SECONDS)
        packer.pack_int(300)

        POINTS = len(tmp)
        packer.pack_int(POINTS)

        logger.debug('Now uploading %s', sensorName)

        for key in tmp.keys():
            channel = '_'.join(key.replace('(', '').replace(')', '').split(' '))
            sc.addChannel(server, auth_token, deviceId, sensorName, channel, channel, channel)

            for item in tmp[key].iteritems():
                val = item[1]
                timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                packer.pack_hyper(int(timestamp))
                packer.pack_float(float(val))

            data = packer.get_buffer()
            sc.uploadData(server, auth_token, deviceId, sensorName, channel, data)
        ctr = ctr + 1
