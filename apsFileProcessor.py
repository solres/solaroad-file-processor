import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc
import logging


def processAPSFile(file):
    logger = logging.getLogger('solaroad.aps')
    x = pd.read_csv(file, delimiter=';', skip_blank_lines=True)
    index_time = pd.to_datetime(x['Date'], format="%Y-%m-%d %H:%M:%S")
    x.index = index_time
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    x = x.dropna()

    for inverterId in x['Inverter ID'].unique():
        logger.debug('======================== Now processing %s ========================', inverterId)
        y = x.loc[x['Inverter ID'] == inverterId]
        del (y['Inverter ID'])
        del (y['Date'])

        server, auth_token = sc.authenticate()
        deviceId = sc.getDeviceId()

        ctr = 0
        total_steps = np.round(len(y) / sc.MAX_POINTS) + 1
        while ctr < total_steps:
            sp = ctr * sc.MAX_POINTS
            tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
            logger.debug('--------------------- RECORD %s/%s ------------------------------', ctr + 1, total_steps)

            sc.addSensor(server, auth_token, deviceId, inverterId, inverterId, inverterId, inverterId)

            packer = xdrlib.Packer()
            packer.pack_int(1)  # version 1

            packer.pack_enum(sc.SECONDS)
            packer.pack_int(300)

            POINTS = len(tmp)
            packer.pack_int(POINTS)

            logger.debug('Now uploading %s', inverterId)

            for key in tmp.keys():
                channel = '_'.join(key.replace('(', '').replace(')', '').split(' '))
                sc.addChannel(server, auth_token, deviceId, inverterId, channel, channel, channel)

                for item in tmp[key].iteritems():
                    val = item[1]
                    timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                    packer.pack_hyper(int(timestamp))
                    packer.pack_float(float(val))

                data = packer.get_buffer()
                sc.uploadData(server, auth_token, deviceId, inverterId, channel, data)
            ctr = ctr + 1
