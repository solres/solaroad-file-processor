import logging
import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc

def processFlirPresenceFile(file):
    logger = logging.getLogger('solaroad.flirpresence')

    x = pd.read_csv(file, delimiter=',', skip_blank_lines=True, skipinitialspace=True)
    index_time = pd.to_datetime(x['Time'], format="%d/%m/%Y %H:%M:%S")
    x.index = index_time
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    del (x['Time'])
    x = x.dropna()

    for l in x['Lane'].unique():
        lane = str(l)

        # Authenticate
        server, auth_token = sc.authenticate()
        deviceId = sc.getDeviceId()

        # Add Sensor
        logger.debug('======================== Now processing lane %s ========================', lane)
        sc.addSensor(server, auth_token, deviceId, lane, 'Flir-Presence', lane, lane)

        # Pre-processing
        y = x.resample('3600S').mean().interpolate(method='linear')
        del (y['Lane'])

        # Break DataFrame into chunks of 100k
        ctr = 0
        total_steps = np.round(len(x) / sc.MAX_POINTS) + 1
        while ctr < total_steps:
            sp = ctr * sc.MAX_POINTS
            tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
            logger.debug('--------------------- RECORD %s/%s of %s ------------------------------', ctr + 1,
                         total_steps, lane)
            for key in tmp.keys():
                channel = '_'.join(key.replace('(', '').replace(')', '').replace('#', '').replace('%', '').split(' '))
                packer = xdrlib.Packer()
                packer.pack_int(1)  # version 1

                packer.pack_enum(sc.SECONDS)
                packer.pack_int(3600)

                POINTS = len(tmp[key])
                packer.pack_int(POINTS)

                logger.debug('Now uploading %s', key)

                if ctr == 0:
                    sc.addChannel(server, auth_token, deviceId, lane, channel, channel, channel)

                for item in tmp[key].iteritems():
                    val = item[1]
                    timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                    packer.pack_hyper(int(timestamp))
                    packer.pack_float(float(val))

                data = packer.get_buffer()
                sc.uploadData(server, auth_token, deviceId, lane, channel, data)
            ctr = ctr + 1
