import logging
import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc


def processFlirBicycleFile(file):
    logger = logging.getLogger('solaroad.flir')
    x = pd.read_csv(file, delimiter=',', skip_blank_lines=True, skipinitialspace=True)
    index_time = pd.to_datetime(x['Timestamp'])
    x.index = index_time
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    del (x['Timestamp'])
    x = x.dropna()

    for zone in x['classNr'].unique():
        # Authenticate
        server, auth_token = sc.authenticate()
        deviceId = sc.getDeviceId()

        sensor = 'Flir_Class_'+str(zone)

        # Add Sensor
        logger.debug('======================== Now processing Class %s ========================', zone)
        sc.addSensor(server, auth_token, deviceId, sensor, sensor, sensor, sensor)

        # Pre-processing
        y = x.loc[x['classNr'] == zone]
        del (y['classNr'])
        y = y.resample('60min').sum().interpolate(method='pad')

        # Break DataFrame into chunks of 100k
        ctr = 0
        total_steps = np.round(len(x) / sc.MAX_POINTS) + 1
        while ctr < total_steps:
            sp = ctr * sc.MAX_POINTS
            tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
            logger.debug('--------------------- RECORD %s/%s of %s ------------------------------', ctr + 1,
                         total_steps, zone)
            for key in tmp.keys():
                channel = '_'.join(key.replace('(', '').replace(')', '').replace('#', '').split(' '))
                packer = xdrlib.Packer()
                packer.pack_int(1)  # version 1

                packer.pack_enum(sc.SECONDS)
                packer.pack_int(3600)

                POINTS = len(tmp[key])
                packer.pack_int(POINTS)

                logger.debug('Now uploading %s', key)

                if ctr == 0:
                    sc.addChannel(server, auth_token, deviceId, sensor, channel, channel, channel)

                for item in tmp[key].iteritems():
                    val = item[1]
                    timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                    packer.pack_hyper(int(timestamp))
                    packer.pack_float(float(val))

                data = packer.get_buffer()
                sc.uploadData(server, auth_token, deviceId, sensor, channel, data)
            ctr = ctr + 1
