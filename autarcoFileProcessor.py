import logging
import xdrlib
import numpy as np
import pandas as pd
import sensorcloud as sc

SENSOR_NAME = 'Autarco'


def processAutarcoFile(file):
    logger = logging.getLogger('solaroad.autarco')
    x = pd.read_csv(file, delimiter=';', skip_blank_lines=True)
    index_time = pd.to_datetime(x['time'], format="%Y-%m-%d %H:%M:%S")
    x.index = index_time
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    x = x.dropna()

    for device in x['device'].unique():
        logger.debug('======================== Now processing %s ========================', device)
        y = x.loc[x['device'] == device]
        del (y['device'])
        del (y['time'])

        ctr = 0
        total_steps = np.round(len(y) / sc.MAX_POINTS) + 1
        while ctr < total_steps:
            server, auth_token = sc.authenticate()
            deviceId = sc.getDeviceId()

            sp = ctr * sc.MAX_POINTS
            tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
            logger.debug('--------------------- RECORD %s/%s of %s ------------------------------', ctr + 1,
                         total_steps, device)

            if ctr == 0:
                sc.addSensor(server, auth_token, deviceId, device, SENSOR_NAME, device, device)

            logger.debug('Now uploading ' + device)

            for parameter in tmp['parameter'].unique():
                param_vals = tmp.loc[tmp['parameter'] == parameter]
                del (param_vals['parameter'])
                param_vals = param_vals.resample('300S').mean().interpolate(method='linear')

                packer = xdrlib.Packer()
                packer.pack_int(1)  # version 1

                packer.pack_enum(sc.SECONDS)
                packer.pack_int(300)

                POINTS = len(param_vals)
                packer.pack_int(POINTS)

                channel = '_'.join(parameter.replace('(', '').replace(')', '').split(' '))
                if ctr == 0:
                    sc.addChannel(server, auth_token, deviceId, device, channel, channel, channel)

                for item in param_vals['value'].iteritems():
                    val = item[1]
                    timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                    packer.pack_hyper(int(timestamp))
                    packer.pack_float(float(val))

                data = packer.get_buffer()
                sc.uploadData(server, auth_token, deviceId, device, channel, data)
            ctr = ctr + 1
