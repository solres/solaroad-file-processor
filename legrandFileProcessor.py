import logging
import xdrlib
import pandas as pd
import sensorcloud as sc

CHANNEL_NAME = 'kWh'

def processLeGrandFile(file):
    if 'Devices' not in file:
        return

    logger = logging.getLogger('solaroad.legrand')

    sensorName = 'LeGrand'

    x = pd.read_csv(file, delimiter=';', skip_blank_lines=True)

    if isinstance(x.index, pd.RangeIndex):
        x.columns = ['Date', 'kWh']
        index_time = pd.to_datetime(x['Date'], format='%m/%d/%Y %I:%M:%S %p')
        del(x['Date'])
    else:
        x.columns = ['kWh', 'kVarh']
        index_time = pd.to_datetime(x.index, format='%m/%d/%Y %I:%M:%S %p')
        x['kVarh'] = pd.to_numeric(x['kVarh'])

    x.index = index_time
    x['kWh'] = pd.to_numeric(x['kWh'])
    for key in x.keys():
        if 'Unnamed' in key:
            del (x[key])
    x = x.dropna()

    # Authenticate
    server, auth_token = sc.authenticate()
    deviceId = sc.getDeviceId()

    # Add Sensor
    logger.debug('======================== Now processing LeGrand %s ========================', sensorName)
    sc.addSensor(server, auth_token, deviceId, sensorName, sensorName, sensorName, sensorName)

    # Pre-processing
    y = x.resample('12H').mean().interpolate(method='linear')

    # Break DataFrame into chunks of 100k
    ctr = 0
    total_steps = round(len(x) / sc.MAX_POINTS) + 1
    while ctr < total_steps:
        sp = ctr * sc.MAX_POINTS
        tmp = y.iloc[sp:sp + sc.MAX_POINTS - 1, :]
        logger.debug('--------------------- RECORD %s/%s ------------------------------', ctr + 1, total_steps)
        for key in tmp.keys():
            packer = xdrlib.Packer()
            packer.pack_int(1)  # version 1

            packer.pack_enum(sc.SECONDS)
            packer.pack_int(43200)

            POINTS = len(tmp[key])
            packer.pack_int(POINTS)

            logger.debug('Now uploading %s', key)

            if ctr == 0:
                sc.addChannel(server, auth_token, deviceId, sensorName, CHANNEL_NAME, CHANNEL_NAME, CHANNEL_NAME)

            for item in tmp[key].iteritems():
                val = item[1]
                timestamp = item[0].to_pydatetime().timestamp() * 1000000000
                packer.pack_hyper(int(timestamp))
                packer.pack_float(float(val))

            data = packer.get_buffer()
            sc.uploadData(server, auth_token, deviceId, sensorName, CHANNEL_NAME, data)
        ctr = ctr + 1
