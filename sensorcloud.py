'''
Created on 26 Jun 2017

@author: subramaniana
'''
import base64
import http.client
import logging
import xdrlib
import configparser
import urllib.parse as urlparse

AUTH_SERVER = 'sensorcloud.microstrain.com'
USERNAME = 'username'
PASSWORD = 'password'
MAX_POINTS = 100000

# samplerate types
HERTZ = 1
SECONDS = 0


config = configparser.ConfigParser()
config.read('config.ini')
logger = logging.getLogger('sensorcloud.internal')

def getDeviceId():
    return config['SensorCloud']['device_id']

def authenticate():
    '''
    authenticates with username and password read off a config file
    '''
    password = base64.urlsafe_b64decode(config['SensorCloud']['password']).decode('utf-8')
    username = urlparse.quote(config['SensorCloud']['username'])
    deviceId = config['SensorCloud']['device_id']

    return authenticateAlternate(deviceId, username, password)


def authenticateKey(device_id, key):
    '''
    authenticate with sensorcloud and get the server and auth_key for all subsequent api requests
    '''
    conn = http.client.HTTPSConnection(AUTH_SERVER)

    headers = {'Accept': 'application/xdr'}
    url = '/SensorCloud/devices/%s/authenticate/?version=1&key=%s' % (device_id, key)

    logger.debug('Authenticating...')
    conn.request('GET', url=url, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 200 ok then we can parse the response to get the auth token and server
    if response.status is 200:
        logger.debug('Credential are correct')

        # read the body of the response
        data = response.read()

        # response will be in xdr format. Create an XDR unpacker and extract the token and server as strings
        unpacker = xdrlib.Unpacker(data)
        auth_token = unpacker.unpack_string().decode('utf-8')
        server = unpacker.unpack_string().decode('utf-8')

        logger.debug('unpacked xdr.  server:%s  token:%s' % (server, auth_token))

        return server, auth_token


def authenticateAlternate(device_id, username, password):
    '''
    authenticate with sensorcloud and get the server and auth_key for all subsequent api requests
    '''
    conn = http.client.HTTPSConnection(AUTH_SERVER)

    headers = {'Accept': 'application/xdr'}
    url = '/SensorCloud/devices/%s/authenticate/?version=1&username=%s&password=%s' % (device_id, username, password)

    logger.debug('authenticating...')
    conn.request('GET', url=url, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 200 ok then we can parse the response to get the auth token and server
    if response.status is 200:
        logger.debug('Credential are correct')

        # read the body of the response
        data = response.read()

        # response will be in xdr format. Create an XDR unpacker and extract the token and server as strings
        unpacker = xdrlib.Unpacker(data)
        auth_token = unpacker.unpack_string().decode('utf-8')
        server = unpacker.unpack_string().decode('utf-8')

        logger.debug('unpacked xdr.  server:%s  token:%s' % (server, auth_token))

        return server, auth_token


def addSensor(server, auth_token, device_id, sensor_name, sensor_type='', sensor_label='', sensor_desc=''):
    '''
    Add a sensor to the device. type, label, and description are optional.
    '''

    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/%s/?version=1&auth_token=%s' % (device_id, sensor_name, auth_token)

    headers = {'Content-type': 'application/xdr'}

    # addSensor allows you to set the sensor type label and description.  All fileds are strings.
    # we need to pack these strings into an xdr structure
    packer = xdrlib.Packer()
    packer.pack_int(1)  # version 1
    packer.pack_string(sensor_type.encode('utf-8'))
    packer.pack_string(sensor_label.encode('utf-8'))
    packer.pack_string(sensor_desc.encode('utf-8'))
    data = packer.get_buffer()

    logger.debug('adding sensor... %s', sensor_name)
    conn.request('PUT', url=url, body=data, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 201 created then we know the sensor was added
    if response.status is 201:
        logger.debug('Sensor added')
    else:
        logger.warning('Error adding sensor. Error: %s', response.read())


def updateSensor(server, auth_token, device_id, sensor_name, sensor_type='', sensor_label='', sensor_desc=''):
    '''
    Add a sensor to the device. type, label, and description are optional.
    '''

    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/%s/?version=1&auth_token=%s' % (device_id, sensor_name, auth_token)

    headers = {'Content-type': 'application/xdr'}

    # addSensor allows you to set the sensor type label and description.  All fileds are strings.
    # we need to pack these strings into an xdr structure
    packer = xdrlib.Packer()
    packer.pack_int(1)  # version 1
    packer.pack_string(sensor_type.encode('utf-8'))
    packer.pack_string(sensor_label.encode('utf-8'))
    packer.pack_string(sensor_desc.encode('utf-8'))
    data = packer.get_buffer()

    logger.debug('updating sensor... %s', sensor_name)
    conn.request('POST', url=url, body=data, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 201 created then we know the sensor was added
    if response.status is 201:
        logger.debug('Sensor updated')
    else:
        logger.warning('Error updating sensor. Error: %s', response.read())


def addChannel(server, auth_token, device_id, sensor_name, channel_name, channel_label='', channel_desc=''):
    '''
    Add a channel to the sensor.  label and description are optional.
    '''

    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/%s/channels/%s/?version=1&auth_token=%s' % (
        device_id, sensor_name, channel_name, auth_token)

    headers = {'Content-type': 'application/xdr'}

    # addChannel allows you to set the channel label and description.  All fileds are strings.
    # we need to pack these strings into an xdr structure
    packer = xdrlib.Packer()
    packer.pack_int(1)  # version 1
    packer.pack_string(channel_label.encode('utf-8'))
    packer.pack_string(channel_desc.encode('utf-8'))
    data = packer.get_buffer()

    logger.debug('adding channel... %s', channel_name)
    conn.request('PUT', url=url, body=data, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 201 created then we know the channel was added
    if response.status is 201:
        logger.debug('Channel successfuly added')
    else:
        logger.warning('Error adding channel.  Error: %s', response.read())


def uploadData(server, auth_token, device_id, sensor_name, channel_name, data):
    '''
    Upload specified data
    '''

    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/%s/channels/%s/streams/timeseries/data/?version=1&auth_token=%s' % (
        device_id, sensor_name, channel_name, auth_token)

    logger.debug('adding data...')
    headers = {'Content-type': 'application/xdr'}
    conn.request('POST', url=url, body=data, headers=headers)
    response = conn.getresponse()
    logger.debug('%s:%s',response.status, response.reason)

    # if response is 201 created then we know the channel was added
    if response.status is 201:
        logger.debug('data successfully added')
    else:
        logger.error('Error adding data.  Error: %s', response.read())


def downloadData(server, auth_token, device_id, sensor_name, channel_name, startTime, endTime):
    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/%s/channels/%s/streams/timeseries/data/?version=1&auth_token=%s&starttime' \
          '=%s&endtime=%s' % (device_id, sensor_name, channel_name, auth_token, startTime, endTime)
    headers = {'Accept': 'application/xdr'}
    logger.debug('Downloading data...')
    conn.request('GET', url=url, headers=headers)
    response = conn.getresponse()
    data = []
    if response.status is 200:
        logger.debug('Data retrieved')
        unpacker = xdrlib.Unpacker(response.read())
        while True:
            try:
                timestamp = unpacker.unpack_uhyper()
                value = unpacker.unpack_float()
                data.append((timestamp, value))
            except Exception as e:
                logger.error(e)
                break
        return data
    else:
        logger.error('Status: %s' % response.status)
        logger.error('Reason: %s' % response.reason)
        return data


def getSensors(server, auth_token, device_id):
    '''
    Download the Sensors and Channel information for the Device.
    Packs into a dict for easy con
    '''
    conn = http.client.HTTPSConnection(server)

    url = '/SensorCloud/devices/%s/sensors/?version=1&auth_token=%s' % (device_id, auth_token)
    headers = {'Accept': 'application/xdr'}
    conn.request('GET', url=url, headers=headers)
    sensors = {}
    response = conn.getresponse()
    if response.status is 200:
        logger.debug('Data Retrieved')
        unpacker = xdrlib.Unpacker(response.read())
        # unpack version, always first
        unpacker.unpack_int()
        # sensor info is an array of sensor structs.  In XDR, first you read an int, and that's the number of items
        # in the array.  You can then loop over the number of elements in the array
        numSensors = unpacker.unpack_int()
        for i in range(numSensors):
            sensorName = unpacker.unpack_string().decode('utf-8')
            sensorType = unpacker.unpack_string().decode('utf-8')
            sensorLabel = unpacker.unpack_string().decode('utf-8')
            sensorDescription = unpacker.unpack_string().decode('utf-8')
            # using sensorName as a key, add info to sensor dict
            sensors[sensorName] = {'name': sensorName, 'type': sensorType, 'label': sensorLabel,
                                   'description': sensorDescription, 'channels': {}}
            # channels for each sensor is an array of channelInfo structs.  Read array length as int, then loop
            # through the items
            numChannels = unpacker.unpack_int()
            for j in range(numChannels):
                channelName = unpacker.unpack_string().decode('utf-8')
                channelLabel = unpacker.unpack_string().decode('utf-8')
                channelDescription = unpacker.unpack_string().decode('utf-8')
                # using channel name as a key, add info to sensor's channel dict
                sensors[sensorName]['channels'][channelName] = {'name': channelName, 'label': channelLabel,
                                                                'description': channelDescription, 'streams': {}}
                # dataStreams for each channel is an array of streamInfo structs, Read array length as int,
                # then loop through the items
                numStreams = unpacker.unpack_int()
                for k in range(numStreams):
                    # streamInfo is a union, where the type indicates which stream struct to use.  Currently we only
                    # support timeseries version 1, so we'll just code for that
                    streamType = unpacker.unpack_string().decode('utf-8')
                    if streamType == 'TS_V1':
                        # TS_V1 means we have a timeseriesInfo struct total bytes allows us to jump ahead in our
                        # buffer if we're uninterested in the units.  For verbosity, we will parse them.
                        total_bytes = unpacker.unpack_int()
                        # units for each data stream is an array of unit structs.  Read array length as int,
                        # then loop through the items
                        numUnits = unpacker.unpack_int()
                        # add TS_V1 to streams dict
                        sensors[sensorName]['channels'][channelName]['streams']['TS_V1'] = {'units': {}}
                        for l in range(numUnits):
                            storedUnit = unpacker.unpack_string().decode('utf-8')
                            preferredUnit = unpacker.unpack_string().decode('utf-8')
                            unitTimestamp = unpacker.unpack_uhyper()
                            slope = unpacker.unpack_float()
                            offset = unpacker.unpack_float()
                            # using unitTimestamp as a key, add unit info to unit dict
                            sensors[sensorName]['channels'][channelName]['streams']['TS_V1']['units'][
                                str(unitTimestamp)] = {'stored': storedUnit,
                                                       'preferred': preferredUnit, 'unitTimestamp': unitTimestamp,
                                                       'slope': slope, 'offset': offset}
    return sensors
