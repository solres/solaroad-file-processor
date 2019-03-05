import re
import pandas as pd
from datetime import datetime as dt
import os
import urllib.request
from pathlib import Path
import logging
from apscheduler.schedulers.background import BlockingScheduler

TABLE_LINE_PATTERN = re.compile(".*</?table|.*</?td|.*</?tr|.*</?th")
SPACE_PATTERN = re.compile("[ \t]+")
SPACES_PATTERN = re.compile("[ \t]+<")
NEWLINE_PATTERN = re.compile("\n")
TR_PATTERN = re.compile("</tr[^>]*>")
TABLE_PATTERN = re.compile("</?(table|tr)[^>]*>")
TD_TH_PATTERN = re.compile("<t[dh][^>]*>")
DIV_THEAD_PATTERN = re.compile("(^</div>|</thead>)")
TH_PATTERN = re.compile("</th>")
TD_PATTERN = re.compile("</td> *")
UNIT_PATTERN = re.compile(" Hz| W| &#176;C")
V_PATTERN = re.compile(" V,")
NL_PATTERN = re.compile(",\n")
NL_NL_PATTERN = re.compile("\\\\n")
MILLI_PATTERN = re.compile("m")
ALPHABET_PATTERN = re.compile("[A-Za-ln-z]+")
INVERTER_ID_PATTERN = re.compile("([0-9]*)-B")

PAGE_PATH = r"http://192.168.1.200/index.php/realtimedata"
APS_LOG_PATH = "C:\\Users\\ra-solaroadzwaarver\\Documents\\APS"

formatter = logging.Formatter('%(asctime)s: %(levelname)-8s - [%(name)s] %(message)s')
logger = logging.getLogger('apsDataLogger')
logger.setLevel(logging.DEBUG)


def publishAPSToCSV(df):
    invIDs = df['Inverter ID'].unique().tolist()
    dfs = dict(tuple(df.groupby('Inverter ID')))
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    for invID in invIDs:
        filename = APS_LOG_PATH + invID + "_" + today + ".csv"
        file = Path(filename)
        if file.is_file():
            args = {"mode": "a", "header": False}
        else:
            args = {"mode": "a"}
        dfs[invID].to_csv(filename, **args)


def doProcessing():

    if 'fh' in locals():
        logger.removeHandler(fh)
        fh.close()

    logfile = os.path.join('log', dt.now().strftime('aps_data_logger_log_%d_%m_%Y.log'))
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    with urllib.request.urlopen(PAGE_PATH) as response:
        logger.debug('ResponseCode = ' + str(response.getcode()))
        f = str(response.read())

    lines = ""
    for line in f.split('\\r\\n'):
        if TABLE_LINE_PATTERN.match(line):
            lines = lines + line

    lines = SPACES_PATTERN.sub("<", lines)
    lines = NEWLINE_PATTERN.sub("", lines)
    lines = TR_PATTERN.sub("\n", lines)
    lines = TABLE_PATTERN.sub("", lines)
    lines = TD_TH_PATTERN.sub("", lines)
    lines = DIV_THEAD_PATTERN.sub("", lines)
    lines = TH_PATTERN.sub(",", lines)
    lines = TD_PATTERN.sub(",", lines)
    lines = UNIT_PATTERN.sub("", lines)
    lines = V_PATTERN.sub(",", lines)
    lines = NL_PATTERN.sub("\n", lines)
    lines = NL_NL_PATTERN.sub("", lines)

    linelist = lines.split('\n')
    lt = []
    for line in linelist:
        ll = line.split(',')
        lt.append(ll)

    df = pd.DataFrame(lt)
    df.transpose
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    df = df.drop(df.index[-1])
    df['Reporting Time'][1:] = df['Reporting Time'][1]
    df.index = df['Reporting Time']
    df = df.drop('Reporting Time', axis=1)

    for index, row in df.iterrows():
        m = INVERTER_ID_PATTERN.match(row['Inverter ID'])
        if m is not None:
            invAid = m.groups()[0] + '-A'
            row['Grid Voltage'] = row['Grid Frequency']
            row['Grid Frequency'] = df.loc[df['Inverter ID'] == invAid]['Grid Frequency'].values[0]
            row['Temperature'] = df.loc[df['Inverter ID'] == invAid]['Temperature'].values[0]

    logger.debug(df)
    try:
        df
    except NameError:
        print("An error occurred while getting APS data")
    else:
        if df is not None:
            publishAPSToCSV(df)


def start():
    doProcessing()
    scheduler = BlockingScheduler()
    scheduler.add_job(doProcessing, 'interval', minutes=5)
    scheduler.start()
