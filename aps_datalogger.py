#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import pandas as pd
import numpy as np
import datetime
import win32gui
import win32con
import win32api
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

READING_COORDINATES = "\"636 257 730 412\""
AGILENT_WINDOW_TITLE = 'MECLAB_3 - BenchLink Data Logger 3'
PAGE_PATH = r"http://192.168.1.200/index.php/realtimedata"
CMD = "C:\\Users\\ra-solaroadzwaarver\\Downloads\\Arun\\Capture2Text\\Capture2Text_CLI.exe --screen-rect %s" % READING_COORDINATES
AGILENT_LOG_PATH = "C:\\Users\\ra-solaroadzwaarver\\Downloads\\Arun\\Agilent\\"
APS_LOG_PATH = "C:\\Users\\ra-solaroadzwaarver\\Downloads\\Arun\\APS\\"
COLS = ['Timestamp', '101<C L1>', '102<A L2>', '108<A T2>', '109<C T5>', '110<A T6>', '111<C T8>', '112<A T7>',
        '113<B HD T9>']


def click(x, y):
    win32api.SetCursorPos((x, y))
    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, x, y, 0, 0)
    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, x, y, 0, 0)


def process_text(hwnd):
    try:
        win32gui.ShowWindow(hwnd, win32con.SW_MAXIMIZE)
        click(388, 122)
        text = os.popen(CMD).read()
        orig_text = text
        win32gui.ShowWindow(hwnd, win32con.SW_MINIMIZE)

        text = ALPHABET_PATTERN.sub(",", text)
        text = MILLI_PATTERN.sub("e-3", text)
        text = SPACE_PATTERN.sub("", text)
        text = NL_PATTERN.sub("", text)
        text = str(datetime.datetime.now()) + ',' + text

        df2 = pd.DataFrame(columns=COLS)
        df2.loc[0] = text.split(',')
        df2.index = df2['Timestamp']
        df2 = df2.drop('Timestamp', axis=1)
        return df2
    except ValueError:
        print("An error occurred trying to parse "+ orig_text)


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

def publishAgilentToCSV(df2):
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    ag_filename = AGILENT_LOG_PATH + today + ".csv"
    file = Path(ag_filename)
    if file.is_file():
        args = {"mode": "a", "header": False}
    else:
        args = {"mode": "a"}
    df2.to_csv(ag_filename, **args)


def doProcessing():
    print(pd.Timestamp.now())
    # hwnd = win32gui.FindWindow(None, AGILENT_WINDOW_TITLE)
    #if hwnd is not 0:
    #    df2 = process_text(hwnd)

    # f = open(PAGE_PATH)

    with urllib.request.urlopen(PAGE_PATH) as response:
        f = str(response.read())

    print(f)

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

    try:
        df
    except NameError:
        print("An error occurred while getting APS data")
    else:
        if df is not None:
            publishAPSToCSV(df)

    # try:
    #    df2
    #except NameError:
    #    print("An error occurred while getting Agilent data")
    #else:
    #    if df2 is not None:
    #        publishAgilentToCSV(df2)

doProcessing()
scheduler = BlockingScheduler()
scheduler.add_job(doProcessing, 'interval', minutes=5)
scheduler.start()