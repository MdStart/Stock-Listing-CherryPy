# -*- coding: utf-8 -*-

# import os

import cherrypy
import redis
import json

from mako.template import Template
from mako.lookup import TemplateLookup
import zipfile
import urllib
import os
from urllib.request import Request, urlopen, urlretrieve
import pandas as pd

# from collections import OrderedDict

root_dir = os.path.abspath(os.path.dirname(__file__))

# path   = os.path.abspath(os.getcwd())

config = {'global': {'server.socket_host': '0.0.0.0',
          'server.socket_port': 1919, 'server.thread_pool': 8}}


# lookup = TemplateLookup(directories=[os.path.join(path, 'view')])

def url_creator(date):
    """
    Input is list i.e. enter value as 13 11 17 to get as [13,11,17] in url_creator input
    """

    (dd, mm, yy) = date

    return 'http://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{0:}{1:02d}{2:}.zip'.format(dd,
            mm, yy)


# print("Input is list i.e. Enter value as 13 11 17 to get as [13,11,17] for the url_creator function argument passing:  ")
# dat=  [int(x) for x in input().split()]

dat = [13, 11, 17]
try:
    url_zip = url_creator(dat)
    print 'Successfully created url for the zip url with desied date as input '
except:
    print 'No such data for the input dat or url link has changed'
(zip_url, headers) = urllib.request.urlretrieve(url=url_zip)
zip_ref = zipfile.ZipFile(file=zip_url, mode='r')
try:
    zip_ref.extractall(path=os.getcwd())  # os.getcwd() directs to current working directory
    print 'Successfully extracted csv file from zip url'
except:
    print 'failed to extract csv file from zip url'
zip_ref.close()

# print("\n")

print '============================================================='
print 'Extracted csv file name would be like EQ_ISINCODE_ddmmyy.CSV i.e. for input as 13 11 17 csv filename would be EQ_ISINCODE_131117.CSV'

# print("\n")
# print("=============================================================")

# print("csv Data insertion to redis DB as key-value")

# print("Creating Dataframe from csv Data & filterting, altering data to desired data for redis")

df = pd.read_csv(root_dir, '/EQ_ISINCODE_131117.CSV')
col = [
    'code',
    'name',
    'open',
    'high',
    'low',
    'close',
    ]
dfc = df[[
    'SC_CODE',
    'SC_NAME',
    'OPEN',
    'HIGH',
    'LOW',
    'CLOSE',
    ]].copy()
dfc.columns = col

# data_dict=dfc.to_dict('record', into=OrderedDict)  # orient as record ignore the storing of index of values in the dict OrderedDict used to maintain the order

data_dict = dfc.to_dict('record')

# Redis DB config- Redis must be installed in the system before that

con = redis.Redis(host='localhost', port=6379, db=3)

# Data saving & retrieving to & from Redis

try:
    for data in data_dict:
        dat = json.dumps(data)
        con.set(data['code'], dat)
    print "Data inserted to Redis localhost'"
except:
    print "Data insertion failed to Redis localhost'"

lookup = TemplateLookup(directories=[os.path.join(root_dir, 'view')])


class Stock(object):

    @cherrypy.expose
    def index(self):
        outdata = []
        for data in data_dict:
            dt = con.get(data['code'])
            dat = dt.decode('utf-8')
            res = json.loads(dat)
            outdata.append(res)
        output = lookup.get_template('home.html'
                ).render(outdata=outdata)
        return output


if __name__ == '__main__':
    cherrypy.quickstart(Stock(), '/', config)