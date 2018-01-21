## -*- coding: utf-8 -*-
#import os
import cherrypy
import redis
import json

from mako.template import Template
#from mako.lookup import TemplateLookup
import zipfile, urllib, os
from urllib.request import Request,urlopen, urlretrieve
import pandas as pd

#from collections import OrderedDict


#path   = os.path.abspath(os.getcwd())
config = {
  'global' : {
    'server.socket_host' : '127.0.0.1',
    'server.socket_port' : 8080,
    'server.thread_pool' : 8
  }
}

#lookup = TemplateLookup(directories=[os.path.join(path, 'view')])



def url_creator(date):
    """
    Input is list i.e. enter value as 13 11 17 to get as [13,11,17] in url_creator input
    """
    dd, mm, yy = date

    return 'http://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{0:}{1:02d}{2:}.zip'.format(dd,
            mm, yy)
#print("Input is list i.e. Enter value as 13 11 17 to get as [13,11,17] for the url_creator function argument passing:  ") 
#dat=  [int(x) for x in input().split()]
dat=[13,11,17]
try:
    url_zip= url_creator(dat)
    print("Successfully created url for the zip url with desied date as input ")
except:
    print("No such data for the input dat or url link has changed")
zip_url, headers = urllib.request.urlretrieve(url = url_zip)
zip_ref = zipfile.ZipFile(file = zip_url, mode = 'r')
try:
    zip_ref.extractall(path = os.getcwd())     #os.getcwd() directs to current working directory
    print("Successfully extracted csv file from zip url")
except:
    print("failed to extract csv file from zip url")
zip_ref.close()
print("\n")
print("=============================================================")
print("Extracted csv file name would be like EQ_ISINCODE_ddmmyy.CSV i.e. for input as 13 11 17 csv filename would be EQ_ISINCODE_131117.CSV")
print("\n")
print("=============================================================")

print("csv Data insertion to redis DB as key-value")


print("Creating Dataframe from csv Data & filterting, altering data to desired data for redis")

df = pd.read_csv("C:\Python34\EQ_ISINCODE_131117.CSV")
col=["code", "name", "open", "high", "low", "close"]
dfc= df[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()
dfc.columns =col

#data_dict=dfc.to_dict('record', into=OrderedDict)  # orient as record ignore the storing of index of values in the dict OrderedDict used to maintain the order 
data_dict=dfc.to_dict('record')

#Redis DB config- Redis must be installed in the system before that

con = redis.Redis(host='localhost', port=6379, db=3)

#Data saving & retrieving to & from Redis 
"""
 data['code'] is used as string to reference each dictionary data , json format to save data into redis as json data with json.dumps
 & retrieve using json.loads but as the data into redis saves as bytes & json.loads works for str format data , So in python 3 need to convert bytes stream to str
 using decode('utf-8') before passing data to  json.loads 
Note into= OrderedDict this code won't work as data_dict saving data list of dict format
"""
try:    
    for data in data_dict:
        dat=json.dumps(data)
        con.set(data['code'],dat)
    print("Data inserted to Redis localhost'")
except:
    print("Data insertion failed to Redis localhost'")

class Stock(object):
    @cherrypy.expose
    def index(self):
        outdata=[]
        for data in  data_dict:
                    dt=con.get(data['code'])
                    dat=dt.decode('utf-8')
                    res=json.loads(dat)
                    outdata.append(res)
        output = Template(filename='home.html').render(outdata = outdata)
        return output


if __name__ == '__main__':
    cherrypy.quickstart(Stock(),'/', config)


