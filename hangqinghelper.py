# !/usr/bin/python
# -*- encoding:utf-8 -*-

import os
import numpy as np
import pandas as pd
from struct import *
import re
import time
import requests

'''
This helper can help transfer tongdaxin .day files into .csv files!

'''
SIDI_MARKET_URL='http://c.4006543218.cn:8888/market/json?'

def dayfilelists(path):
	dirpath = ''
	file_names = []
	filelists = []
	for root,dirs,files in os.walk(path):
		dirpath = root+'/'
		file_names = files
	for file in file_names:
		if file.split('.')[1] == 'day':
			filelists.append(dirpath+file)

	return filelists


def exactStock(fileName,outputDir=None):
	market_code = fileName.split('/')[-1].split('.')[0]	#sh600001
	pattern = re.compile(r'\d+')
	code = pattern.search(market_code).group()
	pattern2 = re.compile(r'[^\d]+')
	market = pattern2.search(market_code).group()
	if outputDir is None:
		outputPath = os.path.split(fileName)[0]+'/'+code+'.csv'
	else:
		outputPath = outputDir+'/'+code+'.csv'
	print '[tdx day to csv] '+code+' start...'
	ofile = open(fileName,'rb')
	buf = ofile.read()
	ofile.close()
	num = len(buf)
	no = num/32
	b = 0
	e = 32
	items = list()
	dates = list()
	for i in range(int(no)):
		a = unpack('IIIIIfII',buf[b:e])
		year = int(a[0]/10000)
		m = int((a[0]%10000)/100)
		month = str(m)
		if m<10:
			month = '0'+month
		d = (a[0]%10000)%100
		day = str(d)
		if d<10:
			day = '0'+day
		dd = str(year)+'-'+month+'-'+day
		openPrice = a[1]/100.0
		high = a[2]/100.0
		low = a[3]/100.0
		close = a[4]/100.0
		amount = a[5]/100.0
		vol = a[6]
		unused = a[7]
		dates.append(dd)
		item = [code+'.'+market,str(openPrice),str(high),str(low),str(close),str(amount),str(vol)]
		items.append(item)
		b = b+32
		e = e+32

	df = pd.DataFrame(items,columns=['code','open','high','low','close','amount','vol'],index=dates)
	df.to_csv(outputPath,encoding='utf-8')
	print '[tdx day to csv]',code,'finished'

def getMarketCode(stockCode):
	firstNumer = stockCode[0:1]
	if firstNumer == '0' or firstNumer =='3':
		market_code = 'SZ'
	elif firstNumer == '6':
		market_code = 'SH'
	else:
		market_code = False
	return market_code

def getSidiMarket(stockCode,field='22:24:2:10:11:9:12:14:6:23:21:3:1:32',funcno='20000'):
	market_code = getMarketCode(stockCode)
	if not market_code:
		return False
	url = SIDI_MARKET_URL+'funcno='+funcno+'&version=1&stock_list='+market_code+':'+stockCode+'&field='+field
	response = requests.get(url)
	return response

# qtype:[day|week|month]
def getHistoryMarket(stockCode,qtype,count,funcno='20002'):
	market_code = getMarketCode(stockCode)
	if not market_code:
		return False
	url = SIDI_MARKET_URL+'funcno='+funcno+'&version=1&stock_code='+stockCode+'&market='+market_code+'&type='+qtype+'&count='+count
	response = requests.get(url)
	return response

if __name__ == '__main__':
	start = time.time()
	sh_rootdir = 'vipdoc/sh/lday'

	sh_filelists = dayfilelists(sh_rootdir)

	for data in sh_filelists:
		exactStock(data,'database')

	sz_rootdir = 'vipdoc/sz/lday'
	sz_filelists = dayfilelists(sz_rootdir)
	for data in sz_filelists:
	    exactStock(data,'database')

	end = time.time()
	print '[tdx day to csv] Total process time:',round(end-start,2),'s'
	