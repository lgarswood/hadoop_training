#!/usr/bin/python

import csv
import socket
from time import sleep, strftime, gmtime
from random import randint, gauss
from datetime import datetime, timedelta

#productNames = ['cheese', 'hammock', 'car', 'hammer', 'wood', 'phone']
#productCategories = ['automotive', 'food', 'utilities', 'materials', 'technology']

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
out = type('', tuple([object]), {'write': (lambda x, y: sock.send(y))})()
writer = csv.writer(out, delimiter=',', quotechar='"')
now = datetime.utcnow()

sock.connect(('localhost', 44444))

while(True):
    #product = choice(productNames)
    category = 'category' + str(randint(1, 20))
    product = category + '_' + 'product' + str(randint(1, 20))

    price = -1
    while price < 0:
        price = gauss(100, 50)

    purchaseDate = (now + timedelta(randint(-7, -1))).strftime('%Y-%m-%d')
    # mean at noon UTC
    secondsInDay = -1
    while not 0 <= secondsInDay < 86400:
        secondsInDay = int(round(gauss(43200, 10800)))
    purchaseTime = strftime('%H:%M:%S', gmtime(secondsInDay))
    purchaseDateTimeString = ' '.join((purchaseDate, purchaseTime))

    #category = choice(productCategories)

    ip = '.'.join(str(randint(0, 255)) for x in range(4))

    #purchaseDateTime = datetime.strptime(purchaseDateTimeString, '%Y-%m-%d %H:%M:%S')
    #timestamp = int(mktime(purchaseDateTime.timetuple())) * 1000
    writer.writerow([product, str(round(price, 2)), purchaseDateTimeString, category, ip])
    sock.recv(1024)
    sleep(0.04)

sock.close()
