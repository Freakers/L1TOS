import Symbols
import sys
import urllib.request
import urllib.response
import time
import threading
import datetime
from datetime import timedelta
from typing import Counter
import pause
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol


class TOS:
    def __init__(self):
        print("Init L1 Object")
        self.symbol = ""
        self.price = 0
        self.size = 0
        self.tos = {}
        self.low = 10000000.00
        self.high = 00.00
        self.totaltradedvolume = 00.00
        self.elapsedcounterstart = time.time()
        self.elapsedcountercurrent = time.time()
        self.elapsedtime = 00.00

    def update(self, localtime, market_time, symbol, tostype,  price, size, elapsedtime):
        msgtos = {}
        msghighlow = {}
        msgtos['LocalTime'] = localtime
        msgtos['MarketTime'] = market_time
        msgtos['Symbol'] = symbol
        msgtos['Type'] = tostype
        msgtos['Price'] = price
        msgtos['Size'] = size
        msgtos['Low'] = self.islow(price)
        msgtos['High'] = self.ishigh(price)
        msgtos['TotalTradedVolume'] = str(self.aggregatetradedvolume(size))
        msgtos['ElapsedTime'] = elapsedtime

        # print("Processing L1 Message")
        self.tos[symbol] = msgtos
        # self.list()`

    def setcurrentelapsedtime(self):
        self.elapsedcountercurrent = time.time()

    def getcurrentelapsedtime(self):
        # return elapsed time in seconds from the start of the app running
        return int(self.elapsedcountercurrent - self.elapsedcounterstart)

    def islow(self, price):
        if float(price) < self.low:
            self.low = float(price)
        return self.low

    def ishigh(self, price):
        if float(price) > self.high:
            self.high = float(price)
        return self.high

    def aggregatetradedvolume(self, size):
        self.totaltradedvolume = self.totaltradedvolume + float(size)
        return(self.totaltradedvolume)

    def list(self):
        for key, tosmessage in self.tos.items():
            print(str(tosmessage))

class L1:
    def __init__(self):
        self.numofbids = 0
        self.numofasks = 0
        self.numoftrades = 0
        self.totalbidvolume = 0
        self.totalaskvolume = 0
        self.totaldownticks = 0
        self.totalupticks = 0
        print("Init L1 Object")
        self.l1 = {}

    def update(self, symbol, bidpr, askpr, bidvol, askvol, msgtime):
        msg = {}
        self.totalbidvolume = self.totalbidvolume + int(bidvol)
        self.totalaskvolume = self.totalaskvolume + int(askvol)
        msg['msgtime'] = msgtime
        msg['bidpr'] = bidpr
        msg['askpr'] = askpr
        msg['bidvol'] = bidvol
        msg['askvol'] = askvol
        msg['totbidvol'] = self.totalbidvolume
        msg['totaskvol'] = self.totalaskvolume
        # print("Processing L1 Message")
        self.l1[symbol] = msg
        # self.list()`

    def list(self):
        print(self.l1.items().__str__())


class L2:
    def __init__(self):
        print("Init L2 Object")
        self.l2 = {}

    def update(self, symbol, side, price, volume, market_time, local_time, seqno, depth):
        msg = {}
        msg['LocalTime'] = local_time
        msg['MarketTime'] = market_time
        msg['Side'] = side
        msg['Price'] = price
        msg['Volume'] = volume
        msg['SequenceNumber'] = seqno
        msg['Depth'] = depth
        # print("Processing L1 Message")
        self.l2[symbol+side+price] = msg
        # self.list()`

    def list(self):
        print(self.l2.items().__str__())


class ppro_datagram(DatagramProtocol):
    def __init__(self, s, starttime, endtime):
        # Code to add widgets will go here...
        self.elapsedcounterstart = time.time()
        self.elapsedcountercurrent = time.time()
        self.strttime = starttime
        self.entime = endtime
        self.triggertime = endtime + datetime.timedelta(seconds=60)
        self.counter = 0
        self.zero = 00.00
        self.rttos = TOS()
        #self.level1 = L1()
        self.bidpr = ""
        self.askpr = ""
        self.asksize = ""
        self.bidsize = ""
        self.avgneutrals = 00.00
        self.avgneutralstotal = 00.00
        self.asks = 0
        self.avgask = 0
        self.avgasktotal = 0
        self.avgbid = 0
        self.avgbidtotal = 0
        self.bids = 0
        self.neutrals = 0
        self.time = ""
        self.this_symbol = str(s)
        self.symbol = ""
        self.starttime = time.time()
        self.min = 00.00
        self.max = 00.00

    def setcurrentelapsedtime(self):
        self.elapsedcountercurrent = time.time()

    def getcurrentelapsedtime(self):
        # return elapsed time in seconds from the start of the app running
        return int(self.elapsedcountercurrent - self.elapsedcounterstart)

    def startProtocol(self):
        # code here what you want to start upon listener creation..
        # I use this space to connect to my logging backend and inter process communication library
        print('starting up..')

    def datagramReceived(self, data, addr):
        # Code to add widgets will go here...
        #self.elapsedtime()
        # decode byte data from UDP port into string, and replace spaces with NONE
        msg = data.decode("utf-8").replace(' ', 'NONE')

        # empty dict we will populate with the string data
        message_dict = {}

        # when processing PPro8 data feeds, processing the line into a dictionary is very useful:
        for item in msg.split(','):
            #print(item)
            if "=" in item:
                couple = item.split('=')
                message_dict[couple[0]] = couple[1]
        # print(message_dict.__str__())
        # now you can call specific data by name in the line you're processing instead of counting colums
        # See the print statement below for examples

        if message_dict['Message'] == "TOS" and datetime.datetime.now() >= self.strttime and datetime.datetime.now() < self.entime:
            self.symbol = message_dict['Symbol']
            if self.symbol == self.this_symbol.__str__():
                #print("This_Symbol      : " + self.this_symbol)
                #print("In TOS for Symbol: " + message_dict['Symbol'])
            #     print('{}\t{}\t{}'.format(message_dict['Symbol'], message_dict['Message'], msg))
            #     # When the time of sale appears update the current elapsed time
                self.setcurrentelapsedtime()
            #     def update(self, localtime, market_time, symbol, tostype,  price, size):
                self.rttos.update(message_dict['LocalTime'],
                                  message_dict['MarketTime'],
                                  message_dict['Symbol'],
                                  message_dict['Type'],
                                  message_dict['Price'],
                                  message_dict['Size'],
                                  datetime.timedelta(0, self.getcurrentelapsedtime()).__str__().rjust(8, ' '))
                #self.rttos.list()
                if float(message_dict['Price']) > float(self.max):
                    self.max = float(message_dict['Price'])
                if float(message_dict['Price']) < float(self.min) or float(self.min) == float("00.00"):
                    self.min = float(message_dict['Price'])
                print("Max Price = " + str(self.max) + "  Min Price = " + str(self.min))
        else:
            if str(datetime.datetime.now()) < str(self.triggertime):
                print("Current Price: " + message_dict['Price'] + " Max Price = " + str(self.max) + "  Min Price = " + str(self.min))
                if self.symbol == self.this_symbol.__str__() and float(message_dict['Price']) > self.max:
                    print("Triggered through upside")
                if self.symbol == self.this_symbol.__str__() and float(message_dict['Price']) < self.min:
                    print("Triggered through downside")
            else:
                print("No Trigger through MIN or MAX for " + self.symbol)

    def connectionRefused(self):
        print("No one listening")

if len(sys.argv) > 1:
    my_symbol = sys.argv[1].__str__()
    print("\nStarting L1TOS monitor for symbol: " + my_symbol.__str__())
    # Load and register symbols of intrest
    Symbols.Symbols(my_symbol, "TOS", "5556", "symbol")
    # Note: If the _SYMBOL_ is omitted it will default to ES\U19.CM
    #pause.until(datetime(n.year, n.month, n.day, 14, 30, 0, 0))

n = datetime.datetime.now()
print(n.year.__str__()+n.month.__str__()+n.day.__str__())
reactor.listenUDP(5556, ppro_datagram(my_symbol,
                                      datetime.datetime(n.year, n.month, n.day, 1, 0, 0, 0),
                                      datetime.datetime(n.year, n.month, n.day, 18, 0, 0, 0)))
reactor.run()
