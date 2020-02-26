import sys
import urllib.request
import urllib.response
import time
import threading
import datetime
from typing import Counter

from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol

class UDPReader:
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

    class TOS:

        def __init__(self):
            self.tos = {}
            self.sqno = UDPReader.seqno()
            self.LocalTime = ""
            self.Message = ""
            self.MarketTime = ""
            self.Symbol = ""
            self.Type = ""
            self.Price = ""
            self.Size = ""
            self.Source = ""
            self.Condition = ""
            self.Tick = ""
            self.Mmid = ""
            self.SubMarketId = ""
            self.Date = ""
            self.BuyerId = ""
            self.SellerId = ""
            self.seqno = 0

        def update(self, tos_message):
            msg = tos_message
            self.local_time = msg['LocalTime']
            self.Message = msg['Message']
            self.MarketTime = msg['MarketTime']
            self.Symbol = msg['Symbol']
            self.Type = msg['Type']
            self.Price = msg['Price']
            self.Size = msg['Size']
            self.Source = msg['Source']
            self.Condition = msg['Condition']
            self.Tick = msg['Tick']
            self.Mmid = msg['Mmid']
            self.SubMarketId = msg['SubMarketId']
            self.Date = msg['Date']
            self.BuyerId = msg['BuyerId']
            self.SellerId = msg['SellerId']
            self.tos[(self.sqno.get(self.Symbol))] = msg

        def list(self):
            for key, record in self.tos.items():
                print("Key: " + str(key) + ":" + str(record))

    class seqno:
        def __init__(self):
            self.key = {}

        def get(self, symbol=""):
            if symbol not in self.key:
                self.key[symbol] = 0
            # Get current seqno for symbol
            ret = self.key[symbol]
            self.key[symbol] = ret + 1
            return str(symbol+str(ret))


    class SellNYSE:
        """Submit NASDAQ Market order to sell based on the symbol and size"""
        def __init__(self, symbol="ACB.NY", shares="10"):
            print("Sell " + shares + " @ Market: " + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=NSDQ%20Sell->Short%20NSDQ%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("NSDQ - Execute Order Response : " + html1.__str__())

    class BuyNYSE:
        """Submit NASDAQ Market order to buy based on the symbol and size"""
        def __init__(self, symbol="ACB.NY", shares="10"):
            print("Buy " + shares + " @ Market: " + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=NSDQ%20Buy%20NSDQ%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("NSDQ - Execute Order Response : " + html1.__str__())

    class SellNasdaq:
        """Submit NASDAQ Market order to sell based on the symbol and size"""
        def __init__(self, symbol="AAPL.NQ", shares="10"):
            print("Sell " + shares + " @ Market: " + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=NSDQ%20Sell->Short%20NSDQ%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("NSDQ - Execute Order Response : " + html1.__str__())

    class BuyNasdaq:
        """Submit NASDAQ Market order to buy based on the symbol and size"""
        def __init__(self, symbol="AAPL.NQ", shares="10"):
            print("Buy " + shares + " @ Market: " + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=NSDQ%20Buy%20NSDQ%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("NSDQ - Execute Order Response : " + html1.__str__())

    class SellFutures:
        """Submit Futures Contract to sell based on the symbol and contract size, default is ES|M19.CM 1 Contract"""
        def __init__(self, symbol="MES\\H20.CM", shares="1"):
            print("Sell " + shares + " Contract Market:" + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=CME%20Sell%20CME%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("API - Execute Order Response : " + html1.__str__())

    class BuyFutures:
        """Submit Futures Contract to buy based on the symbol and contract size, default is ES|M19.CM 1 Contract"""
        def __init__(self, symbol="MES\\H20.CM", shares="1"):
            print("Buy" + shares + " Contract Market:" + symbol)
            with urllib.request.urlopen('http://localhost:8080/ExecuteOrder?symbol=' + symbol +
                                        '&ordername=CME%20Buy%20CME%20Market%20DAY' +
                                        '&shares=' + shares) as response1:
                html1: object = response1.read()
                print("API - Execute Order Response : " + html1.__str__())

    class ppro_datagram(DatagramProtocol):
        def __init__(self,
                     s="MES\\H20.CM",
                     shares="",
                     price="",
                     side="",
                     exit="",
                     profit=""):
            self.elapsedcounterstart = time.time()
            self.elapsedcountercurrent = time.time()
            self.TOS = UDPReader.TOS()
            self.counter = 0
            self.zero = 00.00
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
            self.this_symbol = s
            self.this_price = price
            self.this_exit_price = exit
            self.this_profit_price = profit
            self.this_side = side
            self.this_shares = shares
            self.this_inmarket = 0
            self.symbol = ""
            self.starttime = time.time()
            self.finished = 0

        def startProtocol(self):
            # code here what you want to start upon listener creation..
            # I use this space to connect to my logging backend and inter process communication library
            print('starting up..')

        def datagramReceived(self, data, addr):
            #self.elapsedtime()
            # decode byte data from UDP port into string, and replace spaces with NONE
            msg = data.decode("utf-8").replace(' ', 'NONE')

            # empty dict we will populate with the string data
            message_dict = {}
            # when processing PPro8 data feeds, processing the line into a dictionary is very useful:
            for item in msg.split(','):
                print(item)
                if "=" in item:
                    couple = item.split('=')
                    message_dict[couple[0]] = couple[1]
            if message_dict["Message"] == "TOS":
                self.TOS.update(message_dict)
                self.TOS.list()
            #if self.symbol == self.this_symbol.__str__():
            current_price = message_dict.get("Price")
            # if self.this_price == "":
            #     self.this_price = current_price
            #
            # if self.this_side == "":
            #     self.this_side = "buy"

            # print("This Price = " + str(float(self.this_price)))
            # print("TOS  Price = " + str(float(current_price)))

            # if self.this_side == "buy" and self.this_inmarket == 0:
            #     if float(self.this_price) == float(current_price):
            #         if '.CM' in self.this_symbol:
            #             UDPReader.BuyFutures()
            #         if '.NQ' in self.this_symbol:
            #             UDPReader.BuyNasdaq()
            #         if '.NY' in self.this_symbol:
            #             UDPReader.BuyNYSE()
            #         self.this_inmarket = 1
            #         print("Match Price = " + current_price)
            #
            # if self.this_side == "sell" and self.this_inmarket == 0:
            #     if float(self.this_price) == float(current_price):
            #         if '.CM' in self.this_symbol:
            #             UDPReader.SellFutures()
            #         if '.NQ' in self.this_symbol:
            #             UDPReader.SellNasdaq()
            #         if '.NY' in self.this_symbol:
            #             UDPReader.SellNYSE()
            #     self.this_inmarket = 1
            #     print("Match Price = " + current_price)
            #
            # if self.this_side == "sell" and \
            #         float(current_price) == float(self.this_exit_price) \
            #         or float(current_price) == float(self.this_profit_price) \
            #         and self.this_inmarket == 1:
            #     if '.CM' in self.this_symbol:
            #         UDPReader.BuyFutures()
            #     if '.NQ' in self.this_symbol:
            #         UDPReader.BuyNasdaq(self.this_symbol, self.this_shares)
            #     if '.NY' in self.this_symbol:
            #         UDPReader.BuyNYSE(self.this_symbol, self.this_shares)
            #     print("Order Filled @ " + current_price)
            #     self.this_inmarket = 0
            #     # UDPReader.BuyFutures(self.this_symbol, "1")
            #
            # if self.this_side == "buy" and \
            #         (float(current_price) == float(self.this_exit_price)
            #          or float(current_price) >= float(self.this_profit_price)) \
            #         and self.this_inmarket == 1:
            #     if '.CM' in self.this_symbol:
            #         UDPReader.SellFutures()
            #     if '.NQ' in self.this_symbol:
            #         UDPReader.SellNasdaq(self.this_symbol, self.this_shares)
            #     if '.NY' in self.this_symbol:
            #         UDPReader.SellNYSE(self.this_symbol, self.this_shares)
            #     print("Order Filled @ "+current_price)
            #     self.this_inmarket = 0

            # now you can call specific data by name in the line you're processing instead of counting colums
            # See the print statement below for examples


        def connectionRefused(self):
            print("No one listening")

        def wait_until(end_datetime):
            while True:
                diff = (end_datetime - datetime.now()).total_seconds()
                if diff < 0:
                    return       # In case end_datetime was in past to begin with
                time.sleep(diff/2)
                if diff <= 0.1:
                    return

    @staticmethod
    def start(self, symbol="ES\\H20.CM", shares="10", port="5555", price="", side="", exit="", profit=""):
        my_symbol = symbol
        # Usage: reactor.listenUDP(_PORT_, ppro_datagram(_SYMBOL_))
        # Start listening on UDP _PORT_ 555 for message related to _SYMBOL_
        # Note: If the _SYMBOL_ is omitted it will default to ES\U19.CM
        print("Symbol : " + my_symbol)
        print("Side   : " + side)
        print("Price  : " + price)
        print("Exit : " + exit)
        print("Profit: " + profit)
        if side == "":
            side = "buy"
        reactor.listenUDP(int(port), UDPReader.ppro_datagram(my_symbol, shares, price, side, exit, profit))
        reactor.run()

