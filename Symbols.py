import urllib.request
import urllib.response
import time
import os
import threading
import unittest
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
import pause
import shutil
from datetime import datetime
from pathlib import Path

class Symbols:

    # def __init__(self, symbol, feetype="TOS", output="bytype"):
    #     print("Register Symbol: " + symbol.__str__()+" Feed Type: " + feetype + "Output To : ")
    #     self.symbols = {}
    #     self.symbols[1] = symbol
    #     #self.loadsymbols(file)
    #     #print(self.symbols.__len__())
    #     #self.listsymbols()
    #     #self.deregistersymbol()
    #     self.registersymbols()

    def __init__(self, file, feedtype="TOS", output="bytype", loadtype="symbol"):
        print("Register Symbols File: " + file.__str__()+" Feed Type: TOS   Output To : BYTYPE")
        self.symbols = {}
        if loadtype == "file":
            self.loadsymbols(file)
        if loadtype == "symbol":
            self.loadsymbol(file)
        #print(self.symbols.__len__())
        #self.listsymbols()
        #self.deregistersymbol()
        self.registersymbols(feedtype, output)

    # def __init__(self, file="Symbols.csv"):
    #     print("Loading Symbols File Only: " + file.__str__())
    #     self.symbols = {}
    #     self.loadsymbols(file)
    #     self.registersymbol("TOS", "bytype")
    #
    # def __init__(self, **mocsymbols):
    #     print("Initiate Symbols")
    #     self.symbols = {}
    #     self.loadsymbols(mocsymbols)
    #     print(self.symbols.__len__())
    #     self.listsymbols()
    #     #self.deregistersymbol()
    #     self.registersymbols()

    def setsymbols(self, record, sym):
        self.symbols[record] = sym

    def getsymbols(self):
        return self.symbols

    def getsymbol(self):
        return self.symbols.get(1).__str__()

    def registersymbols(self, feedtype="TOS", output="bytype"):
        print("Starting THREADS to register L1 AND TOS for Symbol List")
        for record, symbol in self.symbols.items():
            print(symbol + " : "+feedtype+" : "+output)
            t = threading.Thread(target=self.registersymbol, args=(symbol, feedtype, output,))
            t.start()
            pause.sleep(0.05)

    def registersyms(self, feedtype="TOS", output="bytype"):
        print("Starting THREADS to register L1 AND TOS for Symbol List")
        for record, symbol in self.symbols.items():
            self.registersymbol(symbol, feedtype, output)


    def deregistersymbols(self):
        print("Starting THREADS to deregister L1 AND TOS for Symbol List")
        for record, symbol in self.symbols.items():
            print(symbol)
            t = threading.Thread(target=self.deregistersymbol, args=(symbol, "TOS", "5556",))
            t.start()

    def registersymbol(self, symbol, feedType, output):
        print('Register Symbol Request  : http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedType)
        with urllib.request.urlopen('http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedType) \
                as response1:
            html1: object = response1.read()
            print("Register Symbol Response : " + html1.__str__())
        print('http://localhost:8080/SetOutput?symbol=' + symbol + '&feedtype=' + feedType + '&output=' + output + '&status=on')
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedType + '&output=' + output + '&status=on') as response2:
            html2: object = response2.read()
            print("Register Output: " + html2.__str__())

    def deregistersymbol(self, symbol, feedType, region, output):
        print('Deregister Symbol Request  : http://localhost:8080/Deregister?symbol=' + symbol + '&region=' +
              region + '&feedtype=' + feedType)
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedType + '&output=' + output + '&status=off') as response0:
            html0: object = response0.read()
            #print("Deregister Symbol Response : " + html0.__str__())
        with urllib.request.urlopen('http://localhost:8080/Deregister?symbol=' + symbol + '&region=' + region +
                                    '&feedtype=' + feedType) as response1:
            html1: object = response1.read()
            #print("Deregister Symbol Response : " + html1.__str__())

    def loadsymbol(self, symbol):
        print("Symbol Loaded: " + str(symbol))
        self.symbols[1] = symbol


    def loadsymbols(self, file):
        print("Loading File: " + time.asctime())
        print("Current Working Directory: " + file)
        file = open(file, "r")
        recordcount = 1
        for symbol in file:
            # print(symbol.rstrip())
            self.symbols[recordcount] = symbol.rstrip()
            recordcount += 1
        print("Symbol File Loaded: " + time.asctime())

    def listsymbols(self):
        for rec, symbol in self.symbols.items():
            print("Symbol[" + str(rec) + "]: " + symbol)

    def len(self):
        return self.symbols.items().__len__()

    def movedata(self, feed, file, hour, minute, sec):
        pause.until(datetime(n.year, n.month, n.day, 12, 00, 00, 0))
        dte = datetime.now()
        pause.until(datetime(n.year, n.month, n.day, hour, minute, sec, 0))
        if not os.path.exists("C:\\logs\\" + dte.year.__str__() +
                              "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0")):
            os.mkdir("C:\\logs\\" + dte.year.__str__() + "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0"))
        shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Inka\\" + feed, "C:\\logs\\" + dte.year.__str__() +
                    "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0") + "\\" + file)

n = datetime.now()
print(n.year.__str__()+n.month.__str__()+n.day.__str__())
# test_Milan = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Milan.csv", "TOS", "bytype", "file")
# test_Amsterdam = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Amsterdam.csv", "TOS", "bytype", "file")
# test_Brusells = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Brussels.csv", "TOS", "bytype", "file")
# test_Lisbon = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Lisbon.csv", "TOS", "bytype", "file")
# test_Paris = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Paris.csv", "TOS", "bytype", "file")
test_NASDAQ = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Nasdaq.csv", "TOS", "bytype", "file")
#test_NYSE = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Nyse.csv", "TOS", "bytype", "file")
# test_symbols = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Symbols.csv", "TOS", "bytype", "file")
# test_symbols.listsymbols()
# pause.until(datetime(n.year, n.month, n.day, 11, 40, 00, 0))
# if not os.path.exists("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2, "0")):
#     os.mkdir("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2, "0"))
# shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Inka\\TOS_2.log", "C:\\logs\\"+n.year.__str__() +
#             "-" + n.month.__str__() + "-" + n.day.__str__().rjust(2, "0") + "\\Europe.csv")
# # test_NASDAQ.movedata("TOS_1", "Europe.log", "12", "22", "00")
