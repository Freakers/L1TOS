import urllib.request
import urllib.response
import time
import os
import threading
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
import pause
import shutil
from datetime import datetime

class Symbols:

    def __init__(self, file):
        self.symbols = {}
        self.tos_records = {}
        self.last_trade_record = {}
        self.close_trade_record = {}
        self.tos_records_closing = {}
        self.loadsymbols(file)
        #self.registersymbols()

    def setsymbols(self, record, sym):
        self.symbols[record] = sym

    def getsymbols(self):
        return print(self.symbols)

    def loadsymbols(self, file=os.getcwd().__str__() + '\\symbols.csv'):
        print("Start Load File: " + time.asctime())
        print("Current Working Directory: " + file)
        file = open(file, "r")
        recordcount = 1
        sym = {}
        for symbol in file:
            self.symbols[recordcount] = symbol.rstrip()
            recordcount += 1

    def registersymbols(self):
        print("Starting THREADS to register TOS for MOC Symbol List")
        for record, symbol in self.symbols.items():
            #print(symbol)
            t = threading.Thread(target=self.registersymbol, args=(symbol, "TOS", "bytype",))
            t.start()

    def registersymbol(self, symbol, feedtype, output):
        with urllib.request.urlopen('http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedtype) \
                as response1:
            html1: object = response1.read()
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedtype + '&output=' + output + '&status=on') as response2:
            html2: object = response2.read()

    def tosreader(self, filename="", exchange="MI"):
        rec_count = 1
        n = datetime.now()
        file = open(filename, "r")
        for record in file:
            if exchange in record:
                self.tos_records[rec_count] = record
                rec_count += 1
        file.close()

    def sorttos(self, filename, exchange):
        file = open(str(filename).replace(".log", exchange + ".csv"), "w")
        rec_count = 1
        for key, symbol in self.symbols.items():
            for key, tosrecord in self.tos_records.items():
                if symbol in tosrecord:
                    file.write(str(tosrecord))
        file.close()

    def writeclosingprice(self, file, exchange):
        now = datetime.now()
        destfile = open(file.replace("Europe"+str(exchange), "EuropeClosing"+str(exchange)+".csv"), "w")
        recordcount = 1
        x = "Date, Symbol, Last Price, Close Price\n"
        nl = "\n"
        dash = "-"
        comma = ","
        destfile.write(x)
        print(str(file))
        print(str(file.replace("Europe"+str(exchange), "EuropeClosing"+str(exchange)+".csv")))
        for key, symbol in self.symbols.items():
            last_price = self.get_last_trade(symbol)
            close_price = self.get_closing_trade(symbol)
            destfile.write(now.year.__str__() + "-" + now.month.__str__() +
                           "-" + now.day.__str__() + "," + str(symbol) +
                           "," + str(last_price) + "," + str(close_price)+"\n")
            recordcount += 1
        destfile.close()

    def lastTrades(self):
        for key, symbol in self.symbols.items():
            lastprice = self.get_closing_trade(symbol)
            print(symbol + " LT @ " + str(lastprice)+"\n")

    def listTOS(self):
        for key, tos in self.tos_records.items():
            if "Symbol=ASR.MI" in tos:
                print("Found ASR.MI: " + tos.__str__())

    def get_closing_trade(self, symbol):
        symbol = "Symbol="+symbol.rstrip()
        message_dict = {}
        for key, tos_record in self.tos_records.items():
            if symbol in tos_record:
                n = datetime.now()
                #print(n.year.__str__() + n.month.__str__() + n.day.__str__())
                for item in tos_record.split(','):
                    couple = item.split('=')
                    message_dict[couple[0]] = couple[1]
                    trdtime  = message_dict.get('MarketTime')
                    mtime = str(trdtime).split(":")
                if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() \
                        > datetime(n.year, n.month, n.day, 17, 30, 00, 0).time():
                    #print("Closing Trade Matched: "+tos_record)
                    self.close_trade_record[key] = tos_record
                    toscloseprice = tos_record.split(",").pop(5).split("=").pop(1)
                    print("Symbol: " + str(symbol) + " Close @ " + toscloseprice)
                    return toscloseprice
        return "00.00"


    def get_last_trade(self, symbol):
        n = datetime.now()
        self.tosreader("C:\\logs\\" + n.year.__str__() + "-" + n.month.__str__() + "-" +
                       n.day.__str__().rjust(2, "0") +\
                       "\\Europe.log", ".MI")
        symbol = "Symbol="+symbol.rstrip()
        message_dict = {}
        toslastprice = ""
        print("Symbol: " + str(symbol))
        for key, tos_record in self.tos_records.items():
            if symbol in tos_record:
                n = datetime.now()
                #print(n.year.__str__() + n.month.__str__() + n.day.__str__())
                for item in tos_record.split(','):
                    couple = item.split('=')
                    message_dict[couple[0]] = couple[1]
                    trdtime  = message_dict.get('MarketTime')
                    mtime = str(trdtime).split(":")
                    #print(trdtime)
                if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() \
                        < datetime(n.year, n.month, n.day, 17, 30, 00, 0).time():
                    self.last_trade_record[key] = tos_record
                    toslastprice = tos_record.split(",").pop(5).split("=").pop(1)
                else:
                    print("Symbol: " + str(symbol) + " Last @ " + toslastprice)
                    return toslastprice
        return "00.00"

n = datetime.now()
print(n.year.__str__()+n.month.__str__()+n.day.__str__())

test_NASDAQ = Symbols("C:\\Users\\tctech\\PycharmProjects\\L1TOS\\Nasdaq.csv", "TOS", "bytype")

pause.until(datetime(n.year, n.month, n.day, 10, 0, 0, 0))
if not os.path.exists("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2, "0")):
    os.mkdir("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2, "0"))
shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Inka\\TOS_1.log", "C:\\logs\\"+n.year.__str__() +
            "-" + n.month.__str__() + "-" + n.day.__str__().rjust(2, "0") + "\\NASDAQ.log")


