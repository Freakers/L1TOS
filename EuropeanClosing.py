import urllib.request
import urllib.response
import time
import os
import threading
import pandas as pd
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

    def tosreader(self, filename="", exchange=".PA"):
        print("File : "+filename)
        infile = open(filename, "r")
        outfl = filename.replace(".csv", exchange+".csv")
        outfile = open(outfl, 'w')
        tos = infile.readlines()
        for reccount, symbol in self.symbols.items():
            for rec in tos:
                if symbol in rec:
                    outfile.write(rec)
                else:
                    pass
        infile.close()
        outfile.close()

    def sorttos(self, filename):

        input_filename  = filename
        output_filename = filename.replace("Europe.", "Europe.Sorted.")
        # df = pd.read_csv(filename)
        with open(input_filename, 'r') as foo:
            bar = foo.readlines()

        foo = []

        for item in bar:
            foo.append(item.split(','))
        foobar = []
        # ['LocalTime=10:39:02.493', 'Message=TOS', 'MarketTime=16:39:01.303', 'Symbol=AB.PA', 'Type=0', 'Price=5.430000', 'Size=1000', 'Source=138', 'Condition=0', 'Tick=D', 'Mmid=E', 'SubMarketId=32', 'Date=2019-11-15', 'BuyerId=0', 'SellerId=0\n']
        columns = []
        for lineitem in foo[0]:
            columns.append(lineitem.split('=')[0])
        for lineitem in foo:
            foobar.append([x.split('=')[1].replace('\n', '') for x in lineitem])
        with open(output_filename, 'w') as working_file:
            working_file.write(','.join(columns) + '\n')
            for working_line in foobar:
                working_file.write(','.join(working_line) + '\n')
        final_df = pd.read_csv(output_filename)
        final_df['Datetime'] = final_df['Date'] + ' ' + final_df['MarketTime']
        final_df['Datetime'] = pd.to_datetime(final_df['Datetime'], format='%Y-%m-%d %H:%M:%S.%f')
        final_df = final_df.sort_values(['Symbol', 'Datetime'])
        final_df.to_csv(output_filename, index=False)

    def writeclosingprice(self, file):
        now = datetime.now()
        destfile = open(file.replace("Europe.Sorted", "Europe.Closing"), "w")
        recordcount = 1
        x = "Date, Symbol, Last Price, Close Price\n"
        nl = "\n"
        dash = "-"
        comma = ","
        destfile.write(x)
        #print(str(file))
        #print(str(file.replace("Europe"+str(exchange), "EuropeClosing"+str(exchange)+".csv")))
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

    def getclosingprice(self, file):
        now = datetime.now()
        tos_df = pd.read_csv(file)
        destinationfile = open(file.replace("Europe.Sorted", "Europe.Closing"), "+w")
        for index, row in tos_df.iterrows():
            #print(row)
            destinationfile.writelines(str(row))
        destinationfile.close()

    def getcp(self, file):
        now = datetime.now()
        tos_df = pd.read_csv(file)
        destinationfile = open(file.replace("Europe.Sorted", "Europe.Closing"), "+w")
        tos_df[(tos_df['MarketTime'] > '2019-11-25 17:25:00') & (tos_df['MarketTime'] < '2019-11-25 17:30:00')]
        tos_df.to_csv(file.replace("Europe.Sorted", "Europe.Closing"))

    def listTOS(self):
        for key, tos in self.tos_records.items():
            print(str(tos))

    def listSymbols(self):
        for key, symbols in self.symbols.items():
            print(str(key)+": "+str(symbols))

    def get_closing_trade(self, symbol):
        symbol = "Symbol=" + symbol.rstrip()+","
        for key, tos_record in self.tos_records.items():
            if symbol in tos_record:
                n = datetime.now()
                #print(n.year.__str__() + n.month.__str__() + n.day.__str__())
                for item in tos_record.split(','):
                    message_dict = {}
                    couple = item.split('=')
                    message_dict[couple[0]] = couple[1]
                    trdtime  = message_dict.get('MarketTime')
                    mtime = str(trdtime).split(":")
                if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() \
                        > datetime(n.year, n.month, n.day, 17, 35, 0, 0).time():
                    #print("Closing Trade Matched: "+tos_record)
                    self.close_trade_record[key] = tos_record
                    toscloseprice = tos_record.split(",").pop(5).split("=").pop(1)
                    #print("Symbol: " + str(symbol) + " Close @ " + toscloseprice)
                    return toscloseprice
                else:
                    pass
        return "00.00"


    def get_last_trade(self, symbol):
        n = datetime.now()
        self.tosreader("C:\\logs\\" + n.year.__str__() + "-" + n.month.__str__() + "-" +
                       n.day.__str__().rjust(2, "0") +\
                       "\\Europe.log", ".MI")
        symbol = "Symbol="+symbol.rstrip()+","
        message_dict = {}
        toslastprice = ""
        #print("Symbol: " + str(symbol))
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
                    #print("Symbol: " + str(symbol) + " Last @ " + toslastprice)
                    return toslastprice
        return "00.00"

n = datetime.now()
print(n.year.__str__()+n.month.__str__()+n.day.__str__())
#
# pause.until(datetime(n.year, n.month, n.day, 12, 41, 0, 0))
# if not os.path.exists("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__()):
#     os.mkdir("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__())
# shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Inka\\TOS_2.log", "C:\\logs\\"+n.year.__str__() +
#             "-" + n.month.__str__() + "-" + n.day.__str__().rjust(2, "0") + "\\Europe.log")
#
S = Symbols("C:\\logs\\Milan.csv")
S.tosreader("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.csv", ".MI")
S.sorttos("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.MI.csv")
# S.getclosingprice("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.MI.csv")
S.getcp("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.MI.csv")
#
# S = Symbols("C:\\logs\\Amsterdam.csv")
# S.tosreader("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.log")
# S.sorttos("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.AS.csv")
# S.writeclosingprice("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.AS.csv")
#
# S = Symbols("C:\\logs\\Brussels.csv")
# S.tosreader("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.csv", ".BR")
# S.sorttos("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.BR.csv")
# S.writeclosingprice("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.BR.csv")
#
# S = Symbols("C:\\logs\\Lisbon.csv")
# S.tosreader("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.csv", ".LS")
# S.sorttos("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.LS.csv")
# S.writeclosingprice("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.LS.csv")
#
# S = Symbols("C:\\logs\\Paris.csv")
# S.tosreader("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.csv", ".PA")
# S.sorttos("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.PA.csv")
# S.writeclosingprice("C:\\logs\\"+n.year.__str__()+"-"+n.month.__str__()+"-"+n.day.__str__().rjust(2,"0")+"\\Europe.Sorted.PA.csv")

