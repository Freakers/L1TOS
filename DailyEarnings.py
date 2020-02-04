import urllib.request
import urllib.response
import time
import os
import threading
import pandas as pd
from datetime import date
import pause
import shutil
from datetime import datetime

class EarningsSymbols:

    def __init__(self, file, feedtype="TOS", output="bytype", loadtype="symbol"):
        print("Register Symbols File: " + file.__str__()+" Feed Type: TOS   Output To : BYTYPE")
        self.symbols = {}
        if loadtype == "file":
            self.loadsymbols(file)
        if loadtype == "symbol":
            self.loadsymbol(file)
        self.registersymbols(feedtype, output)

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
            pause.sleep(0.2)

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
        n, dte = datetime.now()
        pause.until(datetime(n.year, n.month, n.day, hour, minute, sec, 0))
        if not os.path.exists("C:\\logs\\" + dte.year.__str__() +
                              "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0")):
            os.mkdir("C:\\logs\\" + dte.year.__str__() + "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0"))
        shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Inka\\" + feed, "C:\\logs\\" + dte.year.__str__() +
                    "-" + dte.month.__str__() + "-" + dte.day.__str__().rjust(2, "0") + "\\" + file)

    @staticmethod
    def main():
        # Forcing Pandas to display max rows and columns.
        pd.option_context('display.max_rows', None, 'display.max_columns', None)
        # Reading the earnings calendar table on yahoo finance website.
        nasdaq = pd.read_csv("NASDAQ.csv", names=['Symbol'])
        nyse = pd.read_csv("NYSE.csv", names=['Symbol'])
        earnings = pd.read_html('https://finance.yahoo.com/calendar/earnings')[0]
        # print(str(earnings))
        sym = pd.DataFrame(earnings)
        count = sym['Symbol'].size
        # print(count)
        i = 0
        daily_earnings = pd.DataFrame(columns=['Symbol'])
        while i < count:
            symbol = str(sym['Symbol'][i])
            for a, nasadaq_symbol in nasdaq.iterrows():
                #print(str(nasadaq_symbol[0]))
                if symbol + ".NQ" == str(nasadaq_symbol[0]):
                    daily_earnings = daily_earnings.append(
                        {'Symbol': str(nasadaq_symbol[0])},
                        ignore_index=True)
                else:
                    pass
            i += 1
        # Determine if the Symbol Trades on NYSE Exchange
        i = 0
        while i < count:
            symbol = str(sym['Symbol'][i])
            for a, nyse_symbol in nyse.iterrows():
                #print(str(nyse_symbol[0]))
                if symbol + ".NY" == str(nyse_symbol[0]):
                    daily_earnings = daily_earnings.append(
                        {'Symbol': str(nyse_symbol[0])},
                        ignore_index=True)
                else:
                    pass
            i += 1


        # Writing to a CSV file.
        daily_earnings.to_csv(r'Earnings_{}.csv'.format(date.today()), index=None, header=None)
        n = datetime.now()
        print(n.year.__str__() + n.month.__str__() + n.day.__str__())
        EarningsSymbols(r'Earnings_{}.csv'.format(date.today()), "TOS", "5556", "file")

if __name__ == '__main__':
    EarningsSymbols.main()

