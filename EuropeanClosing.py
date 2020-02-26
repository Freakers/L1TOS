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

class EuropeanClosing:

    def __init__(self, file):
        self.symbols = {}
        self.tos_records = {}
        self.last_trade_record = {}
        self.close_trade_record = {}
        self.tos_records_closing = {}
        self.loadsymbols(file)

    def setsymbols(self, record, sym):
        self.symbols[record] = sym

    def getsymbols(self):
        return print(self.symbols)

    def loadsymbols(self, file=os.getcwd().__str__() + '\\symbols.csv'):
        print("Loading Symbols File @ " + time.asctime())
        print("Symbols File         : " + file)
        file = open(file, "r")
        recordcount = 1
        sym = {}
        for symbol in file:
            self.symbols[recordcount] = symbol.rstrip()
            recordcount += 1
        print("Symbols File Loaded  @ " + time.asctime())

    def registersymbols(self):
        print("Starting THREADS to register TOS2 file for EUREX Symbols List")
        for record, symbol in self.symbols.items():
            #print(symbol)
            t = threading.Thread(target=self.registersymbol, args=(symbol, "TOS", "bytype",))
            t.start()

    def registersymbol(self, symbol, feedtype, output):
        #print("Register Symbol: " + symbol + "\tFeed: " + feedtype + "\tOutput: " + output )
        with urllib.request.urlopen('http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedtype) \
                as response1:
            html1: object = response1.read()
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedtype + '&output=' + output + '&status=on') as response2:
            html2: object = response2.read()

    def tosreader(self, filename="", exchange=".PA"):
        print("Reading TOS File : " + filename + " Exchange Id = " + exchange)
        infile = open(filename, "r")
        # outfl = filename.replace(".csv", exchange+".csv")
        # outfile = open(outfl, 'w')
        tos = infile.readlines()
        key = 0
        for reccount, symbol in self.symbols.items():
            for rec in tos:
                if "Symbol="+symbol+"," in rec:
                    # outfile.write(rec)
                    self.tos_records[key] = rec
                    key += 1
                else:
                    pass
        infile.close()
        # outfile.close()

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
        tos_df.set_index('Symbol', 'Datetime')
        destinationfile = open(file.replace("Europe.Sorted", "Europe.Closing"), "+w")
        tos_df.to_csv(file.replace("Europe.Sorted", "Europe.Closing"))

    def listTOS(self):
        for key, tos in self.tos_records.items():
            print(str(tos))

    def listSymbols(self):
        for key, symbols in self.symbols.items():
            print(str(key)+": "+str(symbols))

    def getClosingTrades(self, outfl, date):
        outfile = open(outfl, 'w')
        outfile.write("Date\tSymbol\tCAP\tMarketTime\tSize\tLastTradeTime\tLastTradePrice" + '\n')
        for key, symbols in self.symbols.items():
            outfile.write(str(date) + "\t" + symbols + "\t" + self.get_closing_trade(symbols)+'\n')

    def get_closing_trade(self, symbol):
        symbol = "Symbol=" + symbol +","
        # print(symbol)
        mtime = "00:00:00.000"
        ltprice = ""
        last_trade_time = ""
        last_trade_price = ""
        message_dict = {}
        for key, tos_record in self.tos_records.items():
            if symbol in tos_record:
                # print("Key: " + str(key) + " : " + tos_record)
                n = datetime.now()
                #print(n.year.__str__() + n.month.__str__() + n.day.__str__())
                for item in tos_record.split(','):
                    couple = item.split('=')
                    message_dict[couple[0]] = couple[1]
                trdtime  = message_dict.get('MarketTime')
                size = message_dict.get('Size')
                # print("Trade Time: " + str(trdtime))
                mtime = str(trdtime).split(":")
                if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() \
                        < datetime(n.year, n.month, n.day, 17, 30, 0, 0).time():
                    last_trade_time = trdtime
                    ltprice = message_dict.get('Price')
                if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() \
                        > datetime(n.year, n.month, n.day, 17, 30, 0, 0).time():
                    # print("Closing Trade Matched: "+tos_record)
                    self.close_trade_record[key] = tos_record
                    toscloseprice = tos_record.split(",").pop(5).split("=").pop(1)
                    # print("Symbol: " + str(symbol) + " Close @ " + toscloseprice)
                    return toscloseprice + "\t" + \
                           str(trdtime) + "\t" + \
                           str(size) + "\t" + \
                           str(last_trade_time) + "\t" + \
                           str(ltprice)
        return "00.00"

    def get_last_trade(self, symbol):
        n = datetime.now()
        self.tosreader("C:\\logs\\" + n.year.__str__() + "-" + n.month.__str__().rjust(2, "0") + "-" +
                       n.day.__str__().rjust(2, "0") +\
                       "\\Europe.Sorted.MI.csv", ".MI")
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

    def register_eure_time_of_sale(self):
        currentdate = datetime.now()
        print(currentdate.year.__str__() + currentdate.month.__str__() + currentdate.day.__str__())
        #
        pause.until(datetime(currentdate.year, currentdate.month, currentdate.day, 11, 0, 0, 0))
        eurexsymbols = EuropeanClosing("C:\\logs\\Milan.csv")
        eurexsymbols.registersymbols()
        pause.sleep(3)
        eurexsymbols = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurexsymbols.registersymbols()
        pause.sleep(3)
        eurexsymbols = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurexsymbols.registersymbols()
        pause.sleep(3)
        eurexsymbols = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurexsymbols.registersymbols()
        pause.sleep(3)
        eurexsymbols = EuropeanClosing("C:\\logs\\Paris.csv")
        eurexsymbols.registersymbols()

    def move_eurex_time_of_sale(self):
        currentdate = datetime.now()
        print(currentdate.year.__str__() + currentdate.month.__str__() + currentdate.day.__str__())
        pause.until(datetime(currentdate.year, currentdate.month, currentdate.day, 11, 41, 0, 0))
        if not os.path.exists("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__()):
            os.mkdir("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__())
        shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Awa\\TOS_2.log", "C:\\logs\\"+currentdate.year.__str__() +
                    "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv")

    def get_milan(self, date):
        print("Milan Closing Performance")
        currentdate = date
        eurexsymbols = EuropeanClosing("C:\\logs\\Milan.csv")
        eurexsymbols.tosreader("C:\\logs\\" + currentdate.year.__str__() + "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv", ".MI")
        eurexsymbols.getClosingTrades("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__().rjust(2, "0")+"\\Europe.Milan.ClosePrice.csv", currentdate.date())

    def get_amsterdam(self, date):
        print("Amsterdam Closing Performance")
        currentdate = date
        eurexsymbols = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurexsymbols.tosreader("C:\\logs\\" + currentdate.year.__str__() + "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv", ".AS")
        eurexsymbols.getClosingTrades("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__().rjust(2, "0")+"\\Europe.Amsterdam.ClosePrice.csv", currentdate.date())

    def get_brussels(self, date):
        print("Brussels Closing Performance")
        currentdate = date
        eurexsymbols = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurexsymbols.tosreader("C:\\logs\\" + currentdate.year.__str__() + "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv", ".BR")
        eurexsymbols.getClosingTrades("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__().rjust(2, "0")+"\\Europe.Brussels.ClosePrice.csv", currentdate.date())

    def get_lisbon(self, date):
        print("Lisbon Closing Performance")
        currentdate = date
        eurexsymbols = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurexsymbols.tosreader("C:\\logs\\" + currentdate.year.__str__() + "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv", ".LS")
        eurexsymbols.getClosingTrades("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__().rjust(2, "0")+"\\Europe.Lisbon.ClosePrice.csv", currentdate.date())

    def get_paris(self, date):
        print("Paris Closing Performance")
        currentdate = date
        eurexsymbols = EuropeanClosing("C:\\logs\\Paris.csv")
        eurexsymbols.tosreader("C:\\logs\\" + currentdate.year.__str__() + "-" + currentdate.month.__str__().rjust(2, "0") + "-" + currentdate.day.__str__().rjust(2, "0") + "\\Europe.csv", ".PA")
        eurexsymbols.getClosingTrades("C:\\logs\\"+currentdate.year.__str__()+"-"+currentdate.month.__str__().rjust(2, "0")+"-"+currentdate.day.__str__().rjust(2, "0")+"\\Europe.Paris.ClosePrice.csv", currentdate.date())

    def get_EUREX(self, date):
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)

    @staticmethod
    def main():
        date = datetime(2020, 2, 18)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 19)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 20)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 21)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 24)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 25)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)
        date = datetime(2020, 2, 26)
        eurex = EuropeanClosing("C:\\logs\\Lisbon.csv")
        eurex.get_lisbon(date)
        eurex = EuropeanClosing("C:\\logs\\Paris.csv")
        eurex.get_paris(date)
        eurex = EuropeanClosing("C:\\logs\\Brussels.csv")
        eurex.get_brussels(date)
        eurex = EuropeanClosing("C:\\logs\\Milan.csv")
        eurex.get_milan(date)
        eurex = EuropeanClosing("C:\\logs\\Amsterdam.csv")
        eurex.get_amsterdam(date)

if __name__ == '__main__':
    EuropeanClosing.main()
