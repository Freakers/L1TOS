import urllib.request
import urllib.response
import time
import os
import pause
import threading
from twisted.internet.protocol import DatagramProtocol
import shutil
from datetime import datetime

class TSX_MOC_Report:

    class Symbols:
        symbols = {}

        def _init__(self):
            print("Hello")

        def setsymbols(self, record, sym):
            self.symbols[record] = sym

        def getsymbols(self):
            return print(self.symbols.__str__())


    class mocsymbols:

        def __init__(self, mocsym):
            self.symbols = {}
            self.loadmocsymbols(mocsym)
            print(self.symbols.__len__())
            # self.listsymbols()
            # self.deregistersymbol()
            self.registersymbols()

        def setsymbols(self, record, sym):
            self.symbols[record] = sym

        def getsymbols(self):
            return self.symbols

        def registersymbols(self):
            print("Starting THREADS to register TOS for MOC Symbol List")
            for record, symbol in self.symbols.items():
                #print(symbol)
                t = threading.Thread(target=self.registersymbol, args=(symbol, "TOS", "bytype",))
                t.start()

        def deregistersymbols(self):
            print("Starting THREADS to deregister TOS for MOC Symbol List")
            for record, symbol in self.symbols.items():
                #print(symbol)
                t = threading.Thread(target=self.deregistersymbol, args=(symbol, "TOS", "1",))
                t.start()

        def registersymbol(self, symbol, feedType, output):
            #print('Register Symbol Request  : http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedType)
            with urllib.request.urlopen('http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedType) as response1:
                html1: object = response1.read()
                #print("Register Symbol Response : " + html1.__str__())
            #print('http://localhost:8080/SetOutput?symbol=' + symbol + '&feedtype=' + feedType + '&output=' + output + '&status=on')
            with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                        '&feedtype=' + feedType + '&output=' + output + '&status=on') as response2:
                html2: object = response2.read()
                #print("Register Output: " + html2.__str__())

        def deregistersymbol(self, symbol, feedType, region, output):
            print('Deregister Symbol Request  : http://localhost:8080/Deregister?symbol=' + symbol + '&region=' +
                  region + '&feedtype=' + feedType)
            with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                        '&feedtype=' + feedType + '&output=' + output + '&status=off') as response0:
                html0: object = response0.read()
                print("Deregister Symbol Response : " + html0.__str__())
            with urllib.request.urlopen('http://localhost:8080/Deregister?symbol=' + symbol + '&region=' + region +
                                        '&feedtype=' + feedType) as response1:
                html1: object = response1.read()
                #print("Deregister Symbol Response : " + html1.__str__())

        def loadmocsymbols(self, mocsymbols):
            for key, symbol in mocsymbols.items():
                self.symbols[key] = symbol.rstrip()


    class RegisterSymbol:
        """Registers a single symbol in PPro8 API"""
        def __init__(self, symbol="CRON.TO", feedType="TOS", output="btype"):
            print('Register Symbol Request  : http://localhost:8080/Register?symbol='+symbol+'&feedtype='+feedType)
            with urllib.request.urlopen('http://localhost:8080/Register?symbol='+symbol+'&feedtype='+feedType) \
                    as response1:
                html1: object = response1.read()
                print("Register Symbol Response : " + html1.__str__())
            print('http://localhost:8080/SetOutput?symbol='+symbol +
                  '&feedtype='+feedType+'&output='+output+'&status=on')
            with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol='+symbol +
                                        '&feedtype='+feedType+'&output='+output+'&status=on') as response2:
                html2: object = response2.read()
                print("Register Output: "+html2.__str__())


    class RegisterImbalance:
        """Registers a single symbol in PPro8 API"""
        def __init__(self):
            print("Rergister Imbalance")
            print('API - Register Imbalance Request  : ' +
                  'http://localhost:8080/Register?region=1&feedtype=IMBALANCE&output=bytype')
            with urllib.request.urlopen('http://localhost:8080/Register?region=1&feedtype=IMBALANCE&output=bytype') as response1:
                html1: object = response1.read()
                print("API - Register Imbalance Response : " + html1.__str__())


    class RegisterSymbols:
        """Registers a list of symbols in PPro8 API"""
        def __init__(self, symbols, feedType="TOS"):
            print("Register Symbols")
            print(symbols)
            for k, symbol in symbols.items():
                print("")
                print('Register Symbol Request  : http://localhost:8080/Register?symbol='+symbol.__str__()+'&feedtype='+feedType)
                with urllib.request.urlopen('http://localhost:8080/Register?symbol='+symbol.__str__() + '&feedtype='+feedType.__str__()) as response1:
                    html1: object = response1.read()
                    print("Register Symbol Response : " + html1.__str__())
                with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol='+symbol +
                                            '&feedtype='+feedType+'&output=bytype&status=on') as response2:
                    html2: object = response2.read()
                    #print("Register Output: "+symbol)


    class RegisterDictionarySymbols:
        """Registers a list of symbols in PPro8 API"""
        def __init__(self, symbols={1: {'ticker': 'CRON.TO'}}, feedType="TOS"):
            print("Register Symbols")
            for ticker, symbol in symbols.items():
                #print('Register Symbol Request  : http://localhost:8080/Register?symbol='+symbol+'&feedtype='+feedType)
                with urllib.request.urlopen('http://localhost:8080/Register?symbol='+symbol+'&feedtype='+feedType) as response1:
                    html1: object = response1.read()
                    #print("Register Symbol Response : " + html1.__str__())
                with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol='+symbol+
                                            '&feedtype='+feedType+'&output=bytype&status=on') as response2:
                    html2: object = response2.read()
                    #print("Register Output: "+symbol)

    class SnapShot:
        """Create Snapshot of Time of Sale in PPro8 API"""

        def __init__(self, feedType="TOS", **symbols):
            for k, symbol in symbols:
                print(
                    'Snapshot Request  : http://localhost:8080/GetSnapshot?symbol=' + symbol + '&feedtype=' + feedType)

                with threading.Thread(target=urllib.request.urlopen, args=(
                'http://localhost:8080/GetSnapshot?symbol=' + symbol + '&feedtype=' + feedType,)) as response:
                    # with urllib.request.urlopen('http://localhost:8080/GetSnapshot?symbol='+symbol+'&feedtype='+feedType) as response:
                    html1: object = response.read()
                    print("Snapshot Response: " + html1.__str__())

    class ImbalanceFileReader:
        """Create Time of Sale Feed Reader PPro8 API, Reads log file created by the Register Class"""

        def __init__(self, date):
            self.LOCALTIME = 0
            self.MESSAGE = 1
            self.MARKETTIME = 2
            self.SIDE = 3
            self.TYPE = 4
            self.STATUS = 5
            self.SYMBOL = 6
            self.PRICE = 7
            self.VOLUME = 8
            self.SOURCE = 9
            self.AUCTIONPRICE = 10
            self.CONTINUOUS = 11
            self.PAIREDVOLUME = 12
            n = datetime.now()
            l1_tos_stats = {}
            l1_tos_symbol = {}
            l1_symbols = {}
            rec_count = 1
            moc_file = open("C:\\logs\\" + date + "\\MOCReport.csv", "w")
            imbalance_file = open("C:\\logs\\" + date + "\\IMBAL_CIRC_1.log", "r")
            # Write MOC Column Headers
            moc_file.write("Date" + "\t" + "MarketTime" + "\t" + "Symbol" + "\t" + "Side" + "\t" + "Volume" + "\t" +
                           "Auction Price\tNet Trade Value\tClosing Price\tClosest Time\tClosest Price\n")
            tos_file = TSX_MOC_Report.TOSFileReader(date)
            for record in imbalance_file:
                if ".TO" in record:
                    #print(record)
                    fields = record.split(",")
                    moc_file.write(date + "\t" +
                                   fields[self.MARKETTIME].split("=").pop(1) + "\t" +
                                   fields[self.SYMBOL].split("=").pop(1) + "\t" +
                                   fields[self.SIDE].split("=").pop(1) + "\t" +
                                   fields[self.VOLUME].split("=").pop(1) + "\t" +
                                   fields[self.AUCTIONPRICE].split("=").pop(1) + "\t" +
                                   str(float(fields[self.VOLUME].split("=").pop(1)) * float(fields[self.AUCTIONPRICE].split("=").pop(1))) + "\t" +
                                   str(tos_file.get_last_trade(fields[self.SYMBOL].split("=").pop(1))) + '\t' +
                                   str(tos_file.get_closest_trade(fields[self.SYMBOL].split("=").pop(1),
                                                                  fields[self.AUCTIONPRICE].split("=").pop(1))) + '\n')

    class TOSFileReader:
        """Create Time of Sale File Reader, Reads log file created by the Register Class"""

        def __init__(self, date):
            self.tos_records = {}
            self.last_trade_record = {}
            self.closest_trade_record = {}
            self.closest_trade_price = 999999999.00
            self.closest = "00.00"
            self.tos_records_closing = {}
            rec_count = 1
            n = datetime.now()
            file = open("C:\\logs\\" + date + "\\TOS_1.log", "r")
            for record in file:
                if ".TO" in record:
                    #print(record)
                    self.tos_records[rec_count] = record
                    rec_count = rec_count + 1
                    #self.get_last_trade(symbol)

        def get_last_trade(self, symbol="CNE.TO"):
            symbol = "Symbol="+symbol
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
                    if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]),
                                int(mtime[2].split('.').pop(0)), 0).time() \
                            >= datetime(n.year, n.month, n.day, 16, 00, 00, 0).time():
                        #print("Last Trade Matched: "+tos_record)
                        self.last_trade_record[key] = tos_record
                        toslastprice = tos_record.split(",").pop(5).split("=").pop(1)
                        return toslastprice
                        break

        def get_closest_trade(self, symbol="CNE.TO", auctionprice=""):
            symbol = "Symbol="+symbol
            message_dict = {}
            l = len(self.tos_records)
            counter = 0
            self.closest = "00.00"
            for key, tos_record in self.tos_records.items():
                if symbol in tos_record:
                    n = datetime.now()
                    for item in tos_record.split(','):
                        couple = item.split('=')
                        message_dict[couple[0]] = couple[1]
                        trdtime  = message_dict.get('MarketTime')
                        mtime = str(trdtime).split(":")
                    if datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() > datetime(n.year, n.month, n.day, 15, 55, 00, 0).time() and \
                            datetime(n.year, n.month, n.day, int(mtime[0]), int(mtime[1]), int(mtime[2].split('.').pop(0)), 0).time() < datetime(n.year, n.month, n.day, 16, 00, 00, 0).time():
                        self.closest_trade_record[key] = tos_record
                        closestprice = abs(float(auctionprice) - float(tos_record.split(",").pop(5).split("=").pop(1)))
                        # print("Symbol: " + symbol + " Last Closest Price difference = " + str(self.closest_trade_price))
                        # print("Symbol: " + symbol + " Next Closest Price difference = " + str(closestprice))
                        if closestprice < self.closest_trade_price:
                            self.closest_trade_price = closestprice
                            self.closest = tos_record.split(",").pop(2).split("=").pop(1)+'\t'+tos_record.split(",").pop(5).split("=").pop(1)
                        if closestprice == int(0):
                            self.closest_trade_price = 99999999.00
                            return tos_record.split(",").pop(2).split("=").pop(1)+'\t'+tos_record.split(",").pop(5).split("=").pop(1)
                else:
                    counter += 1
            self.closest_trade_price = 99999999.00
            return self.closest

    class TSXClosingImbalance:
        """Data Class Used to store the TSX closing imbalance information in the Imbalance Records Dictionary"""
        mrec = {}

        def __init__(self):
            print("Initialize Imbalance Object")


        def loadfile(tradeValue, market):
            moc_records = {}
            moc_dict = {}
            symbols = {}
            """Load the Imbalance File for Parsing into the imbalancerecord(s) data dictionaries"""
            print("Start Load Imbalance File: "+time.asctime())
            file = open("C:\\Program Files (x86)\\Ralota\\PPro8 Jawa\\IMBAL_CIRC_1.log", "r")
            mocrpt = open("C:\\logs\\MOCReport.txt", "w")
            recordcount = 1
            imbalancerecords = {}
            for record in file:
                imbalancerecord = {}
                #print(record.__str__())
                if str(market) in record:
                    for field in record.split(','):
                        fieldName  = field.split('=').__getitem__(0)
                        fieldValue = field.split('=').__getitem__(1)
                        imbalancerecord[fieldName] = fieldValue
                        imbalancerecords[recordcount] = imbalancerecord
                        # Store all symbols into symbols dictionary
                        if fieldName == 'Symbol':
                            symbols[recordcount] = fieldValue
                            #print(imbalancerecord)
                    recordcount = recordcount + 1
            print("Imbalance Load Completed:  "+time.asctime())
            recordcount = 1
            for key, value in imbalancerecords.items():
                #print("Auction Price: " + imbalancerecords[key]['AuctionPrice'])
                #print("Volume        : "+ imbalancerecords[key]['Volume'])
                if float(imbalancerecords[key]['AuctionPrice']) * float(int(imbalancerecords[key]['Volume'])) >= tradeValue:
                    if imbalancerecords[key]['Side'] == 'S' and imbalancerecords[key]['Symbol'].endswith(market):
                        print("Market Time: " + imbalancerecords[key]['MarketTime'] +
                              "\tSymbol: " + imbalancerecords[key]['Symbol'].ljust(10) +
                              "\tMarket: " + imbalancerecords[key]['Source'] +
                              "\tSide: " + imbalancerecords[key]['Side'].ljust(4) +
                              "\tVolume: " + imbalancerecords[key]['Volume'].ljust(9) +
                              "\tAuctionPrice: " + imbalancerecords[key]['AuctionPrice'] +
                              "\tTradeValue: " + format(float(imbalancerecords[key]['AuctionPrice']) * float(int(imbalancerecords[key]['Volume'])), ',.2f'))
                        mocrpt.write("Market Time: " + imbalancerecords[key]['MarketTime'] +
                              "\tSymbol: " + imbalancerecords[key]['Symbol'].ljust(10) +
                              "\tMarket: " + imbalancerecords[key]['Source'] +
                              "\tSide: " + imbalancerecords[key]['Side'].ljust(4) +
                              "\tVolume: " + imbalancerecords[key]['Volume'].ljust(9) +
                              "\tAuctionPrice: " + imbalancerecords[key]['AuctionPrice'] +
                              "\tTradeValue: " + format(float(imbalancerecords[key]['AuctionPrice']) * float(int(imbalancerecords[key]['Volume'])), ',.2f'))
                        moc_dict['MarketTime'] = imbalancerecords[key]['MarketTime']
                        moc_dict['Symbol'] = imbalancerecords[key]['Symbol']
                        moc_dict['Side'] = imbalancerecords[key]['Side']
                        moc_dict['Volume'] = imbalancerecords[key]['Volume']
                        moc_dict['AuctionPrice'] = imbalancerecords[key]['AuctionPrice']
                        moc_dict['ClosingPrice'] = "00.00"
                        moc_records[recordcount] = moc_dict
                        recordcount = recordcount + 1
                        #BuyMarketOrder(imbalancerecords[key]['Symbol'], "100")

                    if imbalancerecords[key]['Side'] == 'B' and imbalancerecords[key]['Symbol'].endswith(market):
                        print("Market Time: " + imbalancerecords[key]['MarketTime'] +
                              "\tSymbol: " + imbalancerecords[key]['Symbol'].ljust(10) +
                              "\tMarket: " + imbalancerecords[key]['Source'] +
                              "\tSide: " + imbalancerecords[key]['Side'].ljust(4) +
                              "\tVolume: " + imbalancerecords[key]['Volume'].ljust(9) +
                              "\tAuctionPrice: " + imbalancerecords[key]['AuctionPrice'] +
                              "\tTradeValue: " + format(float(imbalancerecords[key]['AuctionPrice']) * float(int(imbalancerecords[key]['Volume'])), ',.2f'))
                        mocrpt.write("Market Time: " + imbalancerecords[key]['MarketTime'] +
                                     "\tSymbol: " + imbalancerecords[key]['Symbol'].ljust(10) +
                                     "\tMarket: " + imbalancerecords[key]['Source'] +
                                     "\tSide: " + imbalancerecords[key]['Side'].ljust(4) +
                                     "\tVolume: " + imbalancerecords[key]['Volume'].ljust(9) +
                                     "\tAuctionPrice: " + imbalancerecords[key]['AuctionPrice'] +
                                     "\tTradeValue: " + format(float(imbalancerecords[key]['AuctionPrice']) * float(int(imbalancerecords[key]['Volume'])), ',.2f'))
                        moc_dict['MarketTime'] = imbalancerecords[key]['MarketTime']
                        moc_dict['Symbol'] = imbalancerecords[key]['Symbol']
                        moc_dict['Side'] = imbalancerecords[key]['Side']
                        moc_dict['Volume'] = imbalancerecords[key]['Volume']
                        moc_dict['AuctionPrice'] = imbalancerecords[key]['AuctionPrice']
                        moc_dict['ClosingPrice'] = "00.00"
                        moc_records[recordcount] = moc_dict
                        recordcount = recordcount + 1
                        #SellMarketOrder(imbalancerecords[key]['Symbol'], "100")
            # Load Moc Symbols and Register Time of Sale for each imbalance record
            mocrpt.close()
            m = TSX_MOC_Report.mocsymbols(symbols)
            n = datetime.now()
            while datetime(n.year, n.month, n.day,n.hour, n.minute, n.second).time() \
                    <= datetime(n.year, n.month, n.day, 16, 12, 00).time():
                TSX_MOC_Report.SnapShot(m.getsymbols())
                time.sleep(15)
                n = datetime.now()
            print("Collecting TOS Snapshot at " + datetime(n.year, n.month, n.day,n.hour, n.minute, n.second).time().__str__())
            if not os.path.exists("C:\\logs\\" + n.date().__str__()):
                os.makedirs("C:\\logs\\" + n.date().__str__())
            shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Jawa\\IMBAL_CIRC_1.log", "C:\\logs\\" + n.date().__str__())
            shutil.copy("C:\\Program Files (x86)\\Ralota\\PPro8 Jawa\\TOS_1.log", "C:\\logs\\" + n.date().__str__())

    class loadCSVinDictionary():
        """Load CSV record into data dictionaries"""
        def __init__(self, record="field1=a, field2=b", **obj):
            for field in record.split(","):
                obj[field.split("=").__getitem__(0)] = field.split("=").__getitem__(1)

    class loadMaster():
        """Load CSV record into data dictionaries"""
        def __init__(self, moc_records):
            out = open("C:\\logs\\date\\MOCReport.csv", "r")
            out = open("C:\\logs\\master\\tsx_moc.csv", "+a")
            counter = 1;
            for key, rec in moc_records.items():
                if counter == 1:
                    counter = 0
                else:
                    out.write(rec)
            out.close()


    class ppro_datagram(DatagramProtocol):

        def __init__(self):
            self.bidpr = 0
            self.askpr = 0
            self.asks = 0
            self.bids = 0

        def startProtocol(self):
            # code here what you want to start upon listner creation..
            # I use this space to connect to my logging backend and inter process communication library
            print('starting up..')

        def datagramReceived(self, data, addr):

            # decode byte data from UDP port into string, and replace spaces with NONE
            msg = data.decode("utf-8").replace(' ', 'NONE')

            # empty dict we will populate with the string data
            message_dict = {}

            # when processing PPro8 data feeds, processing the line into a dictionary is very useful:
            for item in msg.split(','):
                couple = item.split('=')
                message_dict[couple[0]] = couple[1]
            #print(message_dict.__str__())
            # now you can call specific data by name in the line you're processing instead of counting colums
            # See the print statement below for examples

            # print('{} {} {}'.format(message_dict['Symbol'], message_dict['Message'], msg))
            if message_dict['Message'] == "L1":
                bidpr = float(message_dict['BidPrice'])
                askpr = float(message_dict['AskPrice'])
                bids = 0
                asks = 0
                print("L1 Time  :\\t" + message_dict['MarketTime'] + "\\tSymbol: " + message_dict['Symbol'])
                print("Bid Price:\\t" + message_dict['BidPrice'] + "\\tBid Size: " + message_dict['BidSize'])
                print("Ask Price:\\t" + message_dict['AskPrice'] + "\\tAsk Size: " + message_dict['AskSize'])
                x = 1
            if message_dict['Message'] == "TOS":
                print("TOS Time: " + message_dict['MarketTime'] + " Price: " + message_dict['Price'] +
                      " Size: " + message_dict['Size'])
                if float(message_dict['Price']) >= askpr:
                    asks = asks + 1
                if float(message_dict['Price']) <= bidpr:
                    bids = bids + 1
            print("Bids = " + bids.__str__() + " Asks = " + asks.__str__())
            # but any named column will not be callable:
            # message_dict['MarketTime'] " + message_dict['Symbol'])
            #             print("     Bid Price: " + message_dict['BidPrice'] + " Bid Size: " + message_dict['BidSize'])
            #             print("     Ask Price: " + message_dict['AskPrice'] + " Ask Size: " + message_dict['AskSize'])
            # message_dict['Price']

        def connectionRefused(self):
            print("No one listening")

    @staticmethod
    def main(self):
        # TSXClosingImbalance
        # Process Flow
        # Step 1. Wait until 15:30:00 PM and then register MOC Imbalance for North American Region Region=1
        # Step 2. Wait until 15:40:05 PM (TSX MOC Imbalance Reporting) and generate list of stocks that equal or exceeed a trade value of 10 million or more
        # Step 3. Then register all MOC eligible symbols for TOS (time of sale) data and capture the until 4:12 PM
        # Step 4. Once the market has closed take all moc records and find the corresponding Last Trade Price in the TOS files
        # Step 5. Create data folder and store MOC report and all data
        n = datetime.now()
        today = n.date().__str__()
        print("Starting TMX Closing Imbalance                          @ " + n.strftime("%d/%m/%Y %H:%M:%S"))
        print("Waiting for start registration to  MOC Imbalance Report @ 15:30:00 PM")
        pause.until(datetime(n.year, n.month, n.day, 15, 30, 0, 0))
        TSX_MOC_Report.RegisterImbalance()
        print("MOC Imbalance registered                                @ " + n.strftime("%d/%m/%Y %H:%M:%S"))
        pause.until(datetime(n.year, n.month, n.day, 15, 40, 5, 0))
        print("Waiting for start MOC Imbalance Report                  @ 15:40:00.05 PM")
        TSX_MOC_Report.TSXClosingImbalance.loadfile(10000000.00, ".TO")
        TSX_MOC_Report.ImbalanceFileReader(today)


if __name__ == '__main__':
    TSX_MOC_Report.main(self="")
# if __name__ == '__main__':
#     TSX_MOC_Report.loadMaster
