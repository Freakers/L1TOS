import Symbols

class TOS:

    def __init__(self):
        print("Initialize TOS Object")
        self.message = {}
        self.TOS = self.loadtos("TOS_2_MILAN-2019-10-25.log")
        self.sym = Symbols.Symbols("Milan.csv").getsymbols()
        print(self.TOS.__len__())
        print(self.sym.__len__())
        self.getlasttrades()


    def update(self, localtime, message, mkttime, symbol, price, size, source, condition, tick, mmid, submktid, date, buyerid, sellerid):
        msg = {}
        msg['LocalTime'] = localtime
        msg['Message'] = message
        msg['MarketTime'] = mkttime
        msg['Symbol'] = symbol
        msg['Price'] = price
        msg['Size'] = size
        msg['Source'] = source
        msg['Condition'] = condition
        msg['Tick'] = tick
        msg['Mmid'] = mmid
        msg['SubMarketId'] = submktid
        msg['Date'] = date
        msg['BuyerId'] = buyerid
        msg['SellerId'] = sellerid
        # print("Processing L1 Message")
        self.TOS[symbol+mkttime] = msg

    def getlasttrades(self):
        tos_record = {}
        for key, symbol in self.sym.items():
            for tosmsg , tosrecord in self.TOS.items():
                tosrecord = self.getTOSDictionary(tosrecord.__str__())
                if tosrecord['Symbol'] == symbol:
                    print(tosrecord.__str__())

    def loadtos(self, file):
        records = {}
        file = open(file, "r")
        recordcount = 1
        for record in file:
            records[recordcount] = record.rstrip()
            recordcount += 1
        return records

    def getTOSDictionary(self, record):
        msg = {}
        for item in record.split(','):
            if "=" in item:
                couple = item.split('=')
                msg[couple[0]] = couple[1]
        return msg


x = TOS()

