import datetime

class ImbalanceFileReader:
    """Create Time of Sale Feed Reader PPro8 API, Reads log file created by the Register Class"""

    def __init__(self):
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
        file = open("C:\\logs\\" + n.date().__str__() + "\\IMBAL_CIRC_1.log", "r")
        for record in file:
            if ".TO" in record:
                # print(record)
                fields = record.split(",")
                l1_symbols[rec_count] = fields[self.SYMBOL].split("=").pop(1) + ";" + \
                                        fields[self.MARKETTIME].split("=").pop(1) + ";" + fields[self.VOLUME].split(
                    "=").pop(1) + ";" + \
                                        fields[self.AUCTIONPRICE].split("=").pop(1) + ";" + \
                                        str(float(fields[self.VOLUME].split("=").pop(1)) * float(
                                            fields[self.AUCTIONPRICE].split("=").pop(1)))
                rec_count = rec_count + 1
                # print(rec_count)
