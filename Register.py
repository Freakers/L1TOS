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

class Register:

    def __init__(self, feedtypes=["TOS"], output="5555", s={}):
        self.symbols = s
        self.feeds = feedtypes
        self.out = output
        for rec, symbol in self.symbols.items():
            for feed in self.feeds:
                self.add_symbol(symbol, feed, self.out)

    def remove_symbols(self):
        for record, symbol in self.symbols.items():
            #print(symbol)
            t = threading.Thread(target=self.deregistersymbol, args=(symbol, "TOS", "1",))
            t.start()

    def add_symbol(self, symbol, feedType, output):
        print("Registering Symbol: " + symbol)
        with urllib.request.urlopen('http://localhost:8080/Register?symbol=' + symbol + '&feedtype=' + feedType) \
                as response1:
            html1: object = response1.read()
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedType + '&output=' + output + '&status=on') as response2:
            html2: object = response2.read()

    def deregistersymbol(self, symbol, feedType, region, output):
        with urllib.request.urlopen('http://localhost:8080/SetOutput?symbol=' + symbol +
                                    '&feedtype=' + feedType + '&output=' + output + '&status=off') as response0:
            html0: object = response0.read()
        with urllib.request.urlopen('http://localhost:8080/Deregister?symbol=' + symbol + '&region=' + region +
                                    '&feedtype=' + feedType) as response1:
            html1: object = response1.read()

