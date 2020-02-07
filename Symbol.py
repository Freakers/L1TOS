import time

class Symbol:

    def __init__(self, symbols, loadtype):
        self.symbols = {}
        if "file" in loadtype:
            self.loadsymbols(symbols)
        if "symbol" in loadtype:
            self.loadsymbol(symbols)

    def loadsymbol(self, symbol):
        self.symbols[1] = symbol
        print("Symbol Loaded: " + str(symbol))


    def loadsymbols(self, file):
        print("Loading Symbol File: "+file+" On "+time.asctime())
        file = open(file, "r")
        recordcount = 1
        for symbol in file:
            #print(symbol.rstrip())
            self.symbols[recordcount] = symbol.rstrip()
            recordcount += 1
        print(str(recordcount-1)+" Symbols Loaded")

    def getSymbols(self):
        return self.symbols