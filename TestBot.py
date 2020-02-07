import Symbol
import Register

class TestBot:
    #symbols = Symbol.Symbol("ES\H20.CM", "symbol")
    symbols = Symbol.Symbol("Earnings_2020-02-05.csv", "file")
    symlist = symbols.getSymbols()
    reg = Register.Register(symlist, "TOS", "5555")
    # reg.remove_symbols(symlist)

