import Symbol
import Register
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
import UDPReader

class TestBot:
    symbols = Symbol.Symbol("test.csv", "file")
    #symbols = Symbol.Symbol("AAPL.NQ", "symbol")
    symlist = symbols.getSymbols()
    port = "5555"
    reg = Register.Register(symlist, "TOS", port)
    side = "sell"
    shares = "10"
    entry_price = float("322.30")
    if side == "sell":
        exit_price = float(entry_price) - 0.5
    else:
        exit_price = float(entry_price) + 0.5

    if side == "buy":
        profit_price = float(entry_price) + 2
    else:
        profit_price = float(entry_price) - 2

    UDPReader.UDPReader.start(self="",
                              symbol=symlist.get(1),
                              shares=shares,
                              port=port,
                              price=str(entry_price),
                              side=side,
                              exit=str(exit_price),
                              profit=str(profit_price))
