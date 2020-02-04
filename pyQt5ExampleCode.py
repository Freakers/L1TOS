from PyQt5.QtWidgets import QApplication, QLabel

app = QApplication([])
title = "Trading Meter\n"
label = QLabel(title+"Trading Meter\nTrade Into Bid")
label.show()
app.exec_()
