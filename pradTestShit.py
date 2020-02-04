from tkinter import *

def outputEvent():
    tickerData = ticker.get
    Label(window, text=tickerData).grid(row=3, column=0)


window = Tk()
Label(window, text="Enter ticker: ").grid(row=0, column=0)
ticker = StringVar()
tickerInput = Entry(window, textvariabl=ticker).grid(row=1, column=0)
Button(window, text="Start", command=outputEvent).grid(row=2, column=0)
window.mainloop()
