"# L1TOS" 
<H1>L1TOS Consolidated Feed Reader</H1>
<H1>Technical Overview Documentation</H1>
<H1>Date: September 5, 2019				Version: 1.0</H1>
<H2>Overview</H2>
<p>This documentation is a high level overview of the L1TOS (Level 1 and Time Of Sale) consolidated feed reader and describes how to install, run, and modify the trade utility. This utlity was written to give traders a tool to make better trading decisions.
This tool consolidates the Level 1 quotes (L1) with all Time of Sale (TOS) trade transactions and keeps a running tally on how many trades where traded at the current L1 bid price and how many trades where traded at the current L1 ask price within a given 1 minute time interval.</p>
<p>The L1TOS allows a trader to see the current trading sentiment and if it is biased towards buyers or sellers which allows the trader to quickly identify a trading bias in fast changing markets such as the S&P500 or highly active stocks within the NYSE and Nasdaq Stock Exchange.</p>  
<p>The L1TOS consolidated feed reader is trade utiliy wrtitten in Python using the PProApi and the Ralota trading platform. The use of the utlity is fairly simple and just requires a simple installation process and configuration to get you up and running.</p>
<p>Before you can use this utlity please ask your trader manager to enable the PProApi on your trading account. It will take about 24 four hours to activate (1 to 2 Days).</p>

Installation Instructions
Download the required files for L1TOS from https://github.com/Freakers/L1TOS and install into root hard drive in the C:\logs
It contains all the Python code and files required by the two programs to operate properly.
The following is the quick start instructions to get you up and running quickly.
1) Download the latest Python Interpreter (3.7.4) at https://www.python.org/downloads/
2) Install Python
3) Start DOS command line
4) In DOS command line type the following: cd\logs
5) In DOS command line  type the following py 
6) Verify interpreter started
6.1) Exit Python type CTRL-C
7) Install pip by typing: py get-pip.py (takes a while)
8) Install twisted library type py pip install twisted

This should get you started.
To start the L1 Time of Sale Feed Reader type the following on the command line:
py L1TOS.py
This will start the feed reader with the default symbol set to ES\U19.CM. 
If you want to watch a specific symbol it must be first placed into the file called C:\logs\L1TOS_NCSA_Symbols.txt in the logs directory. 
Add the symbol in the file (example add WORK.NY) and save the file. 
Start the LITOS program by typing the following:
py L1TOS WORK.NY 
Also gave you the TSXClosingImbalance.py program which reports the daily MOC and associated TSX closing prices for each MOC eligible stock that day. 
Tell me what you think and if you have any question do not hesitate to ask.

