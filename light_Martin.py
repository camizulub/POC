from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import argparse
from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from backtrader.analyzers import (SQN, SharpeRatio, DrawDown)
from market_profile import MarketProfile
from math import fsum

data = pd.read_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/ES_1min(2018)NinjaWPOC.csv', parse_dates=True, index_col='Date')
commission, margin, mult = 0.85, 50, 50.0

f = open("C:/Users/MiloZB/Dropbox/Codigos/Scripts/Repos/POC/Backtestings/logbook.txt", "a")
f1 = open("C:/Users/MiloZB/Dropbox/Codigos/Scripts/Repos/POC/Backtestings/Indicators.txt", "a")

class PandasData(btfeeds.PandasData):
    lines = ('POC', 'PPOC','POC3w','PPOC3w', 'POCw', 'PPOCw',)

    params = (
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('close', 'Close'),
        ('volume', 'Volume'),
        ('openinterest', None),
        ('POC', -1),
        ('PPOC', -1),
        ('POC3w', -1),
        ('PPOC3w', -1),
        ('POCw', -1),
        ('PPOCw', -1),
    )

class SimpleMovingAverage1(bt.Indicator):
    lines = ('smapoc', )
    params = (('period', 1),)

    plotlines = dict(smapoc=dict(_skipnan=True, marker='o', ls='none', markersize=2.0))

    def next(self):
        datasum = fsum(self.data.get(size=self.p.period))
        self.lines.smapoc[0] = datasum / self.p.period

class Slope(bt.Indicator):
    lines = ('slope',)
    params = dict(period=5)

    def next(self):
        y = pd.Series(self.data.get(size=self.p.period))
        x = pd.Series(range(0,y.shape[0]))	
        m = (len(x) * np.sum(x*y) - np.sum(x) * np.sum(y)) / (len(x)*np.sum(x*x) - np.sum(x) ** 2)
        self.lines.slope[0] = round(m, 4)

class MultiDataStrategy(bt.Strategy):
    '''
    This strategy operates on 2 datas. The expectation is that the 2 datas are
    correlated and the 2nd data is used to generate signals on the 1st
      - Buy/Sell Operationss will be executed on the 1st data
      - The signals are generated using a Simple Moving Average on the 2nd data
        when the close price crosses upwwards/downwards
    The strategy is a long-only strategy
    '''
    params = dict(
        stake=1,
        printout=True,
        anchor = 5.0, target = 8.0, fstop = -2, tstop = 3.0, bestop = 1.0, atrl = 2.5, pslope=5
        )

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))
            print('%s, %s' % (dt.isoformat(), txt), file=f)

    def notify_order(self, order):
        print('{}: Order ref: {} / Type {} / Status {}'.format(
            self.data.datetime.datetime(0),
            order.ref, 'Buy' * order.isbuy() or 'Sell',
            order.getstatusname()))
        print('{}: Order ref: {} / Type {} / Status {}'.format(
            self.data.datetime.datetime(0),
            order.ref, 'Buy' * order.isbuy() or 'Sell',
            order.getstatusname()), file=f)

        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Size: %.2f, Ref: %d, Price: %.5f, Cost: %.5f' %
                    (order.size,order.ref,order.executed.price,
                    order.executed.value))
                
                self.buyprice = order.executed.price
                
            else:  # Sell
                self.log('SELL EXECUTED, Size: %.2f, Ref: %d,Price: %.5f, Cost: %.5f' %
                        (order.size,order.ref,order.executed.price,
                        order.executed.value))
        
            self.bar_executed = len(self)

        elif order.status in [order.Expired, order.Canceled, order.Margin,  order.Rejected]:
            self.log('%s ,' % order.Status[order.status])
            pass  # Simply log

        if not order.alive() and order.ref in self.orefs:
            self.orefs.remove(order.ref)

        # Allow new orders
        self.orderid = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.tradespnl = trade.history[-1].status.pnlcomm
        self.log('OPERATION PROFIT, GROSS %.5f, NET %.5f' %
                (trade.pnl, trade.pnlcomm))    

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.set_tradehistory(True)
        # Indicators
        poc = SimpleMovingAverage1(self.data0.POC, period=1, plotname='POC', subplot=False)
        ppoc = SimpleMovingAverage1(self.data0.PPOC, period=1, plotname='PPOC', subplot=False)
        poc3w = SimpleMovingAverage1(self.data0.POC3w, period=1, plotname='POC3w', subplot=False)
        pocw = SimpleMovingAverage1(self.data0.POCw, period=1, plotname='POCw', subplot=False)
        sma80 = btind.SimpleMovingAverage(self.data1.close, period=80, plotname='SMA80')
        sma240 = btind.SimpleMovingAverage(self.data1.close, period=240, plotname='SMA240')
        self.atr = bt.talib.ATR(self.data0.high, self.data0.low, self.data0.close, timeperiod=14)
        self.rsi = btind.RSI_EMA(self.data1.close, period=14)
        self.macd = btind.MACDHistogram(self.data1.close)
        #self.ematr = btind.ExponentialMovingAverage(self.atr, period=89)
        self.signal1 = btind.CrossOver(self.data0.close, poc, plot=False)
        self.signal2 = btind.CrossOver(self.data0.close, ppoc, plot=False)
        self.sma80_slope = Slope(sma80, period=self.p.pslope, plot=False)
        self.sma240_slope = Slope(sma240, period=self.p.pslope, plot=False)
        self.macd_slope = Slope(self.macd, period=self.p.pslope, plot=False)
        self.rsi_slope = Slope(self.rsi, period=self.p.pslope, plot=False)
        self.diffPOC = (self.data0.POC - self.data0.PPOC)
        self.diffPOC3w = (self.data0.POC - self.data0.POC3w)
        self.diffPOCw = (self.data0.POC - self.data0.POCw)
        self.diffClPOC = (self.data0.close - self.data0.POC)
        #self.diffClPPOC = (self.data0.close - self.data0.PPOC)
        self.diffClPOC3w = (self.data0.close - self.data0.POC3w)
        self.diffClPPOC3w = (self.data0.close - self.data0.PPOC3w)
        self.diffClPOCw = (self.data0.close - self.data0.POCw)
        self.diffClPPOCw = (self.data0.close - self.data0.PPOCw)
        self.diffSMA = (sma80 - sma240)
        self.flag=True
        self.counter=0
        self.nlost = 0

    def next(self):
        global o1, o2, ots

        pos = self.getposition(self.data0)  # for the default data (aka self.data0 and aka self.datas[0])
        comminfo = self.broker.getcommissioninfo(self.data0)
        pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        if self.orderid:
            return  # if an order is active, no new orders are allowed
        if self.p.printout:
            txt = ','.join(
            #['%04d' % len(self.data0),
            [self.data0.datetime.datetime(0).isoformat(),
             '''\nSlopeSMA80 : %f \nSlopeSMA240 : %f \nSlopeMACD : %f \nSlopeRSI : %f \nDifPOC-PPOC : %f \nDifPOC-POC3w : %f \nDifPOC-POCw : %f \nDifCloPOC : %f \nDifCloPOC3w : %f \nDifCloPOCw : %f \nDifSMA : %f''' % (
                 self.sma80_slope[0], self.sma240_slope[0], self.macd_slope[0], self.rsi_slope[0],
               self.diffPOC[0], self.diffPOC3w[0], self.diffPOCw[0], self.diffClPOC[0], self.diffClPOC3w[0], self.diffClPOCw[0], self.diffSMA[0])])
        print(txt, file=f1)

        if not self.position:  # not yet in market

            if self.counter > 2:

                if (self.tradespnl > 0):
                    self.nlost = 1

                elif (self.tradespnl < 0):
                    self.nlost = 2

            if (self.data.datetime.date().weekday() == 4) and (self.data.datetime.time().hour == 16) and (self.data.datetime.time().minute >= 1) and not self.flag:
                if o1.alive():
                    self.broker.cancel(o1)

            if (self.data.datetime.date().weekday() == 6) and (self.data.datetime.time().hour == 18) and (self.data.datetime.time().minute >= 15) and not self.flag:
                self.flag = True

            if ((self.signal1 == 1.0) or (self.signal1 == -1.0) or (self.signal2 == 1.0) or (self.signal2 == -1.0)) and self.flag and (self.counter <= 2):

                o1 = self.buy(exectype = bt.Order.Stop, price=(self.data0.close + self.p.anchor), size=self.p.stake, transmit=False)
             
                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price))

                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price), file=f)

                ots1 = self.sell(size=self.p.stake, exectype=bt.Order.StopTrail, price=o1.price+self.p.fstop, trailamount=self.p.tstop, transmit=True, parent=o1)
                
                o2 = self.sell(exectype=bt.Order.Stop,price=(self.data0.close - self.p.anchor), size=self.p.stake, transmit=False, oco=o1)
                
                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price))

                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price), file=f)


                ots2 = self.buy(size=self.p.stake, exectype=bt.Order.StopTrail, price=o2.price-self.p.fstop, trailamount=self.p.tstop, transmit=True, parent=o2)

                self.orefs = [o1.ref, o2.ref, ots1.ref, ots2.ref]
                self.flag=False
                self.counter += 1

            elif ((self.signal1 == 1.0) or (self.signal1 == -1.0) or (self.signal2 == 1.0) or (self.signal2 == -1.0)) and self.flag and (self.nlost == 1):

                o1 = self.buy(exectype = bt.Order.Stop, price=(self.data0.close + self.p.anchor), size=self.p.stake, transmit=False)
             
                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price))

                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price), file=f)

                ots1 = self.sell(size=self.p.stake, exectype=bt.Order.StopTrail, price=o1.price+self.p.fstop, trailamount=self.p.tstop, transmit=True, parent=o1)
                
                o2 = self.sell(exectype=bt.Order.Stop,price=(self.data0.close - self.p.anchor), size=self.p.stake, transmit=False, oco=o1)
                
                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price))

                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price), file=f)


                ots2 = self.buy(size=self.p.stake, exectype=bt.Order.StopTrail, price=o2.price-self.p.fstop, trailamount=self.p.tstop, transmit=True, parent=o2)

                self.orefs = [o1.ref, o2.ref, ots1.ref, ots2.ref]
                self.flag=False

            elif ((self.signal1 == 1.0) or (self.signal1 == -1.0) or (self.signal2 == 1.0) or (self.signal2 == -1.0)) and self.flag and (self.nlost > 1):

                o1 = self.buy(exectype = bt.Order.Stop, price=(self.data0.close + self.p.anchor), size=self.p.stake*self.nlost, transmit=False)
             
                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price))

                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price), file=f)

                ots1 = self.sell(size=self.p.stake*self.nlost, exectype=bt.Order.StopTrail, price=o1.price+self.p.fstop, trailamount=abs(0.25*np.ceil((self.tradespnl/o1.size/mult)/0.25)+self.p.fstop),
                 transmit=True, parent=o1)
                
                o2 = self.sell(exectype=bt.Order.Stop,price=(self.data0.close - self.p.anchor), size=self.p.stake*self.nlost, transmit=False, oco=o1)
                
                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price))

                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price), file=f)

                ots2 = self.buy(size=self.p.stake*self.nlost, exectype=bt.Order.StopTrail, price=o2.price-self.p.fstop, trailamount=abs(0.25*np.ceil((self.tradespnl/o2.size/mult)/0.25)+self.p.fstop),
                 transmit=True, parent=o2)

                self.orefs = [o1.ref, o2.ref, ots1.ref, ots2.ref]
                self.flag=False

        else:

            '''if (pnl > self.p.target*mult) and not self.trailing:
                self.close()
                self.broker.cancel(ot)
                self.flag =True'''

            if (pos.size>=self.p.stake) or (pos.size<=self.p.stake*-1):
                self.flag =True

    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')

def runstrategy():

    args = parse_args()

    # Create a cerebro
    cerebro = bt.Cerebro()

    # Add the 1st data to cerebro
    df=PandasData(dataname=data, name ="Minutes Data", timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.adddata(df)

    # Create the 2nd data

    data1 = cerebro.resampledata(df, timeframe=bt.TimeFrame.Minutes,
                             compression=1)
    
    # Add the 2nd data to cerebro

    # Add the strategy
    cerebro.addstrategy(MultiDataStrategy,
                        stake=args.stake)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(args.cash)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(
        commission=commission, margin=margin, mult=mult)

    # Add the analyzers we are interested in
    cerebro.addanalyzer(SharpeRatio, timeframe=bt.TimeFrame.Years)
    cerebro.addanalyzer(SQN, _name="sqn")
    cerebro.addanalyzer(DrawDown)

    cerebro.addwriter(bt.WriterFile, csv=args.writercsv, rounding=4)
    cerebro.addwriter(bt.WriterFile, csv=args.writercsv, rounding=4, out=f)

    # And run it
    cerebro.run(runonce=not args.runnext,
                preload=not args.nopreload,
                oldsync=args.oldsync)

    f.close()
    f1.close()

    # Plot if requested
    #if args.plot:
    cerebro.plot(numfigs=args.numfigs, volume=True, zdown=False)

def parse_args():
    parser = argparse.ArgumentParser(description='MultiData Strategy')

    parser.add_argument('--cash', default=100000, type=int,
                        help='Starting Cash')

    parser.add_argument('--runnext', action='store_true',
                        help='Use next by next instead of runonce')

    parser.add_argument('--nopreload', action='store_true',
                        help='Do not preload the data')

    parser.add_argument('--oldsync', action='store_true',
                        help='Use old data synchronization method')

    parser.add_argument('--commperc', default=0.000, type=float,
                        help='Percentage commission (0.005 is 0.5%%')

    parser.add_argument('--stake', default=1, type=int,
                        help='Stake to apply in each operation')

    parser.add_argument('--plot', '-p', action='store_true',
                        help='Plot the read data')

    parser.add_argument('--writercsv', '-wcsv', action='store_true',
                        help='Tell the writer to produce a csv stream')

    parser.add_argument('--numfigs', '-n', default=1,
                        help='Plot using numfigs figures')

    return parser.parse_args()


if __name__ == '__main__':
    runstrategy()