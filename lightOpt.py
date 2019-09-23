from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import argparse
import datetime
import pandas as pd
import numpy as np
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from backtrader.analyzers import (SQN, SharpeRatio, DrawDown)

data = pd.read_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/ES1M.csv', parse_dates=True, index_col='Date')
commission, margin, mult = 0.85, 50, 50.0

#f = open("/home/milo/Dropbox/Codigos/Scripts/Repos/POC/Backtestings/logbookOpt.txt", "a")


class PandasData(btfeeds.PandasData):
    lines = ('POC', 'PPOC',)

    params = (
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('close', 'Close'),
        ('volume', 'Volume'),
        ('openinterest', None),
        ('POC', -1),
        ('PPOC', -1),
    )

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
        anchor = 3.0, expand = 1.0, target = 5.0, fstop = -4.5, tstop = 1.50, bestop = 0.50, atrl = 1.5,
        )

    def notify_order(self, order):

        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                self.buyprice = order.executed.price

            self.bar_executed = len(self)

        elif order.status in [order.Expired, order.Canceled, order.Margin,  order.Rejected]:
            pass  # Simply log

        if not order.alive() and order.ref in self.orefs:
            self.orefs.remove(order.ref)

        # Allow new orders
        self.orderid = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

    def __init__(self):
        # To control operation entries
        self.orderid = None
        # Indicators
        poc = btind.SimpleMovingAverage(self.data0.POC, period=1, plotname='POC', subplot=False,)
        ppoc = btind.SimpleMovingAverage(self.data0.PPOC, period=1, plotname='PPOC', subplot=False,)
        self.atr = bt.talib.ATR(self.data0.high, self.data0.low, self.data0.close, timeperiod=14)
        #self.rsi = bt.talib.RSI(self.data0.close, timeperiod=14)
        #self.macd = bt.talib.MACD(self.data0.close)
        self.ematr = btind.ExponentialMovingAverage(self.atr, period=89)
        self.signal1 = btind.CrossOver(self.data0.close, poc, plot=False)
        self.signal2 = btind.CrossOver(self.data0.close, ppoc, plot=False)
        self.flag=True
        self.flag1 = False

    def next(self):
        global o1, o2, o3, o4, o5, o6, o7, o1pnl, o2pnl, o3pnl, o4pnl, o5pnl, o6pnl, ot

        pos = self.getposition(self.data0)  # for the default data (aka self.data0 and aka self.datas[0])
        comminfo = self.broker.getcommissioninfo(self.data0)
        pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        if self.orderid:
            return  # if an order is active, no new orders are allowed
        '''if self.p.printout:
            txt = ','.join(
            ['%04d' % len(self.data0),
            self.data0.datetime.datetime(0).isoformat(),
            'Profit : %d' % pnl])
        print(txt)'''

        if not self.position:  # not yet in market
            #self.trailing = True
            if (self.data.datetime.date().weekday() == 4) and (self.data.datetime.time().hour == 16) and (self.data.datetime.time().minute >= 1) and not self.flag:
                if o1.alive():
                    self.broker.cancel(o1)

            if self.flag1:
                if o3.alive():
                    self.broker.cancel(o3)

                if not self.trailing:
                    if ot.alive():
                        self.broker.cancel(ot)

            if (self.data.datetime.date().weekday() == 6) and (self.data.datetime.time().hour == 18) and (self.data.datetime.time().minute >= 15) and not self.flag:
                if (len(self.orefs)==0):
                    self.flag =True

            if ((self.signal1 == 1.0) or (self.signal1 == -1.0) or (self.signal2 == 1.0) or (self.signal2 == -1.0)) and self.flag and self.atr > self.ematr:

                o1 = self.buy(exectype = bt.Order.Stop, price=(self.data0.close + self.p.anchor), size=self.p.stake)
                
                o2 = self.sell(exectype=bt.Order.Stop,price=(self.data0.close - self.p.anchor), size=self.p.stake, oco=o1)

                self.orefs = [o1.ref, o2.ref]
                self.flag=False
                self.trailing = True
        else:

            if self.atr[-1] > self.p.atrl:
                self.p.expand = 2.0
            else:
                self.p.expand = 1.0

            if (pos.size==self.p.stake) and (len(self.orefs)==0):

                o3 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.p.anchor*2*self.p.expand)),
                            size=self.p.stake*3)

                self.orefs.append(o3.ref)

                self.flag1 = True

            elif(pos.size==self.p.stake*-1) and (len(self.orefs)==0):

                o3 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.p.anchor*2*self.p.expand)),
                            size=self.p.stake*3)

                self.orefs.append(o3.ref)

                self.flag1 = True

            if (pnl > self.p.tstop*mult) and (pos.size==self.p.stake) and self.trailing:
                self.broker.cancel(o3)
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-self.p.bestop), size=self.p.stake)
                self.trailing = False
                self.flag = True
                self.orefs.append(ot.ref)

            elif (pnl > self.p.tstop*mult) and (pos.size==self.p.stake*-1) and self.trailing:
                self.broker.cancel(o3)
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+self.p.bestop), size=self.p.stake)
                self.trailing = False
                self.flag = True
                self.orefs.append(ot.ref)

            elif (pnl > 3.0*mult) and (pnl < 5.0*mult) and (pos.size==self.p.stake) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-self.p.bestop), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 3.0*mult) and (pnl < 5.0*mult) and (pos.size==self.p.stake*-1) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+self.p.bestop), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 5.0*mult) and (pos.size==self.p.stake) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-self.p.bestop), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 5.0*mult) and (pos.size==self.p.stake*-1) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+self.p.bestop), size=self.p.stake)
                self.orefs.append(ot.ref)

            if (pnl > self.p.target*mult) and not self.trailing:
                self.close()
                self.broker.cancel(ot)
                self.flag =True

            if (pos.size==self.p.stake*2):
                if (len(self.orefs)==0):
                    o2pnl = round((o3.executed.price-o2.executed.price)*(o2.executed.size+o3.executed.size)*(mult/2), 5)
                    o4 = self.sell(exectype=bt.Order.Stop, price=(float(o2.executed.price)-(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*6)

                    self.orefs.append(o4.ref)

                if (pnl > (25 + abs(o2pnl))):
                    self.close()
                    self.broker.cancel(o4)
                    self.flag =True

            elif (pos.size==self.p.stake*2*-1):                        
                if (len(self.orefs)==0):
                    o2pnl = round((o3.executed.price-o1.executed.price)*(o1.executed.size+o3.executed.size)*(mult/2), 5)
                    o4 = self.buy(exectype=bt.Order.Stop, price=(float(o1.executed.price)+(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*6)

                    self.orefs.append(o4.ref)

                if (pnl > (25 + abs(o2pnl))):
                    self.close()
                    self.broker.cancel(o4)
                    self.flag =True
            
            if (pos.size==self.p.stake*4):
                if (len(self.orefs)==0):
                    o3pnl = round((o4.executed.price-o3.executed.price)*(o3.executed.size+o4.executed.size)*(mult/1.5), 5)                  
                    o5 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*12)

                    self.orefs.append(o5.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl))):
                    self.close()
                    self.broker.cancel(o5)
                    self.flag =True

            elif (pos.size==self.p.stake*4*-1):
                if (len(self.orefs)==0):
                    o3pnl = round((o4.executed.price-o3.executed.price)*(o3.executed.size+o4.executed.size)*(mult/1.5), 5)
                    o5 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*12)

                    self.orefs.append(o5.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl))):
                    self.close()
                    self.broker.cancel(o5)
                    self.flag =True

            if (pos.size==self.p.stake*8):
                if (len(self.orefs)==0):
                    o4pnl = round((o5.executed.price-o4.executed.price)*(o4.executed.size+o5.executed.size)*(mult/1.5), 5)

                    o6 = self.sell(exectype=bt.Order.Stop, price=(float(o2.executed.price)-(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*24)

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl))):
                    self.close()
                    self.broker.cancel(o6)
                    self.flag =True

            elif (pos.size==self.p.stake*8*-1):
                if (len(self.orefs)==0):
                    o4pnl = round((o5.executed.price-o4.executed.price)*(o4.executed.size+o5.executed.size)*(mult/1.5), 5)

                    o6 = self.buy(exectype=bt.Order.Stop, price=(float(o1.executed.price)+(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*24)

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl))):
                    self.close()
                    self.broker.cancel(o6)
                    self.flag =True

            '''elif (pos.size==self.p.stake*16):
                if (len(self.orefs)==0):
                    o5pnl = round((o6.executed.price-o5.executed.price)*(o5.executed.size+o6.executed.size)*(mult/1.5), 5)
                    print(o5pnl)
                    o7 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*48)
                
                    print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                        o7.ref, o7.size, o7.price))

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl+ o5pnl))):
                    self.close()
                    self.broker.cancel(o7)
                    self.flag =True

            elif (pos.size==self.p.stake*16*-1):
                if (len(self.orefs)==0):
                    o5pnl = round((o6.executed.price-o5.executed.price)*(o5.executed.size+o6.executed.size)*(mult/1.5), 5)
                    print(o5pnl)
                    o7 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.p.anchor*2*self.p.expand)),
                    size=self.p.stake*48)
                
                    print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                        o7.ref, o7.size, o7.price))

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl+ o5pnl))):
                    self.close()
                    self.broker.cancel(o7)
                    self.flag =True'''             

            if (pos.size==self.p.stake*16) or (pos.size==self.p.stake*16*-1):
                o5pnl = round((o6.executed.price-o5.executed.price)*(o5.executed.size+o6.executed.size)*(mult/1.5), 5)

                if (pnl > (25 + o2pnl + o3pnl + o4pnl + o5pnl)):
                    self.close()
                    self.flag=True

                elif (pnl < self.p.fstop*pos.size*mult):
                    self.close()
                    self.flag=True

def runstrategy():

    args = parse_args()

    # Create a cerebro
    cerebro = bt.Cerebro(optreturn=False)

    # Add the 1st data to cerebro
    df=PandasData(dataname=data, name ="Seconds Data", timeframe=bt.TimeFrame.Seconds, compression=1)
    cerebro.adddata(df)

    # Create the 2nd data
    '''data1 = data.resample('H').last()
    data1['open'] = data.resample('H').first()
    data1['high'] = data.resample('H').max()
    data1['low'] = data.resample('H').min()
    data1 = data1.dropna()
    
    # Add the 2nd data to cerebro
    df1=PandasData(dataname=data1, name="Hourly Data", timeframe=bt.TimeFrame.Minutes, compression=60)
    cerebro.adddata(df1)'''

    # Add the strategy
    cerebro.optstrategy(MultiDataStrategy,
                        stake=args.stake,target=range(3,11), fstop=range(-5,0),
                        )

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(args.cash)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(
        commission=commission, margin=margin, mult=mult)

    # Add the analyzers we are interested in
    '''cerebro.addanalyzer(SharpeRatio, timeframe=bt.TimeFrame.Years)
    cerebro.addanalyzer(SQN, _name="sqn")
    cerebro.addanalyzer(DrawDown)

    cerebro.addwriter(bt.WriterFile, csv=args.writercsv, rounding=4)
    cerebro.addwriter(bt.WriterFile, csv=args.writercsv, rounding=4, out=f)'''

    # And run it
    opt_runs = cerebro.run()

        # Generate results list
    final_results_list = []
    for run in opt_runs:
        for strategy in run:
            value = round(strategy.broker.get_value(),2)
            PnL = round(value - args.cash,2)
            anchor = strategy.p.anchor
            target = strategy.p.target
            fstop = strategy.p.fstop
            final_results_list.append([anchor, target, fstop, PnL])

    #Sort Results List
    by_PnL = sorted(final_results_list, key=lambda x: x[3], reverse=True)

    #Print results
    print('Results: Ordered by Profit:')
    for result in by_PnL:
        print('Anchor: {}, PnL: {}, Target,: {}, Traling,: {}'.format(result[0], result[3], result[1], result[2]))

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
    #f.close()