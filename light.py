from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import argparse
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from backtrader.analyzers import (SQN, SharpeRatio, DrawDown)
from market_profile import MarketProfile
import time

data = pd.read_csv('C:/Users/MiloZB/Dropbox/Codigos/Data/ES_1sec(2018)NinjaPOC.csv', parse_dates=True, index_col='Date')
commission, margin, mult = 0.85, 50, 50.0

class PandasData(btfeeds.PandasData):
    lines = ('POC', 'PPOC', )

    '''plotlines = dict(POC=dict(_name='POC', style='.'),
                 PPOC=dict(_name='PPOC', style='.'))'''

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
        printout=True,)

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        print('{}: Order ref: {} / Type {} / Status {}'.format(
            self.data.datetime.datetime(0),
            order.ref, 'Buy' * order.isbuy() or 'Sell',
            order.getstatusname()))

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
        self.log('OPERATION PROFIT, GROSS %.5f, NET %.5f' %
                (trade.pnl, trade.pnlcomm))    

    def __init__(self):
        # To control operation entries
        self.orderid = None
        # Indicators
        poc = btind.SimpleMovingAverage(self.data0.POC, period=1, plotname='POC')
        ppoc = btind.SimpleMovingAverage(self.data0.PPOC, period=1, plotname='PPOC')
        self.atr = bt.talib.ATR(self.data0.high, self.data0.low, self.data0.close, timeperiod=14)
        #self.rsi = bt.talib.RSI(self.data0.close, timeperiod=14)
        #self.macd = bt.talib.MACD(self.data0.close)
        self.ematr = btind.ExponentialMovingAverage(self.atr, period=89)
        self.anchor = 2.0
        self.expand = 1.0
        self.signal1 = btind.CrossOver(self.data0.close, poc, plot=False)
        self.signal2 = btind.CrossOver(self.data0.close, ppoc, plot=False)
        self.target = 5.0
        self.flag=True

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
            self.trailing = True
            if (self.data.datetime.date().weekday() == 4) and (self.data.datetime.time().hour == 16) and (self.data.datetime.time().minute == 1) and (self.data.datetime.time().second == 50) and (self.flag == False):
                self.broker.cancel(o1)

            if (self.data.datetime.date().weekday() == 6) and (self.data.datetime.time().hour == 18) and (self.data.datetime.time().minute == 15) and (self.data.datetime.time().second == 0):
                self.flag =True

            if ((self.signal1 == 1.0) or (self.signal1 == -1.0) or (self.signal2 == 1.0) or (self.signal2 == -1.0)) and self.flag==True\
                and self.atr > self.ematr[-1]:

                o1 = self.buy(exectype = bt.Order.Stop, price=(self.data0.close + self.anchor), size=self.p.stake)
             
                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o1.ref, o1.size, o1.price))
                
                o2 = self.sell(exectype=bt.Order.Stop,price=(self.data0.close - self.anchor), size=self.p.stake, oco=o1)
                
                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o2.ref, o2.size, o2.price))

                self.orefs = [o1.ref, o2.ref]
                self.flag=False
        else:

            if self.atr[-1] > 2.5:
                self.expand = 2.0
            else:
                self.expand = 1.0

            if (pos.size==self.p.stake) and (len(self.orefs)==0):

                o3 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.anchor*2*self.expand)),
                            size=self.p.stake*3)
                
                print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                            o3.ref, o3.size, o3.price))

                self.orefs.append(o3.ref)

            elif(pos.size==self.p.stake*-1) and (len(self.orefs)==0):

                o3 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.anchor*2*self.expand)),
                            size=self.p.stake*3)
                
                print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                            o3.ref, o3.size, o3.price))

                self.orefs.append(o3.ref)

            #print(pnl)

            if (pnl > 1.50*mult) and (pos.size==self.p.stake) and self.trailing:
                self.broker.cancel(o3)
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-1.25), size=self.p.stake)
                self.trailing = False
                self.orefs.append(ot.ref)

            elif (pnl > 1.50*mult) and (pos.size==self.p.stake*-1) and self.trailing:
                self.broker.cancel(o3)
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+1.25), size=self.p.stake)
                self.trailing = False
                self.orefs.append(ot.ref)

            '''elif (pnl > 3.0*mult) and (pnl < 5.0*mult) and (pos.size==self.p.stake) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-1.0), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 3.0*mult) and (pnl < 5.0*mult) and (pos.size==self.p.stake*-1) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+1.0), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 5.0*mult) and (pos.size==self.p.stake) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.sell(exectype=bt.Order.Stop, price=(self.data0.close-1.0), size=self.p.stake)
                self.orefs.append(ot.ref)

            elif (pnl > 5.0*mult) and (pos.size==self.p.stake*-1) and (len(self.orefs)==2):
                self.broker.cancel(ot) 
                ot = self.buy(exectype=bt.Order.Stop, price=(self.data0.close+1.0), size=self.p.stake)
                self.orefs.append(ot.ref)'''

            if (pnl > self.target*mult) and ot.alive():
                self.close()
                self.broker.cancel(ot)
                self.flag =True

            '''if (pnl > 50*2) and (pos.size == self.p.stake) and self.trailing:

                ot = self.sell(size=self.p.stake, exectype=bt.Order.StopTrail, trailamount=1.0, parent=o1)
                
                self.trailing = False

            if (pnl > 50*2) and (pos.size == self.p.stake) and self.trailing:

                ot = self.buy(size=self.p.stake, exectype=bt.Order.StopTrail, trailamount=1.0, parent=o2)

                self.trailing = False'''

            if (pos.size==self.p.stake*2):
                if (len(self.orefs)==0):
                    o2pnl = round((o3.executed.price-o2.executed.price)*(o2.executed.size+o3.executed.size)*(mult/2), 5)
                    print(o2pnl)
                    o4 = self.sell(exectype=bt.Order.Stop, price=(float(o2.executed.price)-(self.anchor*2*self.expand)),
                    size=self.p.stake*6)
                
                    print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                        o4.ref, o4.size, o4.price))

                    self.orefs.append(o4.ref)

                if (pnl > (25 + abs(o2pnl))):
                    self.close()
                    self.broker.cancel(o4)
                    self.flag =True

            elif (pos.size==self.p.stake*2*-1):                        
                if (len(self.orefs)==0):
                    o2pnl = round((o3.executed.price-o1.executed.price)*(o1.executed.size+o3.executed.size)*(mult/2), 5)
                    print(o2pnl) 
                    o4 = self.buy(exectype=bt.Order.Stop, price=(float(o1.executed.price)+(self.anchor*2*self.expand)),
                    size=self.p.stake*6)
                
                    print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                        o4.ref, o4.size, o4.price))

                    self.orefs.append(o4.ref)

                if (pnl > (25 + abs(o2pnl))):
                    self.close()
                    self.broker.cancel(o4)
                    self.flag =True
            
            if (pos.size==self.p.stake*4):
                if (len(self.orefs)==0):
                    o3pnl = round((o4.executed.price-o3.executed.price)*(o3.executed.size+o4.executed.size)*(mult/1.5), 5)
                    print(o3pnl)                    
                    o5 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.anchor*2*self.expand)),
                    size=self.p.stake*12)
                
                    print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                        o5.ref, o5.size, o5.price))

                    self.orefs.append(o5.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl))):
                    self.close()
                    self.broker.cancel(o5)
                    self.flag =True

            elif (pos.size==self.p.stake*4*-1):
                if (len(self.orefs)==0):
                    o3pnl = round((o4.executed.price-o3.executed.price)*(o3.executed.size+o4.executed.size)*(mult/1.5), 5)
                    print(o3pnl)
                    o5 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.anchor*2*self.expand)),
                    size=self.p.stake*12)
                
                    print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                        o5.ref, o5.size, o5.price))

                    self.orefs.append(o5.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl))):
                    self.close()
                    self.broker.cancel(o5)
                    self.flag =True

            if (pos.size==self.p.stake*8):
                if (len(self.orefs)==0):
                    o4pnl = round((o5.executed.price-o4.executed.price)*(o4.executed.size+o5.executed.size)*(mult/1.5), 5)
                    print(o4pnl)
                    o6 = self.sell(exectype=bt.Order.Stop, price=(float(o2.executed.price)-(self.anchor*2*self.expand)),
                    size=self.p.stake*24)
                
                    print(self.datetime.datetime(0),' : Oref %d / Sell Stop %d at %.5f' % (
                        o6.ref, o6.size, o6.price))

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl))):
                    self.close()
                    self.broker.cancel(o6)
                    self.flag =True

            elif (pos.size==self.p.stake*8*-1):
                if (len(self.orefs)==0):
                    o4pnl = round((o5.executed.price-o4.executed.price)*(o4.executed.size+o5.executed.size)*(mult/1.5), 5)
                    print(o4pnl)
                    o6 = self.buy(exectype=bt.Order.Stop, price=(float(o1.executed.price)+(self.anchor*2*self.expand)),
                    size=self.p.stake*24)
                
                    print(self.datetime.datetime(0),' : Oref %d / Buy Stop %d at %.5f' % (
                        o6.ref, o6.size, o6.price))

                    self.orefs.append(o6.ref)

                if (pnl > (25 + abs(o2pnl + o3pnl + o4pnl))):
                    self.close()
                    self.broker.cancel(o6)
                    self.flag =True

            '''elif (pos.size==self.p.stake*16):
                if (len(self.orefs)==0):
                    o5pnl = round((o6.executed.price-o5.executed.price)*(o5.executed.size+o6.executed.size)*(mult/1.5), 5)
                    print(o5pnl)
                    o7 = self.sell(exectype=bt.Order.Stop, price=(float(o1.executed.price)-(self.anchor*2*self.expand)),
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
                    o7 = self.buy(exectype=bt.Order.Stop, price=(float(o2.executed.price)+(self.anchor*2*self.expand)),
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
                print(o5pnl)
                print(pnl)
                if (pnl > (25 + o2pnl + o3pnl + o4pnl + o5pnl)):
                    self.close()
                    self.flag=True

                elif (pnl < -900.0):
                    self.close()
                    self.flag=True          

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
    df=PandasData(dataname=data, name ="Seconds Data", timeframe=bt.TimeFrame.Seconds, compression=1)
    cerebro.adddata(df)

    # Create the 2nd data
    '''data1 = data.resample('H').last()
    data1['open'] = data.resample('H').first()
    data1['high'] = data.resample('H').max()
    data1['low'] = data.resample('H').min()
    data1 = data1.dropna()
    data1['median'] = (data1.high + data1.low) / 2.0
    data1['count'] = range(0, len(data1))
    x_ = data1['count'].rolling(lrperiod).mean()
    y_ = data1['close'].rolling(lrperiod).mean()
    mx = data1['count'].rolling(lrperiod).std()
    my = data1['close'].rolling(lrperiod).std()
    c = data1['count'].rolling(lrperiod).corr(data1.close)
    slope = c * (my/mx)
    inter = y_ - slope * x_
    data1['lin_reg'] = data1['count'] * slope + inter
    
    # Add the 2nd data to cerebro
    df1=PandasData(dataname=data1, name="Hourly Data", timeframe=bt.TimeFrame.Minutes, compression=60)
    cerebro.adddata(df1)'''

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

    # And run it
    cerebro.run(runonce=not args.runnext,
                preload=not args.nopreload,
                oldsync=args.oldsync)

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