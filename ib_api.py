# built in
import time
import datetime
# import sys
import threading
import queue

# 3rd party
import pytz

# IB API
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.order import Order
from ibapi.tag_value import TagValue
from ibapi.order_state import OrderState
from ibapi.execution import Execution


class IBAPIWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        self.FINISHED = 'END'
        self._market_data_queues = {}
        self._portfolio_details = queue.Queue()
        self._orders_queue = queue.Queue()

    def error(self, id, errorCode, errorString):
        """ Formats the error messages coming from TWS. """
        error_message = f"IB Error ID ({id}), Error Code ({errorCode}) with response '{errorString}'"
        print(error_message)

    def currentTime(self, server_time):
        """
        Server's current time. This method will receive IB server's system time resulting after the invocation 
        of reqCurrentTime.
        """
        server_time = datetime.datetime.utcfromtimestamp(server_time).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Current server time is: {server_time}')

    def contractDetails(self, reqId, contractDetails):
        print(f"[reqId:{reqId}] Contract details: {contractDetails}")
        # TODO: missing overload for contractDetailsEnd method

    def tickPrice(self, reqId, tickType, price, attrib):
        symbol = self._reqDetails[reqId]['symbol']
        tickType_name = TickTypeEnum.to_str(tickType)
        print(f"[reqId: {reqId}], symbol:{symbol}, tickType: {tickType_name}({tickType}), Value: {price}")
        self._market_data_queues[reqId].put({
            'Symbol': symbol,
            'TickTypeName': tickType_name,
            'TickType': tickType,
            'Price': price
        })

    def tickSnapshotEnd(self, tickerId: int):
        print(f'Finished getting market data for reqId:{tickerId}')
        self._market_data_queues[tickerId].put(self.FINISHED)

    def updatePortfolio(self, contract:Contract, position:float, marketPrice:float, marketValue:float,
                        averageCost:float, unrealizedPNL:float, realizedPNL:float, accountName:str):
        str_msg = (
            f"[updatePortfolio] Symbol: {contract.symbol}, Position: {position}, Market Price: {marketPrice}, "
            f"Market Value: {marketValue} Average Cost: {averageCost}, Unrealized PNL: {unrealizedPNL}, "
            f"Realized PNL: {realizedPNL}"
        )
        print(str_msg)
        self._portfolio_details.put({
            'key': 'Position',
            'symbol': contract.symbol,
            'positionCnt': position,
            'marketPrice': marketPrice,
            'marketValue': marketValue,
            'averageCost': averageCost,
            'unrealizedPNL': unrealizedPNL,
            'realizedPNL': realizedPNL
        })

    def updateAccountValue(self, key:str, val:str, currency:str, accountName:str):
        """
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#ae15a34084d9f26f279abd0bdeab1b9b5
        """
        print(f"[updateAccountValue] Key: {key}, Value: {val}, Currency: {currency}, Account Name: {accountName}")
        self._portfolio_details.put({
            'key': key,
            'val': val,
            'currency': currency,
        })

    def updateAccountTime(self, timeStamp:str):
        print(f"[updateAccountTime] Time: {timeStamp}")

    def accountDownloadEnd(self, accountName: str):
        print('Got all Account Updates!')
        self._portfolio_details.put(self.FINISHED)

    def orderStatus(self, orderId:int , status:str, filled:float, remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int, whyHeld:str, mktCapPrice: float):
        """
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a17f2a02d6449710b6394d0266a353313
        """
        str_msg = (
            f"[orderStatus] orderId: {orderId}, Status: {status}, Filled: {filled}, "
            f"Remaining: {remaining}, Last Fill Price: {lastFillPrice}"
        )
        print(str_msg)
        self._orders_queue.put({
            'callback': 'orderStatus',
            'orderId': orderId,
            'status': status,
            'filled': filled,
            'remaining': remaining,
            'lastFillPrice': lastFillPrice,
        })

    def openOrder(self, orderId:int, contract:Contract, order:Order, orderState:OrderState):
        """
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#aa05258f1d005accd3efc0d60bc151407
        """
        str_msg = (
            f"[openOrder] orderId: {orderId}, Symbol: {contract.symbol}, {contract.secType} at {contract.exchange}: "
            f"{order.action}, {order.orderType}, {order.totalQuantity}. Order state: {orderState.status}"
        )
        print(str_msg)
        self._orders_queue.put({
            'callback': 'openOrder',
            'orderId': orderId,
            'symbol': contract.symbol,
            'exchange': contract.exchange,
            'orderAction': order.action,
            'orderType': order.orderType,
            'totalQuantity': order.totalQuantity,
            'status': orderState.status,
        })

    def openOrderEnd(self):
        print('Got all callbacks from openOrder!')
        self._orders_queue.put(self.FINISHED)

    def execDetails(self, reqId:int, contract:Contract, execution:Execution):
        """
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#a09f82de3d0666d13b00b5168e8b9313d
        """
        str_msg = (
            f"[execDetails] reqId: {reqId}, Symbol: {contract.symbol}, {contract.secType} in {contract.currency}. "
            f"execId: {execution.execId}, orderId: {execution.orderId}, Shares: {execution.shares}, "
            f"Last Liquidity: {execution.lastLiquidity}"
        )
        print(str_msg)
        # TODO: implement
        pass

    def execDetailsEnd(self, reqId:int):
        """
        https://interactivebrokers.github.io/tws-api/interfaceIBApi_1_1EWrapper.html#ac9b605c48d60da99ef595d2bc7ca39e2
        """
        # TODO: implement
        pass


class IBAPIApp(IBAPIWrapper, EClient):
    """
    port : `int`
        The port to connect to TWS/IB Gateway with
    clientId : `int`
        An (arbitrary) client ID, that must be a positive integer
    """
    def __init__(self, port=None, clientId=None):
        """
        currencies: iterable
            Iterable with currencies codes that should be considered by API
        base_currency: str
            Currency code for base currency
        """
        IBAPIWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None

        self._nextReqId = 0
        self._reqDetails = {}

        # Connects to the IB server with the appropriate connection parameters
        self.connect(
            '127.0.0.1', port, clientId=clientId
        )

        # Initialise the threads for various components
        thread = threading.Thread(target=self.run)
        thread.start()
        self._thread = thread

    def get_reqId(self):
        """Note, this is NOT order id."""
        reqId = self._nextReqId
        self._nextReqId += 1
        return reqId

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def get_contract(self, symbol=None, secType='STK', exchange='SMART', primaryExchange='LSE', currency='GBP'):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        # optional params for Contract definition
        if primaryExchange != None:
            contract.primaryExchange = primaryExchange
        return contract

    def create_order(self, action=None, quantity=None, orderType=None, lmtPrice=None, adaptive=False, adaptivePriority=None,
        trailingPercent=None, trailStopPrice=None):
        """
        Creates order definition.
        Base order types: (https://www.interactivebrokers.co.uk/en/index.php?f=41254)
            LMT: "Limit", buy or sell a contract ONLY at the specified price or better
            MKT: "Market", buy or sell an asset at the bid or offer price currently available. You have no guarantee that the order 
                  will execute at any specific price.
            MOC: "Market on Close", a market order that is submitted to execute as close to the closing price as possible
            TRAIL: "Trailing Stop"
                - after SL is triggered it becomes like MKT
                - Usualy you just need to set trailing (TRL) amount
                - https://www.interactivebrokers.co.uk/en/index.php?f=37800
            TRAIL LIMIT: "Trailing Stop Limit"
                - after SL is triggered order becomes like LMT
                - set STP and TRL price

        On top of the basic order types, it is possible to make use of the 'advanced' aglos. E.g.:
            - "Adaptive Limit", "Adaptive Market":
                - https://interactivebrokers.github.io/tws-api/ibalgos.html#adaptive
                - adaptivePriority: Urgent, Normal, Patient
        """
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = orderType
        if lmtPrice != None:
            order.lmtPrice = lmtPrice
        if trailingPercent != None:
            # as int. e.g. if 5% then 5
            order.trailingPercent = trailingPercent
        if trailStopPrice != None:
            order.trailStopPrice = trailStopPrice
        if adaptive == True:
            if adaptivePriority == None:
                adaptivePriority = 'Normal'
            order.algoStrategy = "Adaptive"
            order.algoParams = []
            order.algoParams.append(TagValue("adaptivePriority", adaptivePriority))
        return order

    def get_current_price(self, contract=None, MarketDataType=None, tickTypes='', stream=False, timeout=10):
        """
        MarketDataType : `int`
            1: default, live data. won't be enable for most instruments. unless there is subscription
            2: "Frozen", like 1, but after sessions is closed
            3: "Delayed", if user does not have subscritpion for instruments where it is required
            4: "Deleyed-Frozen", like 3, but after sesssion is closed
        """
        now = datetime.datetime.now(pytz.timezone('Europe/London'))
        dow =  now.weekday() # 0-Monday, 6-Sunday
        hour = now.hour
        # The London stock exchange opens at 08:00 UK time and closes at 16:00, Monday-Friday
        if MarketDataType == None:
            if (dow >= 0 and dow <= 4) and (hour >= 8 and hour <= 16):
                MarketDataType = 3
            else:
                MarketDataType = 4
        self.reqMarketDataType(MarketDataType)
        # send request
        symbol = contract.symbol
        print(f'Requesting market data for: {symbol}')
        reqId = self.get_reqId()
        self._market_data_queues[reqId] = queue.Queue()
        self._set_req_details(reqId, 'reqMktData', symbol, now)
        if stream == False:
            snapshot = True # one time snapshot
        else:
            snapshot = False # streaming data
        self.reqMktData(reqId, contract, tickTypes, snapshot, False, [])
        _output = {'symbol': symbol}
        q = self._market_data_queues[reqId]
        t_start = datetime.datetime.now()
        while True:
            if not q.empty():
                msg = q.get()
                if msg == self.FINISHED:
                    break
                if msg['Price'] != 0:
                    _output[msg['TickTypeName'].replace('DELAYED_', '')] = msg['Price']
            t_cur = datetime.datetime.now()
            if (t_cur - t_start).seconds > timeout:
                print(f'Timed out from getting {symbol} market data')
                return None
        return _output

    def get_portfolio_details(self, timeout=10):
        self.reqAccountUpdates(True, '')
        # to stop publish frequent updates
        self.reqAccountUpdates(False, '')
        _output = {'positions': {}}
        q = self._portfolio_details
        t_start = datetime.datetime.now()
        while True:
            if not q.empty():
                msg = q.get()
                if msg == self.FINISHED:
                    break
                if msg['key'] == ('AccountCode', 'AvailableFunds'):
                    _output[msg['key']] = msg['val']
                elif msg['key'] in ('TotalCashBalance', 'UnrealizedPnL'):
                    _output[f"{msg['key']}_{msg['currency']}"] = msg['val']
                elif msg['key'] == 'Position':
                    _output['positions'][msg['symbol']] = {
                        'positionCnt': msg['positionCnt'],
                        'marketPrice': msg['marketPrice'],
                        'marketValue': msg['marketValue'],
                        'averageCost': msg['averageCost'],
                        'unrealizedPNL': msg['unrealizedPNL'],
                    }
            t_cur = datetime.datetime.now()
            if (t_cur - t_start).seconds > timeout:
                print('Timed out from getting portfolio details')
                return None
        return _output

    def get_current_orders(self, timeout=10):
        self.reqAllOpenOrders()
        q = self._orders_queue
        t_start = datetime.datetime.now()
        _orders = {}
        _symbols = set()
        _orderIds = set()
        while True:
            if not q.empty():
                msg = q.get()
                if msg == self.FINISHED:
                    break
                orderId = msg['orderId']
                _orderIds.add(orderId)
                if orderId not in _orders:
                    _orders[orderId] = {}
                if msg['callback'] == 'openOrder':
                    _symbols.add(msg['symbol'])
                    _orders[orderId]['symbol'] = msg['symbol']
                    _orders[orderId]['action'] = msg['orderAction']
                    _orders[orderId]['status'] = msg['status']
                    _orders[orderId]['orderType'] = msg['orderType']
                    _orders[orderId]['totalQuantity'] = msg['totalQuantity']
                elif msg['callback'] == 'orderStatus':
                    _orders[orderId]['filled'] = msg['filled']
                    _orders[orderId]['remaining'] = msg['remaining']
                    _orders[orderId]['lastFillPrice'] = msg['lastFillPrice']
            t_cur = datetime.datetime.now()
            if (t_cur - t_start).seconds > timeout:
                print('Timed out from getting current orders')
                return None
        _orders['orders'] = sorted(list(_orderIds))
        _orders['symbols'] = sorted(list(_symbols))
        return _orders

    def print_tickTypes(self):
        for i in range(91):
            print(TickTypeEnum.to_str(i), i)

    def _set_req_details(self, reqId, org_call, symbol, now_ts):
        self._reqDetails[reqId] = {
            'org_call': org_call,
            'symbol': symbol,
            'local_ts': now_ts.strftime('%Y-%m-%d::%H:%M:%S')
        }


def main():
    print("Launching IB API application...")
    app = IBAPIApp(
        port=7497,
        clientId=666,
    )
    print("Successfully launched IB API application...")

    # Obtain the server time via the IB API app
    app.reqCurrentTime()
    time.sleep(1)

    contracts = {}
    for symbol in ('III', 'ADM'):
        ctr = app.get_contract(symbol=symbol)
        contracts[symbol] = ctr
        # Get contract details
        app.reqContractDetails(app.get_reqId(), ctr)
        time.sleep(1)
        # Get price data
        mkt_data = app.get_current_price(contract=ctr, timeout=3)
        print(f"{symbol} market data: {mkt_data}")
        time.sleep(2)

    # Get portfolio details
    portfolio_details = app.get_portfolio_details()
    print(f'Portfolio details: {portfolio_details}')

    # Place order
    # print('Placing Adaptive Order')
    # adaptive_order = app.create_order(
    #     action='BUY', quantity=10, orderType='MKT', adaptive=True, adaptivePriority='Patient'
    # )
    # app.placeOrder(
    #     app.nextOrderId(),  # orderId
    #     contracts['ADM'],   # Contract
    #     adaptive_order,     # Order
    # )

    # print('Place SL order')
    # sl_order = app.create_order(
    #     action='SELL', quantity=10, orderType='TRAIL', trailingPercent=1
    # )
    # app.placeOrder(
    #     app.nextOrderId(),  # orderId
    #     contracts['ADM'],   # Contract
    #     sl_order,           # Order
    # )

    print('Getting current orders...')
    current_orders = app.get_current_orders()
    print('current_orders: ', current_orders)

    # Disconnect and finish execution
    # time.sleep(5)
    # app.disconnect()
    # print("Disconnected from the IB API application. Finished.")

if __name__ == '__main__':
    main()

"""
TODOs:
OK - v0 to do get price and details with prints only
OK - properly get portfolio details
OK - properly get market price
OK - place basic order
OK - place SL order
OK - get orders status
- should I wrap placing orders? so e.g. it is assured to be use correct orderId etc?


Useful tutorials for Python API:
- https://www.youtube.com/playlist?list=PL71vNXrERKUpPreMb3z1WGx6fOTCzMaH1
- https://www.quantstart.com/articles/connecting-to-the-interactive-brokers-native-python-api/
- https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/

"""



