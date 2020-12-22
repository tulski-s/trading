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

"""
Useful tutorials for Python API:
- https://www.youtube.com/playlist?list=PL71vNXrERKUpPreMb3z1WGx6fOTCzMaH1
- https://www.quantstart.com/articles/connecting-to-the-interactive-brokers-native-python-api/
- https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/
"""


class IBAPIWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        self.FINISHED = 'END'
        self._market_data = queue.Queue()
        self._portfolio_details = queue.Queue()

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

    def tickPrice(self, reqId, tickType, price, attrib):
        symbol = self._reqDetails[reqId]['symbol']
        tickType_name = TickTypeEnum.to_str(tickType)
        print(f"[reqId: {reqId}], symbol:{symbol}, tickType: {tickType_name}({tickType}), Value: {price}")

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

        self._nextValidReqId = 0
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
        reqId = self._nextValidReqId
        self._nextValidReqId += 1
        return reqId

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

    def get_current_price(self, contract=None, MarketDataType=None, tickTypes=''):
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
        self._setReqDetails(reqId, 'reqMktData', symbol, now)
        self.reqMktData(reqId, contract, tickTypes, False, False, [])

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

    def print_tickTypes(self):
        for i in range(91):
            print(TickTypeEnum.to_str(i), i)

    def _setReqDetails(self, reqId, org_call, symbol, now_ts):
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

    # Get contract details
    iii = app.get_contract(symbol='III')
    app.reqContractDetails(app.get_reqId(), iii)
    time.sleep(1)

    # Get price data
    # Note... this asks for frequent updates... if you live program running you will get more updates...
    # is there End function for this one too?
    app.get_current_price(contract=iii)
    time.sleep(2)

    # Get portfolio details
    portfolio_details = app.get_portfolio_details()
    print(f'Portfolio details: {portfolio_details}')

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
- properly get market price

"""



