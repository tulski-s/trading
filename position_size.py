# built-in
from abc import ABCMeta, abstractmethod
import random

from commons import (
    get_parser,
    setup_logging,
)


class PositionSize(metaclass=ABCMeta):
    """
    Base class for all position sizers. Implements common method across all of them.
    """
    def __init__(self, sort_type='alphabetically', logger=None, debug=False):
        self.log = setup_logging(logger=logger, debug=debug)
        self.fee_perc = 0.0038  # 0.38%
        self.min_fee = 4  # 4 PLN
        self.sort_type = sort_type

    @abstractmethod
    def decide_what_to_buy(self, available_money_at_time, candidates):
        pass

    def calculate_fee(self, transaction_value):
        """Calculates expected transaction fee."""
        fee = transaction_value * self.fee_perc
        if fee < self.min_fee:
            fee = self.min_fee
        return round(fee, 2)
    
    def sort(self, candidates):
        _candidates = candidates.copy()
        if self.sort_type == 'alphabetically':
            _candidates.sort(key=lambda c: c['symbol'])
        elif self.sort_type == 'random':
            random.shuffle(_candidates)
        elif self.sort_type == 'cheapest':
            _candidates.sort(key=lambda c: c['price'])
        elif self.sort_type == 'expensive':
            _candidates.sort(key=lambda c: c['price'], reverse=True)
        return _candidates

    def get_shares_count(self, money, price):
        return money // (price + (price*self.fee_perc))

    def _define_symbol_to_buy(self, candidate, shares_count, trx_value, expected_fee):
        return {
            'symbol': candidate['symbol'],
            'entry_type': candidate['entry_type'],
            'shares_count': shares_count,
            'price': candidate['price'],
            'trx_value': trx_value,
            'fee': expected_fee,
        }

    def _deciding_to_buy_msg(self, symbol, entry_type):
        self.log.debug('\t+ Deciding how much of {} to buy ({}).'.format(symbol, entry_type))

    def _cannot_afford_msg(self, symbol):
        self.log.debug('\t+ Cannot afford any amount of share. Not buying {}.'.format(symbol))

    def _buying_decision_msg(self, shares_count, symbol):
        self.log.debug('\t+ Buying decision: {} shares of {}.'.format(shares_count, symbol))


class MaxFirstEncountered(PositionSize):
    """
    Decides to buy maximum amount of shares of the first encountered stock candidate. Candidates are checked according
    to `sort_type` order. If one cannot offord to buy any stock of given candidate - next one is checked. 
    """
    def decide_what_to_buy(self, available_money_at_time, candidates):
        for candidate in self.sort(candidates):
            self._deciding_to_buy_msg(candidate['symbol'], candidate['entry_type'])
            price = candidate['price']
            shares_count = self.get_shares_count(available_money_at_time, price)
            if shares_count == 0:
                self._cannot_afford_msg(candidate['symbol'])
                return None
            trx_value = shares_count*price
            expected_fee = self.calculate_fee(trx_value)
            self.log.debug('\t+ Buying decision: {} shares of {}.'.format(shares_count, candidate['symbol']))
            return [self._define_symbol_to_buy(candidate, shares_count, trx_value, expected_fee)]
        return []


class FixedCapitalPerc(PositionSize):
    """
    Decides to buy as much different symbols as possible, but for each symbol buys shares for up to `capital_perc` of current 
    capital. For example, if capital is $1000 and capital_perc is 10%, then it will decide to buy up to 10 symbols and shares 
    for up $100 for each sumbol.
    """
    def decide_what_to_buy(self, capital, capital_perc, available_money_at_time, candidates):
        single_buy_limit = capital*capital_perc
        symbols_to_buy = []
        for candidate in self.sort(candidates):
            price = candidate['price']
            self._deciding_to_buy_msg(candidate['symbol'], candidate['entry_type'])
            if available_money_at_time < single_buy_limit:
                shares_count = self.get_shares_count(available_money_at_time, price)
            else:
                shares_count = self.get_shares_count(single_buy_limit, price)
            if shares_count == 0:
                self._cannot_afford_msg(candidate['symbol'])
                continue
            trx_value = shares_count*price
            expected_fee = self.calculate_fee(trx_value)
            self.log.debug('\t+ Buying decision: {} shares of {}.'.format(shares_count, candidate['symbol']))
            symbols_to_buy.append(self._define_symbol_to_buy(candidate, shares_count, trx_value, expected_fee))
            available_money_at_time -= (trx_value+expected_fee)
        return symbols_to_buy
            

"""
TODO(slaw):
- unit tests for FixedCapitalPerc
- MODEL 3: THE PERCENT RISK MODEL  -> 156 strona z ksiazki "trade your way...""
"""


def main():
    parser = get_parser()
    args = parser.parse_args()
    candidates = [
        {'symbol': 'a', 'entry_type': 'long', 'price': 123},
        {'symbol': 'c', 'entry_type': 'long', 'price': 1},
        {'symbol': 'z', 'entry_type': 'long', 'price': 98},
        {'symbol': 'd', 'entry_type': 'long', 'price': 100},
    ]
    ps = MaxFirstEncountered(debug=args.debug)
    print(ps.decide_what_to_buy(1000, candidates))



if __name__ == '__main__':
    main()
