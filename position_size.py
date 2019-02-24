# built-in
from abc import ABCMeta, abstractmethod
import random


class PositionSize(metaclass=ABCMeta):
    def __init__(self, sort_type='alphabetically'):
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
        print('in sort, by is: ', by)
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

    def _define_symbol_to_buy(self, candidate, shares_count, expected_fee):
        return {
                'symbol': candidate['symbol'],
                'entry_type': candidate['entry_type'],
                'shares_count': shares_count,
                'price': candidate['price'],
                'trx_value': shares_count*candidate['price'],
                'fee': expected_fee,
            }


class MaxFirstEncountered(PositionSize):
    def decide_what_to_buy(self, available_money_at_time, candidates):
        candidate = self.sort(candidates)[0]
        price = candidate['price']
        shares_count = available_money_at_time // (price + (price*self.fee_perc))
        if shares_count == 0:
            # do not afford to buy
            return None



def main():
    candidates = [
        {'symbol': 'a', 'entry_type': 'long', 'price': 123},
        {'symbol': 'c', 'entry_type': 'long', 'price': 1},
        {'symbol': 'z', 'entry_type': 'long', 'price': 98},
        {'symbol': 'd', 'entry_type': 'long', 'price': 100},
    ]
    ps = MaxFirstEncountered(available_money=10000, candidates=candidates)



if __name__ == '__main__':
    main()




"""
1. Buy maximum amount of shares of first encountered candidate
2. Buy X shares of every/possible candidates

Will have to have a mechanism to order candidates somehow
"""



"""
- get all candidates
- sort them (alphabetically, random shuffle, by some sort of parameter like risk-to-reward ratio)
- calculate how many and which shares to buy
- ouptut results


"""