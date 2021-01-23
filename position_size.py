# built-in
from abc import ABCMeta, abstractmethod
import random

from commons import (
    setup_logging,
)

#3rd party
import numpy


class PositionSize(metaclass=ABCMeta):
    """
    Base class for all position sizers. Implements common method across all of them.

    Example of costs:
    mBank: fee_perc=0.0038, min_fee=4
    IB: fee_perc=0.0005, min_fee=6 (for orders >50k, else, fixed on 6)
        https://www.interactivebrokers.co.uk/en/index.php?f=39753&p=stocks1
    """
    def __init__(self, fee_perc=0.0038, min_fee=4, sort_type='alphabetically', logger=None, debug=False):
        self.log = setup_logging(logger=logger, debug=debug)
        self.fee_perc = fee_perc
        self.min_fee = min_fee
        self.sort_type = sort_type

    @abstractmethod
    def decide_what_to_buy(self, available_money_at_time, candidates, **kwargs):
        """ Should use _define_symbol_to_buy in output """
        pass

    def calculate_fee(self, transaction_value):
        """Calculates expected transaction fee."""
        fee = transaction_value * self.fee_perc
        if fee < self.min_fee:
            fee = self.min_fee
        return round(fee, 2)
    
    def sort(self, candidates, volatility={}, rrr=None):
        _candidates = candidates.copy()
        if self.sort_type == 'alphabetically':
            _candidates.sort(key=lambda c: c['symbol'])
        elif self.sort_type == 'random':
            random.shuffle(_candidates)
        elif self.sort_type == 'cheapest':
            _candidates.sort(key=lambda c: c['price'])
        elif self.sort_type == 'expensive':
            _candidates.sort(key=lambda c: c['price'], reverse=True)
        elif self.sort_type == 'volatility_highest':
            _candidates.sort(key=lambda c: volatility[c['symbol']], reverse=True)
        elif self.sort_type == 'volatility_lowest':
            _candidates.sort(key=lambda c: volatility[c['symbol']])
        elif self.sort_type == 'rrr':
            _candidates.sort(key=lambda c: rrr[c['symbol']])
        return _candidates

    def get_shares_count(self, money, price):
        return money // (price + (price*self.fee_perc))

    def define_candidate(self, symbol=None, entry_type=None, price=None, stop_loss=None):
        """ Reutrns dictionary with purchease candidates """
        return {
            'symbol': symbol,
            'entry_type': entry_type,
            'price': price,
            'stop_loss': stop_loss
        }

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

    def _money_and_price_msg(self, money, price):
        self.log.debug('\t+ Based on available_money: {} and price: {}'.format(money, price))


class MaxFirstEncountered(PositionSize):
    """
    Decides to buy maximum amount of shares of the first encountered stock candidate. Candidates are checked according
    to `sort_type` order. If one cannot offord to buy any stock of given candidate - next one is checked. 
    """
    def decide_what_to_buy(self, available_money_at_time, candidates, volatility={}, **kwargs):
        for candidate in self.sort(candidates, volatility=volatility):
            self._deciding_to_buy_msg(candidate['symbol'], candidate['entry_type'])
            price = candidate['price']
            self._money_and_price_msg(available_money_at_time, price)
            shares_count = self.get_shares_count(available_money_at_time, price)
            if shares_count == 0:
                self._cannot_afford_msg(candidate['symbol'])
                continue
            trx_value = shares_count*price
            expected_fee = self.calculate_fee(trx_value)
            self._buying_decision_msg(shares_count, candidate['symbol'])
            return [self._define_symbol_to_buy(candidate, shares_count, trx_value, expected_fee)]
        return []


class FixedCapitalPerc(PositionSize):
    """
    Decides to buy as much different symbols as possible, but for each symbol buys shares for up to `capital_perc` of current 
    capital. For example, if capital is $1000 and capital_perc is 10%, then it will decide to buy up to 10 symbols and spend up
    to $100 for each sumbol.
    """
    def __init__(self, capital_perc=None, **kwargs):
        super().__init__(**kwargs)
        self.capital_perc = capital_perc

    def decide_what_to_buy(self, available_money_at_time, candidates, capital=None, volatility={}, **kwargs):
        single_buy_limit = round(capital*self.capital_perc, 2)
        self.log.debug('\t+ Based on capital: {} which gives signle transaction valiue limit: {} ({}%)'.format(
            capital, single_buy_limit, self.capital_perc*100
        ))
        symbols_to_buy = []
        for candidate in self.sort(candidates, volatility=volatility):
            price = candidate['price']
            self._money_and_price_msg(available_money_at_time, price)
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
            self._buying_decision_msg(shares_count, candidate['symbol'])
            symbols_to_buy.append(self._define_symbol_to_buy(candidate, shares_count, trx_value, expected_fee))
            available_money_at_time -= (trx_value+expected_fee)
        return symbols_to_buy


class PercentageRisk(PositionSize):
    """
    *perc_risk* is % of account value one will risk per one position. Model controls size of position as a function of risk. 
    In general, the bigger stop losses and reward-to-risk ration in strategy the higher perc_risk can be (but still
    between 1 and 3%). If stops in strategy are shorter % should be probably smaller than 1. Expectation of the system also 
    counts - if its fairly large one can probably risk more.
    """
    def __init__(self, perc_risk=None, fallback_sl=0.1, **kwargs):
        super().__init__(**kwargs)
        self.perc_risk = perc_risk
        self.fallback_sl = fallback_sl

    def decide_what_to_buy(self, available_money_at_time, candidates, capital=None, volatility={}, **kwargs):
        symbols_to_buy = []
        for candidate in self.sort(candidates, volatility=volatility):
            if not candidate.get('stop_loss', None):
                raise ValueError(
                    'Candidate ({}) does not have available stop loss. Cannot use PercentageRisk position sizer!'.format(
                        candidate['symbol']
                    )
                )
            if numpy.isnan(candidate.get('stop_loss')):
                raise ValueError(
                    'Candidate ({}) has NaN as stop loss. It sohuld be a number!'.format(
                        candidate['symbol']
                    )
                )
            price = candidate['price']
            stop_loss = candidate['stop_loss']
            self._money_and_price_msg(available_money_at_time, price)
            self._deciding_to_buy_msg(candidate['symbol'], candidate['entry_type'])
            # how many shares you can theoretically get
            if price == stop_loss:
                if candidate['entry_type'] == 'short':
                    stop_loss = price + (price*self.fallback_sl)
                elif candidate['entry_type'] == 'long':
                    stop_loss = price - (price*self.fallback_sl)
            value_at_risk_per_share = abs(price - stop_loss)
            risk_per_transaction = round(capital*self.perc_risk, 2)
            theoretical_shares_count = risk_per_transaction//value_at_risk_per_share
            theoretical_trx_value = theoretical_shares_count * price
            # how many shares you can actually get
            if available_money_at_time < theoretical_trx_value:
                shares_count = self.get_shares_count(available_money_at_time, price)
            else:
                shares_count = self.get_shares_count(theoretical_trx_value, price)
            if shares_count == 0:
                self._cannot_afford_msg(candidate['symbol'])
                continue
            real_trx_value = shares_count*price
            expected_fee = self.calculate_fee(real_trx_value)
            self._buying_decision_msg(shares_count, candidate['symbol'])
            symbols_to_buy.append(self._define_symbol_to_buy(candidate, shares_count, real_trx_value, expected_fee))
            available_money_at_time -= (real_trx_value+expected_fee)
        return symbols_to_buy


class FixedRisk(PositionSize):
    """
    There will be always fixed risk per position. E.g. $100 (`risk_per_trade` parameter). Requires stop loss.
    Also, it support RRR (reward-to-risk ratio) sorting. RRR = total_risk / total_gain
    total_risk == risk_per_trade and total_gain = (shares_count * volatility)
    If allow_partial is True is will allow to be as much as possible in case there is not enough money. Eg.
    if with given risk_per_trade you should but 10 share but there is money only for 6 - return those 6. By 
    default is all or nothing. 
    """
    def __init__(self, risk_per_trade=None, allow_partial=False, **kwargs):
        super().__init__(**kwargs)
        self.risk_per_trade = risk_per_trade
        self.allow_partial = allow_partial

    def decide_what_to_buy(self, available_money_at_time, candidates, volatility={}, **kwargs):
        # go over all candidates first time
        rrrs = {}
        all_candidates_processed = []
        for candidate in candidates:
            sym = candidate['symbol']
            price = candidate['price']
            stop_loss = candidate['stop_loss']
            value_at_risk_per_share = abs(price - stop_loss)
            theoretical_trx_value = (self.risk_per_trade//value_at_risk_per_share) * price
            if available_money_at_time < theoretical_trx_value:
                # Implement here allow_partial fill
                if self.allow_partial == True:
                    # -1 as safety guard
                    shares_count = self.get_shares_count(available_money_at_time, price) - 1
                    self.log.debug(f'Partial order from Position Sizer: {sym}:{shares_count}')
                else:
                    self.log.debug((
                        f'Not enough money to fully buy {sym}. '
                        f'Need: {theoretical_trx_value}, have: {available_money_at_time}'
                    ))
                    continue
            else:
                shares_count = self.get_shares_count(theoretical_trx_value, price)
            gain_per_trade = shares_count*volatility.get(sym, 0)
            # assumes that price can rise/fall curr_price +- 1std
            if gain_per_trade > 0:
                rrrs[sym] = self.risk_per_trade / gain_per_trade
            else:
                rrrs[sym] = 1
            if shares_count == 0:
                continue
            real_trx_value = shares_count*price
            expected_fee = self.calculate_fee(real_trx_value)
            all_candidates_processed.append(self._define_symbol_to_buy(candidate, shares_count, real_trx_value, expected_fee))
        # final pass based on appropriate sorting
        symbols_to_buy = []
        for candidate in self.sort(all_candidates_processed, volatility=volatility, rrr=rrrs):
            remaining_money = available_money_at_time - (candidate['trx_value'] + candidate['fee'])
            if remaining_money > 0:
                self._buying_decision_msg(candidate['shares_count'], candidate['symbol'])
                symbols_to_buy.append(candidate)
                available_money_at_time = remaining_money
            else:
                return symbols_to_buy
        return symbols_to_buy
