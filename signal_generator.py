# 3rd party
import numpy as np
import pandas as pd

# custom
from commons import (
    setup_logging,
)

import rules


class SignalGenerator():
    def __init__(self, df=None, config=None, logger=None, debug=False):
        self.log = setup_logging(logger=logger, debug=debug)
        self.config = config
        self.index = len(df.index)
        self.data = {}
        self.simple_rules = []
        self.convoluted_rules = []
        self.rules_results = {}
        # TODO(slaw) - would be good to have validation of config file here
        self.strategy_type = config['strategy']['type']
        self.strategy_rules = config['strategy']['strategy_rules']
        if config['strategy'].get('constraints', None):
            self.wait_entry_confirmation = config['strategy']['constraints'].get('wait_entry_confirmation', None)
            self.hold_x_days = config['strategy']['constraints'].get('hold_x_days', None)
        self.max_lookback = 0
        for rule in config['rules']:
            # initiate empty list for rules outputs
            self.rules_results[rule['id']] = []
            # segregate simple and convoluted rules
            if rule['type'] == 'simple':
                self.simple_rules.append(rule)
                # store every rule timeseries as array and make sure all number of rows is equal
                self.data[rule['ts']] = df[rule['ts']].to_numpy()
                assert(self.data[rule['ts']].shape[0] == self.index)
                # check max lookback. in 'generate' it will be starting point so all data is present
                if self.max_lookback < rule['lookback']:
                    self.max_lookback = rule['lookback']
            elif rule['type'] == 'convoluted':
                self.convoluted_rules.append(rule)
        # (TODO: are those still needed?) helper variables to correctly track final signal assignment 
        self._signal = 0
        self._previous_signal = 0
        self._holding_period = 0
        self._wait_entry_confirmation_tracker = -1

    def generate(self):
        initial_signal = self._generate_initial_signal()
        if any([self.wait_entry_confirmation, self.hold_x_days]):
            self._generate_final_signal_with_constraints(initial_signal)
        else:
            self._generate_final_signal(initial_signal)

    def _generate_final_signal(self, initial_signal):
        pass

    def _generate_final_signal_with_constraints(self, initial_signal):
        pass
    
    def combine_simple_results(self, rules_results=None, aggregation_type=None, aggregation_params=None):
        """
        Resolves set of simple rules results into single outcome. It can be done based on `aggregation_type`.
        That is 'combine' (different votings and aggregation) or 'state-based'.
        """
        if aggregation_type == 'combine':
            mode = aggregation_params['mode']
            if mode == 'strong':
                # for signal to be 1 or -1, all simple rules results has to be 1 or -1. else 0
                results_sum = sum(rules_results)
                len_resutls = len(rules_results)
                if results_sum == len_resutls:
                    return 1
                elif results_sum == -1*len_resutls:
                    return -1
                return 0
            elif mode == 'majority_voting':
                # signal will be same as most frequent result. 0 in case of tie
                sum_zero, sum_plus, sum_minus = 0, 0, 0
                for result in rules_results:
                    if result == 0:
                        sum_zero += 1
                    elif result == 1:
                        sum_plus += 1
                    else:
                        sum_minus += 1    
                if (sum_plus > sum_zero) and (sum_plus > sum_minus):
                    return 1
                elif (sum_minus > sum_zero) and (sum_minus > sum_plus):
                    return -1
                else:
                    return 0
            else:
                raise NotImplementedError('mode "{}" for "combine" aggregation is not supported'.format(mode))
        elif aggregation_type == 'state-based':
            # Implement it later when doint event-based strategies. based on dict. same as fixed?
            # It should return the same sequential output (eg. 11110000111-1-1-1-1000) showing position
            # at each day. Same as "combine" with all the votings
            pass
        else:
            raise NotImplementedError('aggregation_type "{}" is not supported'.format(aggregation_type))


    def _get_ts(self, ts_name, idx, lookback):
        """
        Returns portion of timeseries used as rule input. If lookback is 6, then resulting array will have 7 elemements.
        That is: "current" element (idx+1) and 6 days of loockback period. 
        """
        return self.data[ts_name][idx-lookback:idx+1]

    def _get_simple_rules_results(self, rules_ids, result_idx):
        """
        `rules_ids` is iterable witch ids of simple rules. `result_idx` should be index of results
        base on which to output combined result.
        """
        rules_results = [self.rules_results[rid][result_idx] for rid in rules_ids]
        # for the future map based convoluted rules it will result either list or dict
        return rules_results

    def _generate_initial_signal(self):
        initial_signal = []
        idx = self.max_lookback
        while idx < self.index:
            result_idx = idx-self.max_lookback
            # get results from simple rules
            for simple_rule in self.simple_rules:
                # self.log.debug('cheking simple rule: {r} for idx: {i}'.format(r=simple_rule['id'], i=idx))
                self.rules_results[simple_rule['id']].append(
                    simple_rule['func'](
                        self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                        **simple_rule['params']
                    )
                )
            # get results from convoluted rules
            for conv_rule in self.convoluted_rules:
                # self.log.debug('cheking convoluted rule: {r} for idx: {i}'.format(r=conv_rule['id'], i=idx))
                simple_rules_results = self._get_simple_rules_results(conv_rule['simple_rules'], result_idx)
                self.rules_results[conv_rule['id']].append(
                    self.combine_simple_results(
                        rules_results=simple_rules_results,
                        aggregation_type=conv_rule['aggregation_type'], 
                        aggregation_params=conv_rule['aggregation_params'],
                    )
                )
            # create initial signal
            if self.strategy_type == 'fixed':
                for rule in self.strategy_rules:
                    signal = self.rules_results[rule][result_idx]
                    # use signal from first encountered rule with -1 or 1. or leave 0
                    if signal in (-1, 1):
                        break
            elif self.strategy_type == 'learning':
                # not implemented
                pass
            initial_signal.append(signal)
            idx += 1

    def deprecated_generate(self):
        idx = self.max_lookback
        while idx < self.index:
            result_idx = idx-self.max_lookback
            # get results from simple rules
            for simple_rule in self.simple_rules:
                # self.log.debug('cheking simple rule: {r} for idx: {i}'.format(r=simple_rule['id'], i=idx))
                self.rules_results[simple_rule['id']].append(
                    simple_rule['func'](
                        self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                        **simple_rule['params']
                    )
                )
            # get results from convoluted rules
            for conv_rule in self.convoluted_rules:
                # self.log.debug('cheking convoluted rule: {r} for idx: {i}'.format(r=conv_rule['id'], i=idx))
                simple_rules_results = self._get_simple_rules_results(conv_rule['simple_rules'], result_idx)
                self.rules_results[conv_rule['id']].append(
                    self.combine_simple_results(
                        rules_results=simple_rules_results,
                        aggregation_type=conv_rule['aggregation_type'], 
                        aggregation_params=conv_rule['aggregation_params'],
                    )
                )

            # create initial signal
            if self.strategy_type == 'fixed':
                for rule in self.strategy_rules:
                    self._signal = self.rules_results[rule][result_idx]
                    # use signal from first encountered rule with -1 or 1. or leave 0
                    if self._signal in (-1, 1):
                        break
            elif self.strategy_type == 'learning':
                # not implemented
                pass

            # if wait for entry confirmation constrain is on
            if self.wait_entry_confirmation:
                # 0-> 1, 0-> -1, 1-> -1, -1 -> 1   te chce
                # a co z: -1 -> 0   oraz 1 -> 0
                # tracker is not yet active. set up expected signal and activate tracker
                if self._wait_entry_confirmation_tracker == -1 and self._signal in (-1, 1):
                    _expected_signal == self._signal
                    self._wait_entry_confirmation_tracker = 1
                    final_signal = self._previous_signal
                # tracker not active but neutral signal
                elif self._wait_entry_confirmation_tracker == -1 and self._signal == 0:
                    final_signal = self._signal
                # trancker active. still have to wait
                elif self._wait_entry_confirmation_tracker < self.wait_entry_confirmation:
                    self._wait_entry_confirmation_tracker += 1
                    final_signal = self._previous_signal
                # already waited enough days. check if signal is what it is expectd
                elif self._wait_entry_confirmation_tracker == self.wait_entry_confirmation:
                    # signal as expected or neutral
                    if self._signal == _expected_signal or self._signal == 0:
                        final_signal = self._signal
                        self._wait_entry_confirmation_tracker = -1
                    # signal is opposite to what was expected, go to neutral position
                    elif self._signal == _expected_signal * -1:
                        self._wait_entry_confirmation_tracker = -1
                        final_signal = 0

            
            # if hold X days constrain is on
            # if (
            #     self.hold_x_days and 
            #     self._signal in (-1, 1) and 
            # ):

            idx += 1

        

def main():
    pricing_df = pd.read_csv('/Users/slaw/osobiste/gielda/CCC_pricing.csv')

    test_config = {
        'rules': [
            {
                'id': 'trend',
                'type': 'simple',
                'ts': 'close',
                'lookback': 20,
                'params': {},
                'func': rules.trend,
            },
            {
                'id': 'supprot/resistance',
                'type': 'simple',
                'ts': 'close',
                'lookback': 30,
                'params': {},
                'func': rules.support_resistance,
            },
            {
                'id': 'trend+supprot/resistance',
                'type': 'convoluted',
                'simple_rules': ['trend', 'supprot/resistance'],
                'aggregation_type': 'combine',
                'aggregation_params':{'mode':'strong'}
            },
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['trend+supprot/resistance'],
            'constraints': {
                'hold_x_days': 5,
                'wait_entry_confirmation': 3
            }
        }
    }

    signal_generator = SignalGenerator(
        df = pricing_df,
        config = test_config,
        debug=True
    )

    signal_generator.generate()

if __name__ == '__main__':
    main()


"""
which data structures should I use ???

in backtester I have sth like:
signals = {
    symbol_X: {
        'close': {
            '2019-01-01': 12,
            '2019-01-02': 13,
            '2019-01-03': 10,
            ...
        },
        'exit_long': {
            '2019-01-01': 0,
            '2019-01-02': 0,
            '2019-01-03': 1,
            ...
        }
    }
}

to tam dziala spoko bo latwo skoordynowac wiele timeseries (access is by ds key) tylko tam nigy nie patrze X dni w tyl....

tutaj jak bym chcial tego uzyc to by musial wiedziec ktore dni ida i skomponowac array...

inna opcja mogloby byc poprostu skomponowanie duzej DataFrame gdzie index to ds a columny to kolejne timeseries. minus tutaj
tylko taki ze iteruje wlasnie przez ta dataframe. nie koniec swiata, troche mnie performat. no i prierdolenie sie z df...
kolejna mozlwiosc to nie rabanie sie z pandas, tylko zrobienie to w numpy... czy to cos wielce zmini?

to opcja z pandas szczerze prezentuje sie spoko.... no i jest tylko 1 iteracja wiec performance nie bedzie tutaj
problemu?

chyba ze przygotowalbym dane w taki sposob zebym mial:
data = {
    ts1 = arr[]
    ts2 = arr[]
    ...
}
gdzie ts1,2..X maja arrays o dokladnie takim samym rozmiarze i odpowiadajace dokladnie tym samym dnia.
pracowalbym wtedy na indexach i nie byloby problemu

nie chce przekombinowac na tym etapie... ale najwiekszym problemem bedzie tak sie nie bede zgadzaly dni... cos przesudziete
pomiedzy dwoma ts i jedna nie odpowiada drugiej.
wpakowanie wszystkiego do DF, uporzadkowania a potem "wyciecie" tego co Cie interesuje brzmi spoko. 

zwykle nie bedzie duzo timeseries. tylko 1 sybol i ewentualnie dane contextualne...


jest kolejna mozliwos. SignalGenerator moze zakladac ze dostaje DF gdzie wszystko jest juz przygotowane. i tak kazda kolumna
to jest poprostu timeseries (wszysktie szeruja common indeks). dla wszystkich przykladow z mojego POC bedize sie to zgadzalo


wygladaloby tak. do strategy dajesz dataframe. w config precyzujesz kolumne(y). a potem to jest przerabiane na 
data = {
    ts1 = arr[]
    ts2 = arr[]
    ...
}
gdzie wszystkie indeksy itp sie zgadzaja bo sa wziete z datarame.



"""