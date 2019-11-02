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

        # self.signal = {} somewhere final signal has to be stored. with at least date and 4 columns
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

    def generate(self):
        """
        pseudocode

        1) parse and config
        2) get and prepare data
            - if multiple timeseries is used they will have to be synched.
        3) itereate over every day, each time:
            (for fixed)
            - execute appropriate functions
            - gather results
            - reduce complex signals etc
            - decide on the flag of given day

        for day in days
            for simple_rule in simple_rules:
                # execute each (func, params) over given ts (timeseries) and store results
            for conv_rule in convoluted_rules:
                # execute each (func, params) given results from simple rules
                # aggregate
                # get results, store it
            if fixed_type:
                for rule in strategy_rules:
                    # check if rule gives signal. return first encountered
                    # check constraints (e.g. hold X days)
        """
        idx = self.max_lookback
        while idx < self.index:
            for simple_rule in self.simple_rules:
                # self.log.debug('cheking simple rule: {r} for idx: {i}'.format(r=simple_rule['id'], i=idx))
                self.rules_results[simple_rule['id']].append(
                    simple_rule['func'](
                        self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                        **simple_rule['params']
                    )
                )
            for conv_rule in self.convoluted_rules:
                self.log.debug('cheking convoluted rule: {r} for idx: {i}'.format(r=conv_rule['id'], i=idx))
                simple_rules_results = self._get_simple_rules_results(conv_rule['simple_rules'], idx-self.max_lookback)
                self.rules_results[conv_rule['id']].append(
                    self.combine_simple_results(
                        rules_results=simple_rules_results,
                        aggregation_type=conv_rule['aggregation_type'], 
                        aggregation_params=conv_rule['aggregation_params'],
                    )
                )
            # TODO: implement fixed_type execution

            idx += 1

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
        elif aggregation_type == 'fixed':
            # it will be based on dict with fixed rules. eg. trend must be 1 and sth else 0, then sth.
            pass
        elif aggregation_type == 'state-based':
            # Implement it later when doint event-based strategies
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
            'rules': ['trend']
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