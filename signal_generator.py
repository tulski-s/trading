# 3rd party
import numpy as np
import pandas as pd

# custom
from commons import (
    setup_logging,
)

from rules import (
    trend
)

class SignalGenerator():
    def __init__(self, df=None, config=None, logger=None, debug=False):
        self.log = setup_logging(logger=logger, debug=debug)
        self.config = config
        self.index = len(df.index)
        self.data = {}
        self.simple_rules = []
        self.convoluted_rules = []
        self.rules_results = {}
        # self.signal = {} somewhere final signal has to be stored. with at least date and 4 columns
        self.max_lookback = 0
        for rule in config['rules']:
            # initiate empty list for rules outputs
            self.rules_results[rule['id']] = []
            # store every rule timeseries as array and make sure all number of rows is equal
            self.data[rule['ts']] = df[rule['ts']].to_numpy()
            assert(self.data[rule['ts']].shape[0] == self.index)
            # segregate simple and convoluted rules
            if rule['type'] == 'simple':
                self.simple_rules.append(rule)
            elif rule['type'] == 'convoluted':
                self.convoluted_rules.append(rule)
            # check max lookback. in 'generate' it will be starting point so all data is present
            if self.max_lookback < rule['lookback']:
                self.max_lookback = rule['lookback']


    def generate(self):
        """
        pseudocode

        1) parse config, so that:
            - it is known which functions and parameters will be used to generate signals 
            - it is known which mode (strategy) to exectue
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
        """
        idx = self.max_lookback
        while idx < self.index:
            for simple_rule in self.simple_rules:
                self.log.debug('cheking rule: {r} for idx: {i}'.format(r=simple_rule['id'], i=idx))
                self.rules_results[simple_rule['id']].append(
                    simple_rule['func'](
                        self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                        **simple_rule['params']
                    )
                )
            for conv_rule in convoluted_rules:
                # TODO(slaw) - implement
                pass
            idx += 1


    def _get_ts(self, ts_name, idx, lookback):
        """
        Returns portion of timeseries used as rule input. If lookback is 6, then resulting array will have 7 elemements.
        That is: "current" element (idx+1) and 6 days of loockback period. 
        """
        return self.data[ts_name][idx-lookback:idx+1]
        

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
                'func': trend,
            }
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