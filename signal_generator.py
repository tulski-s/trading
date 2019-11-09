# 3rd party
import numpy as np
import pandas as pd

# custom
from commons import (
    setup_logging,
)

import rules

import gpw_data


class SignalGenerator():
    def __init__(self, df=None, config=None, logger=None, debug=False):
        self.log = setup_logging(logger=logger, debug=debug)
        self.config = config
        self.df = df
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
        self._triggers = ('entry_long', 'exit_long', 'entry_short', 'exit_short')

    def generate(self):
        initial_signal = self._generate_initial_signal()
        if any([self.wait_entry_confirmation, self.hold_x_days]):
            # TODO(slaw) -> implement _generate_final_signal_with_constraints
            return self._generate_final_signal_with_constraints(initial_signal)
        return self._generate_final_signal(initial_signal)

    def _generate_final_signal(self, initial_signal):
        dates = self.df.index.tolist()
        triggers_dates = dates[self.max_lookback:]
        current = 0
        previous = 0
        signal_triggers = {k: [] for k in self._triggers}
        for idx in range(len(triggers_dates)):
            current = initial_signal[idx]
            if current == previous:
                self._remain_position(signal_triggers)
            else:
                self._change_position(previous, current, signal_triggers)
            previous = current
        signal_triggers_df = pd.DataFrame(signal_triggers, index=self.df.index[self.max_lookback:])
        final_signal = pd.merge(
            left=self.df,
            right=signal_triggers_df,
            how='left',
            left_index=True,
            right_index=True
        )
        final_signal.fillna(0, inplace=True)
        return final_signal

    def _generate_final_signal_with_constraints(self, initial_signal):
        dates = self.df.index.tolist()
        triggers_dates = dates[self.max_lookback:]
        current, previous, idx = 0, 0, 0
        signal_triggers = {k: [] for k in self._triggers}
        _wait_entry_confirmation_tracker = None
        _previous_at_wait_start = None
        _expected_signal = None
        while idx < len(triggers_dates):
            current = initial_signal[idx]
            # TODO-1 - implement logic for both constraints
            # TODO-2 - test for logic with both contraints
            if self.wait_entry_confirmation:
                # Case-0 Tracker not active, already in long/short position
                if not _wait_entry_confirmation_tracker and (current == previous):
                    self._remain_position(signal_triggers)
                # Case-1 Tracker not active and entry signal. Set up expected signal and start to wait.
                elif not _wait_entry_confirmation_tracker and current in (-1, 1):
                    _expected_signal = current
                    _wait_entry_confirmation_tracker = 1
                    _previous_at_wait_start = previous
                    self._remain_position(signal_triggers)
                # Case-2 Tracker not active, but no entry signal. Do nothing.
                elif not _wait_entry_confirmation_tracker and current == 0:
                    self._remain_position(signal_triggers)
                # Case-3 Tracker is active, but still need to wait.
                elif _wait_entry_confirmation_tracker < self.wait_entry_confirmation:
                    _wait_entry_confirmation_tracker += 1
                    self._remain_position(signal_triggers)
                # Case-4 Waited enough time. Check if signal is what it was expected
                elif _wait_entry_confirmation_tracker == self.wait_entry_confirmation:
                    # signal as expected
                    if current == _expected_signal:
                        self._change_position(_previous_at_wait_start, current, signal_triggers)
                    # Signal is opposite or neutral. Deactivate tracker and force going into neutral.
                    elif current == -1*_expected_signal:
                        current = 0
                        self._change_position(_previous_at_wait_start, current, signal_triggers)
                    _wait_entry_confirmation_tracker = None
                    _previous_at_wait_start = None
            if not self.wait_entry_confirmation and self.hold_x_days:
                if current == previous:
                    self._remain_position(signal_triggers)
                elif current in (-1, 1):
                    # enter position
                    self._change_position(previous, current, signal_triggers)
                    # stay there for x days
                    self._remain_position(signal_triggers, days=self.hold_x_days)
                    # next go back to neutral position
                    previous, current = current, 0
                    self._change_position(previous, current, signal_triggers)
                    # skip proper number of days as you just assigned all positions
                    # that should be +1d for changed position + Xdays holding
                    idx += self.hold_x_days + 1
                else:
                    self._change_position(previous, current, signal_triggers)
            previous = current
            idx += 1
        signal_triggers_df = pd.DataFrame(signal_triggers, index=self.df.index[self.max_lookback:])
        final_signal = pd.merge(
            left=self.df,
            right=signal_triggers_df,
            how='left',
            left_index=True,
            right_index=True
        )
        final_signal.fillna(0, inplace=True)
        return final_signal

    def _remain_position(self, signal_triggers, days=1):
        for trigger in self._triggers:
            signal_triggers[trigger].extend(days*[0])

    def _change_position(self, previous, current, signal_triggers):
        new_triggers ={k: 0 for k in self._triggers}
        if previous == 1:
            new_triggers['exit_long'] = 1
        elif previous == -1:
            new_triggers['exit_short'] = 1
        if current == 1:
            new_triggers['entry_long'] = 1
        elif current == -1:
            new_triggers['entry_short'] = 1
        for k in self._triggers:
            signal_triggers[k].append(new_triggers[k])
    
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
      

def main():
    pricing_df = gpw_data.GPWData().load(symbols='CCC', from_csv=True)

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

    # signal_generator.generate()
    signal_generator._generate_final_signal([1,1,1,1,0,0,0,-1,-1,-1])

if __name__ == '__main__':
    main()


