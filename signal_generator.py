# built in
import os
import pickle

# 3rd party
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# custom
from commons import (
    setup_logging,
)
import rules
import gpw_data


class NotAllRuleResultsPresentError(Exception):
    pass


class SignalGenerator():
    def __init__(
        self, df=None, config=None, logger=None, debug=False, load_rules_results_path=None, load_rules_results_prefix='',
        load_only_simple=False,
    ):
        self.log = setup_logging(logger=logger, debug=debug)
        self.config = config
        self.df = df
        self.index = len(df.index)
        self.data = {}
        self.simple_rules = []
        self.convoluted_rules = []
        self.rules_results = {}
        # TODO(slaw) - would be good to have config validation here
        self.strategy_type = config['strategy']['type']
        self.strategy_rules = config['strategy']['strategy_rules']
        if config['strategy'].get('strategy_id', None):
            self.strategy_id = config['strategy']['strategy_id']
        if config['strategy'].get('constraints', None):
            self.wait_entry_confirmation = config['strategy']['constraints'].get('wait_entry_confirmation', None)
            self.hold_x_days = config['strategy']['constraints'].get('hold_x_days', None)
        else:
            self.wait_entry_confirmation = None
            self.hold_x_days = None
        if config['strategy'].get('params', None):
            self.strategy_memory_span = config['strategy']['params'].get('memory_span', None)
            self.strategy_review_span = config['strategy']['params'].get('review_span', None)
            self.strategy_metric = config['strategy']['params'].get('performance_metric', None)
            self.strategy_price_label = config['strategy']['params'].get('price_label', None)
        if self.strategy_type == 'learning':
            self.past_reviews = {rule_id: [] for rule_id in self.strategy_rules}
            # basic validity checks for the learning strategy
            required_learning_params = [
                self.strategy_memory_span, self.strategy_review_span, self.strategy_metric, self.strategy_price_label
            ]
            if not all(required_learning_params):
                raise AttributeError(
                    'Learning strategy required params: memory_span, review_span, performance_metric and price_label'
                )
            if self.strategy_review_span > self.strategy_memory_span:
                raise AttributeError(
                    'Strategy review_span has to be smaller than memory_span'
                )
            # calculate daily returns used by most of learning performance metrics
            if self.strategy_metric == 'daily_returns':
                self.daily_returns__learning = df[self.strategy_price_label].pct_change().tolist()
            elif self.strategy_metric in ('avg_log_returns', 'avg_log_returns_held_only'):
                self.daily_log_returns__learning = (
                    np.log(df[self.strategy_price_label]) - df[self.strategy_price_label].shift(1)
                ).tolist()


        self.hold_x_days_rule_lvl = {}
        self.init_review_span_tracker = 0
        self.max_lookback = 0
        self.rules_idxs = {}
        for idx, rule in enumerate(config['rules']):
            # build map -> idx: rule_id
            self.rules_idxs[rule['id']] = idx 
            # initiate empty list for rules outputs
            self.rules_results[rule['id']] = []
            # segregate simple and convoluted rules
            if rule['type'] == 'simple':
                self.simple_rules.append(rule)
                # store every rule timeseries as array and make sure all number of rows is equal
                if load_rules_results_path == None:
                    if isinstance(rule['ts'], str):
                        if rule['ts'] not in self.data:
                            self.data[rule['ts']] = df[rule['ts']].to_numpy()
                            assert(self.data[rule['ts']].shape[0] == self.index)
                    elif isinstance(rule['ts'], list):
                        for ts in rule['ts']:
                            if ts not in self.data:
                                self.data[ts] = df[ts].to_numpy()
                                assert(self.data[ts].shape[0] == self.index)
                # check max lookback. in 'generate' it will be starting point so all data is present
                if self.max_lookback < rule['lookback']:
                    self.max_lookback = rule['lookback']
                # simple rule may be forced to hold for specific num. of days
                if rule.get('hold_fixed_days', None):
                    self.hold_x_days_rule_lvl[rule['id']] = []
            elif rule['type'] == 'convoluted':
                if rule['aggregation_type'] == 'state-based':
                    rule['_is_state_base'] = True
                else:
                    rule['_is_state_base'] = False
                self.convoluted_rules.append(rule)
        self._triggers = ('entry_long', 'exit_long', 'entry_short', 'exit_short')
        self.final_positions = self._reset_final_positions()
        # load stored rules results (to speed up execution)
        self.load_only_simple = load_only_simple
        if load_rules_results_path:
            self._load_rules_results(load_rules_results_path, load_rules_results_prefix)
            self._rules_results_loaded = True
        else:
            self._rules_results_loaded = False

    def generate(self):
        initial_signal = self._generate_initial_signal()
        if any([self.wait_entry_confirmation, self.hold_x_days]):
            signal_triggers_df = self._generate_final_signal_with_constraints(initial_signal)
        else:
            signal_triggers_df = self._generate_final_signal(initial_signal)
        final_signal = self._merge_final_signal(signal_triggers_df)
        final_signal.loc[:, 'position'] = self.final_positions
        if self.config['strategy'].get('reversed', None):
            # reverse all simple and convoluted rules
            for rule_id, rule_signal in self.rules_results.items():
                inversed_signal = (-1 * np.array(self.rules_results[rule_id])).tolist()
                self.rules_results[rule_id] = inversed_signal
            # reverse all final signals/positions
            for col in self._triggers:
                final_signal[f'copy_{col}'] = final_signal[col]
            final_signal['entry_long'] = final_signal['copy_entry_short']
            final_signal['exit_long'] = final_signal['copy_exit_short']
            final_signal['entry_short'] = final_signal['copy_entry_long']
            final_signal['exit_short'] = final_signal['copy_exit_long']
            for col in self._triggers:
                final_signal.drop(f'copy_{col}', axis=1, inplace=True)
            final_signal['position'] = -1*final_signal['position']
        return final_signal

    def plot_strategy_result(self, df, price_label=None):
        # get long/short periods
        periods = {}
        for _type in ('long', 'short'):
            idxs_entries = df.index[df['entry_'+_type] == 1].tolist()
            idxs_exits = df.index[df['exit_'+_type] == 1].tolist()
            if len(idxs_entries) > len(idxs_exits):
                idxs_entries = idxs_entries[:-1]            
            elif len(idxs_exits) > len(idxs_entries):
                idxs_exits = idxs_exits[:-1]            
            elif len(idxs_entries) != len(idxs_exits):
                # logically they can differ only by 1, so if its not the case sth is wrong
                raise ValueError
            periods[_type]  = list(zip(idxs_entries, idxs_exits))
        # plot line with proper timeseries (line for now, no candles as they are fucking slow on graph)
        fig, ax = plt.subplots(figsize=(7,5))
        ax.plot(df[price_label], color='black', linestyle='-', linewidth=1)
        for lp in periods['long']:
            if df[price_label][lp[1]] - df[price_label][lp[0]] <= 0:
                ax.axvspan(lp[0], lp[1], alpha=0.5, color='red')
            else:
                ax.axvspan(lp[0], lp[1], alpha=0.5, color='green')
        for lp in periods['short']:
            if df[price_label][lp[1]] - df[price_label][lp[0]] <= 0:
                ax.axvspan(lp[0], lp[1], alpha=0.5, color='green')
            else:
                ax.axvspan(lp[0], lp[1], alpha=0.5, color='red')

    def plot_rule_results(self, rule_id, ts=None):
        _rule_idx = self.rules_idxs[rule_id]
        _rule_type = self.config['rules'][_rule_idx]['type']
        if _rule_type == 'convoluted' and ts == None:
            raise AttributeError('Chosen rule_id is convoluted hence needs explicit `ts` attribute')
        if _rule_type == 'simple':
            _lookback = self.config['rules'][_rule_idx]['lookback']
            _ts = self.config['rules'][_rule_idx]['ts']
        elif _rule_type == 'convoluted':
            lookbacks = []
            for s_rule_id in self.config['rules'][_rule_idx]['simple_rules']:
                lookbacks.append(
                    self.config['rules'][self.rules_idxs[s_rule_id]]['lookback']
                )
            _lookback = max(lookbacks)
            _ts = ts
        dates = self.df.index.tolist()
        results = _lookback*[0] + self.rules_results[rule_id]
        reference_ts = self.df[_ts]
        # find segments of continuous -1,0,1s for muliti-color line to plot
        segments = []
        prev_r = 999 # not valid number for result. just to instantiate
        cols_map = {-1: 'red', 0: 'blue', 1: 'green'}
        for i, cur_r in enumerate(results):
            if cur_r != prev_r:
                # append old segment and strat the new one
                if i > 0:
                    segments.append(segment)
                    segment = [
                        [dates[i-1], dates[i]],
                        [reference_ts[i-1], reference_ts[i]],
                        cols_map[cur_r]
                    ]
                else:
                # first iteration. just start new segment with single element
                    segment = [
                        [dates[i]],
                        [reference_ts[i]],
                        cols_map[cur_r]
                    ]
            else:
                segment[0].append(dates[i])
                segment[1].append(reference_ts[i])
            prev_r = cur_r
        fig, ax = plt.subplots(figsize=(7,5))
        for segment in segments:
            ax.plot(
                segment[0],
                segment[1],
                color=segment[2],
                linestyle='-',
                linewidth=2
            )

    def save_rules_results(self, path=None, prefix=''):
        """
        Saves results of simple and complex rule into file.
        """
        for rule_id, rule_res in self.rules_results.items():
            file_full_path = os.path.join(path, prefix+rule_id)
            with open(file_full_path, 'wb') as fh:
                pickle.dump(rule_res, fh)

    def _load_rules_results(self, path, prefix):
        """
        Loads saved (in `path`) rule results. All rules has to be present.
        """
        if self.load_only_simple == False:
            rules_to_be_loaded = set(self.rules_results.keys())
        else:
            rules_to_be_loaded = set([r['id'] for r in self.simple_rules])
        all_files = set([f for f in os.listdir(path)])
        rules_results = {}
        for rule_id in rules_to_be_loaded.copy():
            file_name = prefix+rule_id
            if file_name in all_files:
                # will throw error if file with rule does not exists
                with open(os.path.join(path, file_name), 'rb') as fh:
                    rules_results[rule_id] = pickle.load(fh)
                rules_to_be_loaded.remove(rule_id)
        
        if len(rules_to_be_loaded) == 0:
            self.rules_results.update(rules_results)
        else:
            raise NotAllRuleResultsPresentError(
                f'Not all rules are present in {path}. Missing are: {rules_to_be_loaded}'
                )

    def _generate_final_signal(self, initial_signal, return_signal=False):
        """
        Transforms initial signal to the final output. That is, it takes sequence of positions like
        1,1,0,0,0,-1,-1 and outputs oryginal pricing dataframe with 4 additional columns: enter_long,
        exit_long, enter_short, exit_short. Such an format is expected by backtester.

        No constraints are implemented here such just transformation is performed.
        """

        self._reset_final_positions()
        dates = self.df.index.tolist()
        triggers_dates = dates[self.max_lookback:]
        current = 0
        previous = 0
        signal_triggers = {k: [] for k in self._triggers}
        for idx in range(len(triggers_dates)):
            current = initial_signal[idx]
            if current == previous:
                self._remain_position(signal_triggers, position=current)
            else:
                self._change_position(previous, current, signal_triggers)
            previous = current
        signal_triggers_df = pd.DataFrame(signal_triggers, index=self.df.index[self.max_lookback:])
        if return_signal:
            return self._merge_final_signal(signal_triggers_df)
        else:
            return signal_triggers_df

    def _generate_final_signal_with_constraints(self, initial_signal, return_signal=False):
        """
        Same as _generate_final_signal but with constraints implementation. Contraints can be used 
        separately or together. Available contraints are:
        -> wait_entry_confirmation - wait without changing position for defined amount of
        days. On the last waiting day checks if rule still shows position which was initally
        attmpted to enter. If signal is the same enter. Else, change to neutral position.
        -> hold_x_days - after entering position hold additional x days. Change to neutral
        position after that.
        """
        """
        TODO - hold_x_days should be changed to hold_min_x_days. in case that after holding x days
        signal is still same - do not exit and re-enter again
        """
        self._reset_final_positions()
        dates = self.df.index.tolist()
        triggers_dates = dates[self.max_lookback:]
        current, previous, idx = 0, 0, 0
        signal_triggers = {k: [] for k in self._triggers}
        _wait_entry_confirmation_tracker = None
        _previous_at_wait_start = None
        _expected_signal = None
        while idx < len(triggers_dates):
            current = initial_signal[idx]
            if self.wait_entry_confirmation:
                # Case-0 Tracker not active, already in long/short position
                if not _wait_entry_confirmation_tracker and (current == previous):
                    self._remain_position(signal_triggers, position=current)

                # Case-1 Tracker not active and entry signal. Set up expected signal and start to wait.
                elif not _wait_entry_confirmation_tracker and current in (-1, 1):
                    _expected_signal = current
                    _wait_entry_confirmation_tracker = 1
                    _previous_at_wait_start = previous
                    # position is kept as previous. as not entered yet
                    self._remain_position(signal_triggers, position=_previous_at_wait_start)

                # Case-2 Tracker not active, but no entry signal. Do nothing or go neutral
                elif not _wait_entry_confirmation_tracker and current == 0:
                    _previous_from_processed = self.final_positions[-1]
                    if _previous_from_processed in (-1, 1):
                        # go neutral
                        self._change_position(_previous_from_processed, current, signal_triggers)
                    else:
                        # remain current neutral position
                        self._remain_position(signal_triggers, position=current)

                # Case-3 Tracker is active, but still need to wait.
                elif _wait_entry_confirmation_tracker < self.wait_entry_confirmation:
                    _wait_entry_confirmation_tracker += 1
                    # again, as its not yet time to enter position is kept as previous of waiting
                    self._remain_position(signal_triggers, position=_previous_at_wait_start)

                # Case-4 Waited enough time. Check if signal is what it was expected
                elif _wait_entry_confirmation_tracker == self.wait_entry_confirmation:
                    # signal as expected
                    if current == _expected_signal:
                        self._change_position(_previous_at_wait_start, current, signal_triggers)
                        # if both wait_entry_confirmation and hold_x_days are active
                        if self.hold_x_days:
                            self._remain_position(signal_triggers, days=self.hold_x_days, position=current)
                            previous, current = current, 0
                            self._change_position(previous, current, signal_triggers)
                            idx += self.hold_x_days + 1
                    # Signal different from what was expected = eactivate tracker and force going into neutral.
                    else:
                        current = 0
                        self._change_position(_previous_at_wait_start, current, signal_triggers)
                    _wait_entry_confirmation_tracker = None
                    _previous_at_wait_start = None
            if not self.wait_entry_confirmation and self.hold_x_days:
                if current == previous:
                    self._remain_position(signal_triggers, position=current)
                elif current in (-1, 1):
                    # enter position
                    self._change_position(previous, current, signal_triggers)
                    # stay there for x days
                    self._remain_position(signal_triggers, days=self.hold_x_days, position=current)
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
        # if any signal trigger is longer than index, it means that entered somwehere near the end and
        # hold posiition contraint made it to hold above index. truncating it here for simplicity
        ref_length = len(self.df.index[self.max_lookback:])
        if len(signal_triggers['entry_long']) > ref_length:
            for trigger in self._triggers:
                signal_triggers[trigger] = signal_triggers[trigger][:ref_length]
        signal_triggers_df = pd.DataFrame(signal_triggers, index=self.df.index[self.max_lookback:])
        if return_signal:
            return self._merge_final_signal(signal_triggers_df)
        else:
            return signal_triggers_df

    def _remain_position(self, signal_triggers, days=1, position=None):
        """
        Keep current position by not triggering any signal to change position.
        """
        self.final_positions.extend(days*[position])
        for trigger in self._triggers:
            signal_triggers[trigger].extend(days*[0])

    def _change_position(self, previous, current, signal_triggers):
        """
        Changes long/short/neutral "previous" position to current one.
        """
        self.final_positions.append(current)
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
            _conv_rule_id = list(rules_results.keys())[0]
            if not len(self.rules_results[_conv_rule_id]) == 0:
                _previous_state = self.rules_results[_conv_rule_id][-1]
            else:
                _previous_state = 0
            position_ints = {'long': 1, 'short': -1, 'neutral': 0}
            for position in aggregation_params.keys():
                for state in aggregation_params[position]:
                    _matches = [
                        True if rules_results[_conv_rule_id][rule] == result else False
                        for rule, result in state.items()    
                    ]
                    if all(_matches):
                        return position_ints[position]
            return _previous_state
        else:
            raise NotImplementedError('aggregation_type "{}" is not supported'.format(aggregation_type))

    def _get_ts(self, ts_name, idx, lookback):
        """
        Returns portion of timeseries used as rule input. If lookback is 6, then resulting array will have 7 elemements.
        That is: "current" element (idx+1) and 6 days of loockback period.

        If *ts_name* is list of names, result will be a dictionary with {ts_name1: array1, ... ts_nameN: arrayN}
        """
        if isinstance(ts_name, str):
            return self.data[ts_name][idx-lookback:idx+1]
        elif isinstance(ts_name, list):
            return {
                name: self.data[name][idx-lookback:idx+1]
                for name in ts_name
            }

    def _get_simple_rules_results(self, rules_ids, result_idx, as_dict=False, conv_rule_id=None):
        """
        `rules_ids` is iterable witch ids of simple rules. `result_idx` should be index of results
        base on which to output combined result.
        """
        if as_dict:
            return {
                conv_rule_id: {rid: self.rules_results[rid][result_idx] for rid in rules_ids}
            }
        return [self.rules_results[rid][result_idx] for rid in rules_ids]

    def _generate_initial_signal(self):
        """
        Goes over simple and convoluted rules to generate rules results. Then executes strategy (fixed or learning) 
        to get initial signal. Rules should be implemented the way that generated result is sequence of positions.
        For example: 1,1,0,0,-1,-1 (2 days holding long, 2 days neutral and 2 days short). If the rule
        logic is more "event driven" (e.g enter if something happened, exit if sth different happened)
        then use convoluted rule and appropriate `aggregation_type`. Such an output will be then transformed
        correctly.
        """
        initial_signal = []
        idx = self.max_lookback
        signal = 0
        # variables specific to learning strategy type
        if self.strategy_type == 'learning':
            if self.strategy_metric == 'voting':
                follow = {'_type': 'position'}
            # else:
            else:
                follow = {'_type': 'rule'}
            _is_tmp_review_span = False
            if self.strategy_review_span > 10:
                # to avoid too long periods at the begining of backfill, where one waits for enough data to 
                # learn from. review_span will be dynamically increased over time
                review_span = 5
                _is_tmp_review_span = True
            else:
                review_span = self.strategy_review_span
            review_span_tracker = self.init_review_span_tracker
        while idx < self.index:
            result_idx = idx-self.max_lookback
            if self._rules_results_loaded == False:
                # get and append results from simple rules
                for simple_rule in self.simple_rules:
                    _hold_fixed_days = simple_rule.get('hold_fixed_days', None)
                    if not _hold_fixed_days:
                        rule_res = simple_rule['func'](
                            self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                            **simple_rule['params']
                        )
                    else:
                        # if rule has holding fox x days on rule lvl set up it will output
                        # same result for x consequent days
                        if len(self.hold_x_days_rule_lvl[simple_rule['id']]) > 0:
                            rule_res = self.hold_x_days_rule_lvl[simple_rule['id']].pop()
                        else:
                            rule_res = simple_rule['func'](
                                self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                                **simple_rule['params']
                            )
                            # hold only long or short. ignore neutral
                            if rule_res in (-1, 1):
                                self.hold_x_days_rule_lvl[simple_rule['id']].extend(
                                    (_hold_fixed_days-1)*[rule_res]
                                )
                    self.rules_results[simple_rule['id']].append(rule_res)
            if (self._rules_results_loaded == False) or (self.load_only_simple == True):
                # get and append results from convoluted rules
                for conv_rule in self.convoluted_rules:
                    simple_rules_results = self._get_simple_rules_results(
                        conv_rule['simple_rules'],
                        result_idx,
                        as_dict=conv_rule['_is_state_base'],
                        conv_rule_id=conv_rule['id'],
                    )
                    self.rules_results[conv_rule['id']].append(
                        self.combine_simple_results(
                            rules_results=simple_rules_results,
                            aggregation_type=conv_rule['aggregation_type'], 
                            aggregation_params=conv_rule['aggregation_params'],
                        )
                    )
            if self.strategy_type == 'fixed':
                for rule in self.strategy_rules:
                    signal = self.rules_results[rule][result_idx]
                    # use signal from first encountered rule with -1 or 1. or leave 0
                    # that makes order of rules important. rules with highest priority
                    # should be earlier in list
                    if signal in (-1, 1):
                        break
            elif self.strategy_type == 'learning':
                review_span_tracker += 1
                if _is_tmp_review_span:
                    # dynamic review span at the begining of the backfill
                    if review_span_tracker == review_span:
                        # perform review and reset span tracker. +1 to make current result inclusive
                        follow['_value'] = self._review_performance(
                            strat_idx=self._get_learning_start_idx(result_idx+1),
                            end_idx=result_idx+1
                        )
                        review_span_tracker = self.init_review_span_tracker
                        # increase tmp review span, if new tmp span is >= actual - use and flag it
                        review_span += 5
                        if review_span >= self.strategy_review_span:
                            review_span = self.strategy_review_span
                            _is_tmp_review_span = False
                else:
                    # enough days to use actual review span
                    if review_span_tracker == review_span:
                        # perform review and reset span tracker. +1 to make current result inclusive
                        follow['_value'] = self._review_performance(
                            strat_idx=self._get_learning_start_idx(result_idx+1),
                            end_idx=result_idx+1
                        )
                        review_span_tracker = self.init_review_span_tracker
                # set up signal. if no rule/position yet - go neutral
                if not follow.get('_value', None):
                    signal = 0
                else:
                    if follow['_type'] == 'rule':
                        signal = self.rules_results[follow['_value']][result_idx]
                    elif follow['_type'] == 'position':
                        signal = follow['_value']
            initial_signal.append(signal)
            idx += 1
        return initial_signal

    def _get_learning_start_idx(self, result_idx):
        # if not enough days - take all previous results to fully cover memory span
        if result_idx <= self.strategy_memory_span-1:
            return 0
        return result_idx - self.strategy_memory_span
    
    def _review_performance(self, strat_idx=None, end_idx=None):
        """
        Output depends on metric. If "voting" it returns position which should be taken. Otherwise it returns 
        rule_id which should be followed.
        """
        # need to adjust start/end idx here as they refers to results idx not (not idx within df)
        df_start_idx = strat_idx+self.max_lookback
        df_end_idx = end_idx+self.max_lookback
        # for each rule and appropriate indices: calculate and store performance metric(s)

        cur_res = {}
        _best_rule = [(-9999999, None)]
        for rule_id in self.strategy_rules:
            # get given rules signals
            rule_signals = self.rules_results[rule_id][strat_idx:end_idx]
            # calculate performance metric against signals
            if self.strategy_metric == 'daily_returns':
                daily_returns = self.daily_returns__learning[df_start_idx:df_end_idx]
                metric = sum([ret*sig for ret,sig in zip(daily_returns, rule_signals)])
            elif self.strategy_metric == 'avg_log_returns':
                daily_log_returns = self.daily_log_returns__learning[df_start_idx:df_end_idx]
                _realized_rets = [ret*sig for ret,sig in zip(daily_log_returns, rule_signals)]
                metric = sum(_realized_rets) / len(_realized_rets)
            elif self.strategy_metric == 'avg_log_returns_held_only':
                daily_log_returns = self.daily_log_returns__learning[df_start_idx:df_end_idx]
                _realized_rets_pos = [ret*sig for ret,sig in zip(daily_log_returns, rule_signals) if sig != 0]
                try:
                    metric = sum(_realized_rets_pos) / len(_realized_rets_pos)
                except ZeroDivisionError:
                    metric = 0
            elif self.strategy_metric == 'voting':
                metric = (
                    rule_signals.count(-1),
                    rule_signals.count(0),
                    rule_signals.count(1),
                )
            self.past_reviews[rule_id].append(metric)
            cur_res[rule_id] = metric
            # for non voting strategies find best performing rule(s) already in loop
            if self.strategy_metric != 'voting':
                if metric > _best_rule[0][0]:
                    _best_rule = [(metric, rule_id)]
                elif metric == _best_rule[0][0]:
                    _best_rule.append((metric, rule_id))
        # get best historical results in case many rules have the same (best) result
        if self.strategy_metric != 'voting':
            if len(_best_rule) > 1:
                _best_hist = (
                    sum(self.past_reviews[_best_rule[0][1]])/len(self.past_reviews[_best_rule[0][1]]),
                    _best_rule[0][1]
                )
                for _, rule_id in _best_rule[1:]:
                    hist_perf = sum(self.past_reviews[rule_id])/len(self.past_reviews[rule_id])
                    if hist_perf > _best_hist[0]:
                        _best_hist = (hist_perf, rule_id)
                return _best_hist[1]
            return _best_rule[0][1]
        # in case of 'voting' - define most freqeunt position by counting results from all the rules signals
        else:
            positions_counts = {p: 0 for p in (-1,0,1)}
            for rule_id in self.strategy_rules:
                positions_counts[-1] += cur_res[rule_id][0]
                positions_counts[0] += cur_res[rule_id][1]
                positions_counts[1] += cur_res[rule_id][2]
            poll_results = max([(p, cnt) for p, cnt in positions_counts.items()], key=lambda x: x[1])
            most_freq_position = poll_results[0]
            majority_votes_cnt = poll_results[1]
            if list(positions_counts.values()).count(majority_votes_cnt) > 1:
                # if there is a tie in voting -> go neutral
                return 0
            return most_freq_position

    def _reset_final_positions(self):
        self.final_positions = self.max_lookback*[0]

    def _merge_final_signal(self, signal_triggers_df):
        final_signal = pd.merge(
            left=self.df,
            right=signal_triggers_df,
            how='left',
            left_index=True,
            right_index=True
        )
        final_signal.fillna(0, inplace=True)
        # truncate final_positions in case they are longer (can happen with e.g. strategy constraints)
        self.final_positions = self.final_positions[:final_signal.shape[0]]
        return final_signal   


def triggers_to_states(df):
    """
    DEPRECATED
    
    Note - this implementation is slow and no longer needed. Use final_positions attribute of
    SignalGenerator insted. Keeping it for backward compatibility

    Given final rule triggers (0, 1 for entry_long, exit_long, entry_short, exit_short),
    return position state at each day (like: 1, 1, 1, 0, 0, 0, -1, -1 .... etc.) as list.
    States are: -1 (short), 0 (neutral), 1 (long).
    """
    last_state = 0
    states = []
    for idx in range(len(df.index)):
        triggers_vals = df.iloc[idx][['entry_long', 'exit_long', 'entry_short', 'exit_short']]
        # go neutral
        if triggers_vals[1] == 1 or triggers_vals[3] == 1:
            last_state = 0
        # go long
        if triggers_vals[0] == 1:
            last_state = 1
        # go short 
        elif triggers_vals[2] == 1:
            last_state = -1
        states.append(last_state)
    return states