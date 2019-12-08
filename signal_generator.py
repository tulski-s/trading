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
        # TODO(slaw) - would be good to have config validation here
        self.strategy_type = config['strategy']['type']
        self.strategy_rules = config['strategy']['strategy_rules']
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
            self.df.loc[:, 'prev_price__learning'] = df[self.strategy_price_label].shift(1)
            self.df.loc[:, 'daily_returns__learning'] = df[self.strategy_price_label].pct_change()
            self.df.loc[:, 'daily_log_returns__learning'] = (
                np.log(df[self.strategy_price_label]) - df['prev_price__learning']
            )

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
                self.data[rule['ts']] = df[rule['ts']].to_numpy()
                assert(self.data[rule['ts']].shape[0] == self.index)
                # check max lookback. in 'generate' it will be starting point so all data is present
                if self.max_lookback < rule['lookback']:
                    self.max_lookback = rule['lookback']
            elif rule['type'] == 'convoluted':
                if rule['aggregation_type'] == 'state-based':
                    rule['_is_state_base'] = True
                else:
                    rule['_is_state_base'] = False
                self.convoluted_rules.append(rule)
        self._triggers = ('entry_long', 'exit_long', 'entry_short', 'exit_short')

    def generate(self):
        initial_signal = self._generate_initial_signal()
        if any([self.wait_entry_confirmation, self.hold_x_days]):
            return self._generate_final_signal_with_constraints(initial_signal)
        return self._generate_final_signal(initial_signal)

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

    def _generate_final_signal(self, initial_signal):
        """
        Transforms initial signal to the final output. That is, it takes sequence of positions like
        1,1,0,0,0,-1,-1 and outputs oryginal pricing dataframe with 4 additional columns: enter_long,
        exit_long, enter_short, exit_short. Such an format is expected by backtester.

        No constraints are implemented here such just transformation is performed.
        """
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
        """
        Same as _generate_final_signal but with constraints implementation. Contraints can be used 
        separately or together. Available contraints are:
        -> wait_entry_confirmation - wait without changing position for defined amount of
        days. On the last waiting day checks if rule still shows position which was initally
        attmpted to enter. If signal is the same enter. Else, change to neutral position.
        -> hold_x_days - after entering position hold additional x days. Change to neutral
        position after that.
        """
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
                        # if both wait_entry_confirmation and hold_x_days are active
                        if self.hold_x_days:
                            self._remain_position(signal_triggers, days=self.hold_x_days)
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
        # if any signal trigger is longer than index, it means that entered somwehere near the end and
        # hold posiition contraint made it to hold above index. truncating it here for simplicity
        ref_length = len(self.df.index[self.max_lookback:])
        if len(signal_triggers['entry_long']) > ref_length:
            for trigger in self._triggers:
                signal_triggers[trigger] = signal_triggers[trigger][:ref_length]

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
        """
        Keep current position by not triggering any signal to change position.
        """
        for trigger in self._triggers:
            signal_triggers[trigger].extend(days*[0])

    def _change_position(self, previous, current, signal_triggers):
        """
        Changes long/short/neutral "previous" position to current one.
        """
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
        """
        return self.data[ts_name][idx-lookback:idx+1]

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
            # performance_reviews = {}
            if self.strategy_review_span > 10:
                # to avoid too long periods at the begining of backfill, where one waits for enough data to 
                # learn from. review_span will be dynamically increased over time
                review_span = 5
                _is_tmp_review_span = True
            else:
                review_span = self.strategy_review_span
        review_span_tracker = 0
        while idx < self.index:
            result_idx = idx-self.max_lookback
            # get and append results from simple rules
            for simple_rule in self.simple_rules:
                self.rules_results[simple_rule['id']].append(
                    simple_rule['func'](
                        self._get_ts(simple_rule['ts'], idx, simple_rule['lookback']),
                        **simple_rule['params']
                    )
                )
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
                    if signal in (-1, 1):
                        break
            elif self.strategy_type == 'learning':
                if self.strategy_metric != 'voting':
                    follow = {'_type': 'position'}
                else:
                    follow = {'_type': 'rule'}
                if _is_tmp_review_span:
                    # dynamic review span at the begining of the backfill
                    if review_span_tracker == review_span:
                        # perform review and reset span tracker
                        follow['_value'] = self._review_performance(
                            strat_idx=self._get_learning_start_idx(result_idx),
                            end_idx=result_idx+1
                        )
                        review_span_tracker = 0
                        # increase tmp review span, if new tmp span is >= actual - use and flag it
                        review_span += 5
                        if review_span >= self.strategy_review_span:
                            review_span - self.strategy_review_span
                            _is_tmp_review_span = False
                else:
                    # enough days to use actual review span
                    if review_span_tracker == review_span:
                        # perform review and reset span tracker
                        follow['_value'] = self._review_performance(
                            strat_idx=self._get_learning_start_idx(result_idx),
                            end_idx=result_idx+1
                        )
                        review_span_tracker = 0
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
            review_span_tracker += 1
        return initial_signal

    def _get_learning_start_idx(self, result_idx):
        # if not enough days - take all previous results to fully cover memory span
        if result_idx < self.strategy_memory_span-1:
            return 0
        return result_idx - self.strategy_memory_span + 1
    
    def _review_performance(self, strat_idx=None, end_idx=None):
        """
        Output depends on metric. If "voting" it returns position which should be taken. Otherwise it returns 
        rule_id which should be followed.
        """
        # need to adjust start/end idx here as they refers to results idx not (not idx within df)
        df_start_idx = strat_idx+self.max_lookback
        df_end_idx = end_idx+self.max_lookback
        # for each rule and appropriate indices: calculate and store performance metric(s)
        for rule_id in self.strategy_rules:
            # get given rules signals
            rule_signals = np.array(self.rules_results[rule_id][strat_idx:end_idx])
            # calculate performance metric against signals
            if self.strategy_metric == 'daily_returns':
                daily_returns = np.array(self.df['daily_returns__learning'][df_start_idx:df_end_idx])
                metric = sum(daily_returns * rule_signals)
            elif self.strategy_metric == 'avg_log_returns':
                daily_log_returns = np.array(self.df['daily_log_returns__learning'][df_start_idx:df_end_idx])
                metric = avg(daily_log_returns * rule_signals)
            elif self.strategy_metric == 'avg_log_returns_held_only':
                daily_log_returns = np.array(self.df['daily_log_returns__learning'][df_start_idx:df_end_idx])
                daily_log_returns_from_positions = daily_log_returns * rule_signals
                metric = avg(daily_log_returns_from_positions[daily_log_returns_from_positions != 0])
            elif self.strategy_metric == 'voting':
                metric = (
                    sum(rule_signals == -1),
                    sum(rule_signals == 0),
                    sum(rule_signals == 1),
                )
            self.past_reviews[rule_id].append(metric)
        # get current review results
        review_idx = len(self.past_reviews[self.strategy_rules[0]]) - 1
        cur_res = {
            rule_id: metric_vals[review_idx] for rule_id, metric_vals in self.past_reviews.items()
        }
        # define best performing rule. if tie - use the one with better historical performance
        if self.strategy_metric != 'voting':
            cur_best_rule, cur_best_val = max([(k,v) for k,v in cur_res.items()], key=lambda x: x[1])
            if list(cur_res.values()).count(cur_best_val) > 1:
                # calculate historical performance
                all_best_rules = [r for r, v in cur_res.items() if v == cur_best_val]
                avg_metric_res = [
                    (rule_id, avg(self.past_reviews[rule_id]))
                    for rule_id in all_best_rules
                ]
                # overwrite best rule. in case historical average is the same for some rules - just choose
                # first (that is achieved via "max" which choose first max value occured in list)
                cur_best_rule = max(avg_metric_res, key=lambda x: x[1])[0]
            return cur_best_rule
        # in case of 'voting' - define most freqeunt position by counting results from all the rules signals
        else:
            positions_counts = {p: 0 for p in (-1,0,1)}
            for rule_id in self.strategy_rules:
                positions_counts[-1] += cur_res[rule_id][0]
                positions_counts[0] += cur_res[rule_id][1]
                positions_counts[1] += cur_res[rule_id][2]
            return max([(p, cnt) for p, cnt in positions_counts.items()], key=lambda x: x[1])[0]
