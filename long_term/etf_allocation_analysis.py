# built in
import functools
import itertools
import os
import statistics

# 3rd part
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as sc_stats

# custom
import get_ib_data


def get_etf_data():
    all_etf_data = {}
    etf_data_path = '/Users/slaw/osobiste/etfs_data'
    for fn in os.listdir(etf_data_path):
        if fn.endswith('.csv'):
            all_etf_data[fn.replace('.csv', '')] = pd.read_csv(os.path.join(etf_data_path, fn))
    for df in all_etf_data.values():
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    return all_etf_data


def analyse_allocation(
        selected_etfs=None, splits=None, etfs_cnt=(1, 2, 3), amount=None, no_samples=10, no_years=10,
        etfs_real_results=None, fixed_symbols=None
    ):
    """
    Runs simulations to get expexted return&risk for all combinations of ETFs, splits and ETF counts.
    - As splits one can divide bonds, equities and commodities ETFs into 50%|40%|10% or 40%|40%|20% of portfolio
    - ETF count is how much different ETFs will be purchased within given asset class. It can be 2 bonds ETF,
      3 equities ETF and 1 commodity ETF.
    - For each asset class one defines list of symbols (don't need to be equal amount per class).
    - There is no rebalancing in test (TODO: implement rebalancing each year)
    - One can use optional parameter to "fix" some symbols and assure that they are always used in simulation

    - Currently, it does NOT supprot simulation with different commodity classes in split (e.g bond+equity and bondy only)

    Parameters:    
    split: 
        % split between asset classes. It is list with {asset1: perc1, ...} where perc is integer (eg. 40 is 40%)
    etfs_cnt:
      Number of ETFs per asset class. Default is (1, 2, 3). This is then expanded into all combinations
      with no. of asset class. E.g. with two assest classes then: (1,1), (1,2), ... , 
      One can read those tuples as: (1 ETF in 1st cls, 1 ETF in 2nd cls, (1 ETF in 1st cls, 2 ETFs in 2nd cls) 
      and so on...
    selected_etfs:
      dict with asset class as key and list of ETF symbols as values.
    amount:
      size of portfolio
    no_samples:
      Final value per given split|etf_counts is takes as average from X trials. X is no_samples.
    no_years:
      For each sample result from X years is taken. X is no_years. Value for each year is chosen from
      normal distribution with (avg,std) taken from historical ETF results
    etfs_real_results:
      dict <sym: results_dict> containing all ETFs in analysis. results_dict should should contain values
      for ETF's 'avg_annualized_ret' and 'risk'. Those values will be used in simulation
    fixed_symbols:
      list of symbols. should be subset of `selected_etfs`. if used, they will be always part of 
      simulation
    """
    classes = sorted(list(selected_etfs.keys()))
    fixed_per_cls = {}
    if fixed_symbols:
        for cls in classes:
            _fixed_cls = set(fixed_symbols).intersection(set(selected_etfs[cls]))
            if len(_fixed_cls) > len(selected_etfs[cls]):
                raise ValueError(f'Too many fixed symbols per {cls}')
            fixed_per_cls[cls] = _fixed_cls
    classes_idxs = range(len(classes))
    splits = [[s[c] for c in classes] for s in splits]
    etf_cnts = list(itertools.product(*[etfs_cnt for _ in range(len(classes))]))
    # stores results from all setups and possibilities within them
    all_results = {}
    for setup in itertools.product(splits, etf_cnts):
        split, etf_cnt = setup[0], setup[1]        
        # print(f'Setup: {setup}, Split: {split}, ETF counts: {etf_cnt}')
        possibilities = _gen_possible_etfs(classes, selected_etfs, etf_cnt, fixed_per_cls)
        if possibilities == None:
            continue        
        # print(f'possibilities are: {possibilities}')
        for pos in possibilities:
            cls_symbols = [pos[idx] for idx in classes_idxs]
            # print(cls_symbols)
            invest_amounts = [(split[idx]/100)*amount  for idx in classes_idxs]
            # print(invest_amounts)
            samples_avg_anualized_ret = []
            samples_risk = []
            for sample in range(no_samples):
                """
                Simulates X years of results for each symbol in given possibility.
                For each asset class and appropriate symbols within given class it draws estimated
                return. Draw is taken from normal distribution for each earl independly.
                Avg and std for normal distr. is takes from actual results achieved by ETF in the past.
                
                class_results will have number of elements equal to number of classess. Each element
                will be a dictionary where keys are ETFs in given class and value list with amount at the
                end of each year.
                
                It is handled how much initially is invested in each symbol. This depends from % of total
                investment within assett class and number of ETFs within this class. Total amount for asset 
                class is divided equally between ETFs.
                """
                class_results = []
                for idx in classes_idxs:
                    cls_invest = invest_amounts[idx] / len(cls_symbols[idx])
                    cls_rets = {}
                    for cls_sym in cls_symbols[idx]:
                        cls_rets[cls_sym] = []
                        cls_r = np.random.normal(
                            etfs_real_results[cls_sym]['avg_annualized_ret'],
                            etfs_real_results[cls_sym]['risk'],
                            no_years
                        )
                        for yidx, yr in enumerate(cls_r):
                            cls_invest = cls_invest + (cls_invest*(yr/100))
                            cls_rets[cls_sym].append(cls_invest)
                    class_results.append(cls_rets)
                """
                Calculate yeary total value of the account. From that get yeary total returns.
                Based on that get final samples results: average annualized return and riks (std.dev)
                """
                yearly_value = [amount]
                for idx in range(no_years):
                    value = 0
                    for cls_res in class_results:
                        for cls_sym in cls_res.keys():
                            value += cls_res[cls_sym][idx]
                    yearly_value.append(value)
                yearly_returns = []
                for i in range(no_years):
                    yearly_returns.append(
                        ((yearly_value[i+1]*100)/yearly_value[i])-100
                    )
                    
                avg_anualized_ret = (
                    (functools.reduce(
                        lambda x,y: x*y,
                        [1+(ret/100) for ret in yearly_returns]) ** (1/len(yearly_returns))
                    )-1
                )*100
                risk = round(statistics.stdev(yearly_returns), 2)
                # print(f'''results for all symbols by classes for sample({sample}): {class_results}. 
                # Yearly val: {yearly_value}. Yearly rets: {yearly_returns}
                # Avg. Annualized Ret. is {avg_anualized_ret} and risk is {risk}''')
                samples_avg_anualized_ret.append(avg_anualized_ret)
                samples_risk.append(risk)
            """
            Format proper name and store setup/possibility results (as averages from samples)
            """
            name_p1 = '|'.join(
                [f'{asset[:2]}{portion}' for asset, portion in zip(classes, split)]
            ) # e.g.: bo40|co20|eq40
            name_p2 = ''.join([str(cnt) for cnt in etf_cnt]) # e.g. 111 if only 1 etf in each asset class
            name_p3 = '|'.join([sym for asset in pos for sym in asset]) # e.g. SYB4|SPAL|GBDV
            name = f'{name_p1}_{name_p2}_{name_p3}'
            all_results[name] = (
                sum(samples_avg_anualized_ret)/no_samples,
                sum(samples_risk)/no_samples,
            )
    return all_results


def _gen_possible_etfs(classes, selected_etfs, etf_cnt, fixed_per_cls):
    """
    `analyse_allocation` helper function to get available possibilities
    """ 
    combs = []
    for idx, cls in enumerate(classes):
        # None if there is not enough ETF symbols for count
        if len(selected_etfs[cls]) < etf_cnt[idx]:
            return None
        # same if there are more fixed symobls than desired count for asset class
        elif (len(fixed_per_cls) > 0) and len(fixed_per_cls.get(cls, set())) > etf_cnt[idx]:
            return None
        # simple case - no fixed symbols
        if len(fixed_per_cls) == 0:
            combs.append(
                list(itertools.combinations(selected_etfs[cls], etf_cnt[idx]))
            )
        # some of the symbols are fixed. make sure they are always included
        else:
            init_symbols = tuple(fixed_per_cls[cls])
            remaining_cnt = etf_cnt[idx] - len(init_symbols)
            if remaining_cnt >= 1:
                remaining_sym = list(set(selected_etfs[cls]).difference(fixed_per_cls[cls]))
                cmbs = list(itertools.combinations(remaining_sym, remaining_cnt))
                final_combinations = [init_symbols + cmb for cmb in cmbs]
            else:
                final_combinations = [init_symbols]
            combs.append(final_combinations)
    return list(
        itertools.product(*combs)
    )


def split_to_asset_class(matched_etfs, all_etf_data):
    available_symbols = set(all_etf_data.keys())
    classes = ('unknown', 'other', 'currency', 'commodity', 'bond', 'equity')
    etf_to_class = {k: [] for k in classes}
    for etf in matched_etfs:
        if etf['asset_class'] != None:
            for idx, ib_sym in enumerate(etf['ib_symbols']):
                if ib_sym in available_symbols:
                    etf_to_class[etf['asset_class']].append(etf['ib_symbols'][idx])
                    break
        else:
            etf_to_class['unknown'].append(etf['ib_symbols'][0])
    for c in classes:
        print(f'No of symbols with class: {c} is {len(etf_to_class[c])}')
    return etf_to_class


def get_returns(etfs_data):
    """
    Get average cumalitve returns and their standard deviations
    """
    prices_labels = ('Open', 'High', 'Low', 'Close')
    results = {}
    for sym, df in etfs_data.items():
        # print(f'Processing: {sym}')
        df.loc[:, 'year'] = df.index.year
        first_year = df.index[0].year
        last_year = df.index[-1].year
        # start from 1st "full" year. end on most recent year from data
        years =  list(range(first_year, last_year+1))[1:]
        if len(years) <= 1:
            #print('Not enough data for symbol: ', sym)
            continue
        yearly_cum_rets = []
        ignore_symbol = 0
        for y in years:
            df_year = df[df['year'] == y]
            if df_year.shape[0] == 0:
                ignore_symbol = 1
                break
            # init/final price is avg from open, high, low and close
            init_price = sum([df_year.iloc[0][c] for c in prices_labels])/len(prices_labels)
            final_price = sum([df_year.iloc[-1][c] for c in prices_labels])/len(prices_labels)
            cum_ret = round(((final_price/init_price) - 1)*100, 2)
            yearly_cum_rets.append(cum_ret)
            #print(f'[{y}] init price is: {init_price} and final price is: {final_price}. cum ret is: {cum_ret}%')
        if ignore_symbol == 1:
            # skip symbol in case of stange malformation/missing data
            continue
        avg_annualized_ret = (
            (functools.reduce(
                lambda x,y: x*y,
                [1+(ret/100) for ret in yearly_cum_rets]) ** (1/len(yearly_cum_rets))
            ) -1)*100
        if len(years) == 1:
            risk = None
        else:
            risk = round(statistics.stdev(yearly_cum_rets), 2)
        if risk > 100:
            # ignore extreme volotality and outliers
            continue
        results[sym] = {
            'years': years,
            'yearly_cum_rets': yearly_cum_rets,
            'avg_annualized_ret': round(avg_annualized_ret, 2),
            'risk': risk
        }
        #print(f'Avg. annualized ret is: {avg_annualized_ret}. Risk is: {risk}')
    return results


def filter_symbols(etfs_results, returns=None, risk=None, years=None):
    """
    Filter etfs based on returns, risk and years:
    - ETF returns have to be > then defined
    - ETF risk has to be < than defined
    - ETF years have to be < than defined
    """
    filtered_etfs = {}
    for etf, res in etfs_results.items():
        etf_ret = res['avg_annualized_ret']
        etf_risk = res['risk']
        etf_years = len(res['years'])
        if (returns !=None) and (etf_ret < returns):
            continue
        if (risk !=None) and (etf_risk > risk):
            continue
        if (years !=None) and (etf_years < years):
            continue
        filtered_etfs[etf] = res
    return filtered_etfs


def correlation_df(etf_results):
    etf_names_lst = list(etf_results.keys())
    pairs = list(itertools.product(etf_names_lst, etf_names_lst))
    corr_df = pd.DataFrame(
        {e: len(etf_names_lst)*[.0] for e in etf_names_lst},
        index=etf_names_lst,
        columns=etf_names_lst,
    )
    for a1, a2 in pairs:
        common_years = sorted(
            list(set(etf_results[a1]['years']).intersection(set(etf_results[a2]['years'])))
        )
        # print(f'Common years for {a1}|{a2}: {common_years}')
        xs_idxs = [etf_results[a1]['years'].index(y) for y in common_years]
        xs = [etf_results[a1]['yearly_cum_rets'][idx] for idx in xs_idxs]
        ys_idxs = [etf_results[a2]['years'].index(y) for y in common_years]
        ys = [etf_results[a2]['yearly_cum_rets'][idx] for idx in ys_idxs]
        corr, _ = sc_stats.pearsonr(xs, ys)
        corr = round(corr, 3)
        if (a1 != a2) and (corr in (-1.0, 1.0)):
            print(f'{a1}|{a2}, cor:{corr}, common years: {common_years}')
        corr_df.at[a1, a2] = corr
    return corr_df


def risk_vs_ret_plot(xs, ys, names):
    fig,ax = plt.subplots()
    sc = plt.scatter(xs,ys)
    annot = ax.annotate("", xy=(0,0), xytext=(20,20),textcoords="offset points",
                        bbox=dict(boxstyle="round", fc="w"),
                        arrowprops=dict(arrowstyle="->"))
    annot.set_visible(False)

    def update_annot(ind):
        pos = sc.get_offsets()[ind["ind"][0]]
        annot.xy = pos
        text = "{}".format(' '.join([names[n] for n in ind["ind"]]))
        annot.set_text(text)
        annot.get_bbox_patch()


    def hover(event):
        vis = annot.get_visible()
        if event.inaxes == ax:
            cont, ind = sc.contains(event)
            if cont:
                update_annot(ind)
                annot.set_visible(True)
                fig.canvas.draw_idle()
            else:
                if vis:
                    annot.set_visible(False)
                    fig.canvas.draw_idle()

    fig.canvas.mpl_connect("motion_notify_event", hover)
    plt.show()


def print_stacked_etfs_data(etfs_names, etfs_data):
    fig, ax = plt.subplots(len(etfs_names), 1, figsize=(8,7))
    # find common years across all ETFs to plot only those
    etfs_years = [set(etfs_data[name]['year'].unique()) for name in etfs_names]        
    common_years = etfs_years[0] & etfs_years[1]
    for etf_y in etfs_years[2:]:
        common_years = common_years & etf_y
    # plot    
    for idx, name in enumerate(etfs_names):
        ax[idx].plot(
            etfs_data[name][etfs_data[name]['year'].isin(common_years)].index.tolist(),
            etfs_data[name][etfs_data[name]['year'].isin(common_years)]['Close'],
        )
        ax[idx].title.set_text(name)
    fig.tight_layout()


def main():
    # all_etf_data is dict {symbol: df}, where df is investpy df for symbol
    all_etf_data = get_etf_data()

    """
    matched_etfs is list with parsed metadata of IB and Investpy matched ETFs. e.g.:
    [
        {
            'etf': 'Amun Bitcoin Cash',
            'country': 'switzerland',
            'currency': 'USD',
            'stock_exchange': 'Switzerland',
            'asset_class': 'other',
            'ib_symbols': ['ABCH']
        },
        ...
    ]
    """
    matched_etfs = get_ib_data.get_matched_etfs()
    etfs_by_class = split_to_asset_class(matched_etfs)

    # example for commodity ETFs
    commodity_etfs = {s:all_etf_data[s] for s in etfs_by_class['commodity']}
    commodity_results = get_returns(commodity_etfs)
    selected_com_etf = filter_symbols(commodity_results, returns=12, risk=20, years=3)


    # example for allocation analysis
    selected_etfs = {
        'bond': ['SYB4', 'SAAA'],
        'equity': ['GBDV', 'IDJG'],
        'commodity': ['SPAL', 'IPDM', 'CHGX', 'EWG2']
    }
    splits = [
        {'bond': 40, 'equity': 40, 'commodity': 20},
        {'bond': 50, 'equity': 30, 'commodity': 20},
        {'bond': 30, 'equity': 50, 'commodity': 20},
        {'bond': 45, 'equity': 45, 'commodity': 10},
        {'bond': 40, 'equity': 50, 'commodity': 10},
        {'bond': 30, 'equity': 60, 'commodity': 10},
        {'bond': 35, 'equity': 60, 'commodity': 5},
    ]
    amount = 30000
    selected_etfs_symbols = [sym for lst in selected_etfs.values() for sym in lst]
    etfs_real_results = get_returns({s:all_etf_data[s] for s in selected_etfs_symbols})
    alcs_results = analyse_allocation(
        selected_etfs=selected_etfs,
        splits=splits,
        amount=amount,
        etfs_real_results=etfs_real_results,
    )

    # correlaction df example
    corr_df = correlation_df(etfs_real_results)


def test_gen_possible_etfs():
    classes = ['bond', 'commodity', 'equity']
    selected_etfs = {
        'bond': ['SYB4', 'SAAA'],
        'equity': ['GBDV', 'IDJG'],
        'commodity': ['SPAL', 'IPDM', 'CHGX', 'EWG2']
    }
    etf_cnt = (2,1,2)
    fixed_per_cls = {
        'bond': {'SYB4', 'SAAA'},
        'equity': {'GBDV', 'IDJG'},
        'commodity': {},
    }

    res = _gen_possible_etfs(classes, selected_etfs, etf_cnt, fixed_per_cls)

    for pos in res:
        print(pos)


if __name__ == '__main__':
   # main()

   test()
