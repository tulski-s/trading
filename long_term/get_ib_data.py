# built in
import datetime
import os

# 3rd party
import investpy
import pandas as pd


IB_ETFS_PATH = '/Users/slaw/osobiste/trading/long_term'


def download_etfs_data(matched_etfs):
    all_etfs = {}
    print('Getting data for all ETFs...')
    for etf in matched_etfs:
        print(f"Getting data for: {etf['etf']}")
        try:
            etf_data = investpy.get_etf_historical_data(
                etf=etf['etf'],
                country=etf['country'],
                stock_exchange=etf['stock_exchange'],
                from_date=datetime.date(1990,1,1).strftime('%d/%m/%Y'),
                to_date=datetime.datetime.now().strftime('%d/%m/%Y'),
            )
        except Exception as e:
            print(f'Error while getting: {etf}. Traceback: {repr(e)}')
            continue
        all_etfs[etf['ib_symbols'][0]] = etf_data
    return all_etfs


def match_ib_investpy(ib_etfs_df_grp, etfs):
    """
    match investpy ETFs with IB ones
    """
    ib_2_investpy = []
    for idx, row in ib_etfs_df_grp.iterrows():
        name, currency, ib_symbols, symbols = row
        for sym in symbols:
            available_etfs = etfs[
                (etfs['symbol'] == sym) & (etfs['currency'] == currency)
            ]
            no_etfs = available_etfs.shape[0]
            if no_etfs == 1:
                ib_2_investpy.append(
                    _create_etf_row(available_etfs.iloc[0], ib_symbols)
                )
            elif no_etfs > 1:
                no_def_ex = available_etfs[
                    available_etfs['def_stock_exchange'] == True
                ].shape[0]
                # there is only one def stock exchange
                if no_def_ex == 1:
                    ib_2_investpy.append(
                        _create_etf_row(
                            available_etfs[available_etfs['def_stock_exchange'] == True].iloc[0],
                            ib_symbols
                        )
                    )
                # many def stock exchanges
                elif no_def_ex > 1:
                    ib_2_investpy.append(
                        _create_etf_row(
                            available_etfs[available_etfs['def_stock_exchange'] == True].iloc[0],
                            ib_symbols
                        )
                    )
                # no def stock exchange
                else:
                    ib_2_investpy.append(
                        _create_etf_row(
                            available_etfs.iloc[0], ib_symbols
                        )
                    )
    return ib_2_investpy


def _create_etf_row(etf, ib_symbols):
        return {
            'etf': etf['name'],
            'country': etf['country'],
            'currency': etf['currency'],
            'stock_exchange': etf['stock_exchange'],
            'asset_class':etf['asset_class'],
            'ib_symbols': list(ib_symbols)
        }


def get_matched_etfs():
    # prepare ETFs available on IB platform
    ib_etfs_df = pd.read_csv(os.path.join(IB_ETFS_PATH, 'ib_etfs.csv'))
    # group IB ETFs to distinct (name, currency) and aggregate possible symbols
    # symbol used in IB does not always corresponds to the one from investpy
    group_key = ['NAME', 'CURRENCY']
    ib_etfs_df_grp = ib_etfs_df.groupby(group_key, as_index=False)['IB_SYMBOL', 'SYMBOL'].aggregate(lambda x: set(x))
    ib_etfs_df_grp[
        ib_etfs_df_grp['NAME'] == '21Shares Bitcoin ETP'
    ]
    ib_etfs_df_grp['UNIQUE_SYMBOLS'] = ib_etfs_df_grp.apply(
        lambda row: row.IB_SYMBOL.union(row.SYMBOL),
        axis=1
    )
    ib_etfs_df_grp = ib_etfs_df_grp[['NAME', 'CURRENCY', 'IB_SYMBOL', 'UNIQUE_SYMBOLS']]
    ib_etfs_df_grp.rename(
        columns={
            'NAME': 'name',
            'CURRENCY': 'currency',
            'IB_SYMBOL': 'ib_symbol',
            'UNIQUE_SYMBOLS': 'symbols'
        },
        inplace=True
    )
    # ETFs available in investpy
    etfs = investpy.get_etfs()
    # match both sources and get data. it can take a long while (all ETF took couple of hours)
    matched_etfs = match_ib_investpy(ib_etfs_df_grp, etfs)
    return matched_etfs


def main():
    # prepare and match ETFs available on IB platform and Investpy api
    matched_etfs = get_matched_etfs()
    all_etf_data = download_etfs_data(matched_etfs)
    print(f'Got data for {len(test)} symbol(s)')

    # wirte data to file
    for etf_symbol, etf_df in all_etf_data.items():
        etf_df.to_csv(
            os.path.join(IB_ETFS_PATH, f'{etf_symbol}.csv')
        )


if __name__ == '__main__':
    main()
