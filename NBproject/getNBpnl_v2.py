import h5py
import datetime
import pandas as pd
import numpy as np
from hikyuu.interactive import *

def get_participant_df(participant_id):
    with h5py.File('./mission0/total_holdings.h5','r') as f:
        datelst=list(f.keys())
        datelst.remove('col_name')
        daily_holding=[]
        for date in datelst:
            try:
                temp=f[date].asstr()[:]
                daily_holding.append(temp[temp[:,0]==participant_id,:])
            except IndexError:
                print(date)
                pass
    # # one name for 1m 4s
        # print(f['dtypes'])
        par_df=pd.DataFrame(np.concatenate(daily_holding),columns=f['col_name'].asstr())
        par_df=par_df.astype(dtype={'col_shareholding':'float64'})
    return par_df
def shift(xs, n):
    if n >= 0:
        return np.concatenate((np.full(n, np.nan), xs[:-n]))
    else:
        return np.concatenate((xs[-n:], np.full(-n, np.nan)))

def filldate(test,datelst=datelst):
    key=test.name
    test.sort_values('trade_date',inplace=True)
    if len(test)!=len(datelst):
        test=test.merge(pd.Series(datelst,name='trade_date'),how='right',on='trade_date')
        test['stock_code']=key[0]
        test['col_participant_name']=key[1]
    #     test['col_participant_id']=test['col_participant_id'].unique()[1]
    
    
    price=sm[key[0]].get_kdata(Query(Datetime(datelst[0]),Datetime(datelst[-1]),ktype=Query.DAY,recover_type=Query.BACKWARD)).to_df()['close']
    test=test.merge(price,how='left',left_on='date',right_index=True) #如果test加上fillna date这一列就会有空值 不能merge
    if len(test)!=len(datelst):
        print(f'Error: {key} length is : {len(test)}')
    return test.dropna(subset='close') #在这一步，有些hikyuu没有股票data的股票就会被删掉

def get_participant_pnl(ID):
    holding=get_participant_df(ID)
    holding['stock_code']=holding.apply(lambda x: 'sz'+x['stock_code'][:6] if x['stock_code'][-1]=='Z' else 'sh'+x['stock_code'][:6],axis=1)
    holding['date']=pd.to_datetime(holding['trade_date'],format='%Y%m%d').astype('<M8[s]')
    datelst=holding['trade_date'].unique().tolist()
    stocklst=holding['stock_code'].unique().tolist()

    #耗时10s左右
    h2=holding.groupby(['stock_code','col_participant_name']).apply(lambda x: filldate(x,datelst))
    h2['close_diff']=h2.groupby(level=[0,1])['close'].shift(1)
    h2['ret']=np.log(h2['close'])-np.log(h2['close_diff'])
    h2['mv']=h2['close']*h2['col_shareholding']
    h2['pnl']=(h2['close']-h2['close_diff'])*h2.groupby(level=[0,1])['col_shareholding'].shift(1)
    h2['cum_pnl']=h2.groupby(level=[0,1])['pnl'].cumsum()
    h2.groupby('trade_date')['cum_pnl'].sum().plot()
    return h2
get_participant_pnl('C00093')