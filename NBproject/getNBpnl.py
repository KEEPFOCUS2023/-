import pandas as pd
from sqlalchemy import column
%time from hikyuu.interactive import*
iodog.open()
import h5py
import numpy as np

#STORE ATTRIBUTES IN HDF5


holding=pd.read_pickle('../holdings_20220901_20231013.pickle') #这个要1分半orz - -）
holding['col_participant_id']=holding['col_participant_id'].astype('str')
holding['date']=holding['date'].astype('int')
def shift(xs, n):
    if n >= 0:
        return np.concatenate((np.full(n, np.nan), xs[:-n]))
    else:
        return np.concatenate((xs[-n:], np.full(-n, np.nan)))
    
holding['stock_code']=holding.apply(lambda x: 'sz'+x['stock_code'][:6] if x['stock_code'][-1]=='Z' else 'sh'+x['stock_code'][:6],axis=1)

def filldate(test,datelst=datelst):
    key=test.name
    test.sort_values('trade_date',inplace=True)
    
    price=sm[key[0]].get_kdata(Query(Datetime(datelst[0]),Datetime(datelst[-1]),ktype=Query.DAY,recover_type=Query.BACKWARD)).to_df()['close']
    
    if len(test)!=267:
        test=test.merge(pd.Series(datelst,name='trade_date'),how='right',left_on='trade_date',right_on='trade_date')
        test['stock_code']=key[0]
        test['col_participant_name']=key[1]
    #     test['col_participant_id']=test['col_participant_id'].unique()[1]
        
    test=test.fillna(0).merge(price,how='left',left_on='trade_date',right_index=True)
    if len(test)!=267:
        print(f'Error: {key} length is : {len(test)}')
    return test.dropna(subset='close') #在这一步，有些hikyuu没有股票data的股票就会被删掉
h2=holding.groupby(['stock_code','col_participant_name']).apply(lambda x: filldate(x,datelst))
h2['close_diff']=h2.groupby(level=[0,1])['close'].shift(1)
h2['pnl']=(h2['close']-h2['close_diff'])*h2.groupby(level=[0,1])['col_shareholding'].shift(1)
h2['cum_pnl']=h2.groupby(level=[0,1])['pnl'].cumsum()

datelst=list(h2['trade_date'].unique())
stocklst=list(h2['stock_code'].unique())
namelst=list(h2['col_participant_name'].unique())
# keylst=list(h2.index.unique())

h1=dict(list(h2.groupby(level=[0,1])))
keylst=list(h1.keys())
# h2.loc[keylst[1]]
# f.close()
f=h5py.File('NB.hdf5','w')
f['keylst']=pd.DataFrame(keylst).astype('S')
f['namelst']=np.array([i.encode('utf-8') for i in namelst])
f['stocklst']=np.array([i.encode('utf-8') for i in stocklst])
f['datelst']=np.array([i.strftime("%Y%m%d").encode('utf-8') for i in datelst])

#STORE DATA IN HDF5
h3=h2.copy()
h3.drop(columns=['trade_date','stock_code','col_participant_id','col_participant_name'],inplace=True)
# f=h5py.File('NB.hdf5','a')
p=f.create_group('pnl_231012')
for i in range(len(keylst)):
    p.create_dataset(keylst[i][0]+' '+keylst[i][1],data=h3.loc[keylst[i]].values)
f.close()
