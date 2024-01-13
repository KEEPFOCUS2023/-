'''
Author: dkl
Description: aaa
Date: 2023-07-20 09:37:47
'''
'''
El Update version3.0
Tidy up the code and store data in hdf5.
Date: 2024-01-13 00:02:00
'''

from fake_useragent import UserAgent
import threading
import requests
import os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
import h5py
# %time from hikyuu.interactive import*
# iodog.open()

class BasicSpyder(object):
    def __init__(self, maxtries=50, timeout=5):
        self.maxtries = maxtries
        self.timeout = timeout
        self.lock = threading.Lock()

    def get(self, url, params=None):
        if not isinstance(url, str):
            raise ValueError('url must be str')
        tries = 0
        while tries <= self.maxtries:
            try:
                headers = {'User-Agent': UserAgent().random}
                response = requests.get(url=url,
                                        params=params,
                                        timeout=self.timeout,
                                        headers=headers)
                if response.status_code == 200:
                    return response
            except Exception as e:
                print(e)
                continue
        print('Reach maxtries')
        return response



def divide_lst(lst, n_groups):
    # 每个组应该分配的数量
    group_num = int(len(lst) / n_groups)
    res_lst = []
    for i in range(n_groups - 1):
        idx_start = group_num * i
        idx_end = group_num * (i + 1)
        res_lst.append(lst[idx_start:idx_end])
    res_lst.append(lst[group_num * (n_groups - 1):])
    return res_lst


class MySpyder(BasicSpyder):
    def __init__(self, maxtries=50, timeout=5):
        super().__init__(maxtries, timeout)
        self.data_lst = []

    def get_hsgt_stock(self, trade_date=None):
        '''
        Description
                            ----------
        获取指定时间的沪深股通股票和对应的A股/港股代码

        Parameters
                            ----------
        trade_date: str. 指定时间.格式为YYYYMMDD. 默认为None,即昨天
            非交易日也可，获取结果与上个交易日相同

        Return
                            ----------
        pandas.DataFrame. columns是trade_date, hkshare_code, ashare_code, name
        依次为交易日, 港股代码, A股代码, 股票名称
        '''
        if trade_date is None:
            trade_date = datetime.datetime.today() - datetime.timedelta(days=1)
            trade_date = trade_date.strftime(r'%Y%m%d')
        url = f"""https://www3.hkexnews.hk/sdw/search/stocklist.aspx?
                  sortby=stockcode&shareholdingdate={trade_date}"""
        response = self.get(url.replace(" ", "").replace("\n", ""))
        text = response.text
        lst = eval(text)
        df = pd.DataFrame(lst)
        df.columns = ['hkshare_code', 'name']

        def get_ashare_code(x):
            pattern = r'\(A #(.*?)\)'
            lst = re.findall(pattern, x)
            if len(lst) == 0:
                return None
            else:
                s = lst[0]
                if s[0] == '6':
                    return s + '.SH'
                else:
                    return s + '.SZ'

        df['ashare_code'] = df['name'].apply(get_ashare_code)
        df['trade_date'] = trade_date
        df = df[['trade_date', 'hkshare_code', 'ashare_code', 'name']].copy()
        df = df.dropna().reset_index(drop=True)
        return df

    def get_ccass_hold_detail(self,
                              hkshare_code,
                              ashare_code=None,
                              trade_date=None):
        '''
        Description
                                ----------
        获取中央结算系统的持股量

        Parameters
                                ----------
        hkshare_code: str. 港股代码. 
        ashare_code: str. A股代码. 默认为None, 即返回结果的stock_code为港股代码
        trade_date: str. 指定时间.格式为YYYYMMDD. 默认为None,即昨天
            非交易日也可，获取结果与上个交易日相同

        Return
                                ----------
        pandas.DataFrame.
        '''
        if ashare_code is None:
            ashare_code = hkshare_code
        if trade_date is None:
            trade_date = datetime.datetime.today() - datetime.timedelta(days=1)
            trade_date = trade_date.strftime(r'%Y%m%d')
        url = r'https://www3.hkexnews.hk/sdw/search/searchsdw.aspx/'
        shareholdingdate = '/'.join(
            [trade_date[0:4], trade_date[4:6], trade_date[6:]])
        params = {
            '__EVENTTARGET': 'btnSearch',
            '__EVENTARGUMENT': '',
            'sortBy': 'participantid',
            'sortDirection': 'asc',
            'alertMsg': '',
            'txtShareholdingDate': shareholdingdate,
            'txtStockCode': hkshare_code
        }
        response = self.get(url, params=params)
        htmltext = response.text
        soup = BeautifulSoup(htmltext, 'lxml')
        content = soup.find(
            "div",
            attrs={
                "class":
                "search-details-table-container table-mobile-list-container"
            })
        if content is None:
            col_lst = [
                "stock_code", "trade_date", "col_participant_id",
                "col_participant_name", "col_shareholding",
                "col_shareholding_percent"
            ]
            return pd.DataFrame(columns=col_lst)
        body_lst = content.find_all('tbody')
        if len(body_lst) == 0:
            col_lst = [
                "stock_code", "trade_date", "col_participant_id",
                "col_participant_name", "col_shareholding",
                "col_shareholding_percent"
            ]
            return pd.DataFrame(columns=col_lst)
        body = body_lst[0]
        body = body.find_all('tr')
        rows=[]
        for soup in body:
            data = {
                'col_participant_id': soup.find('td', class_='col-participant-id').find('div', class_='mobile-list-body').text,
                'col_participant_name': soup.find('td',class_='col-participant-name').find('div', class_='mobile-list-body').text,
                'col_shareholding': soup.find('td', class_='col-shareholding text-right').find('div', class_='mobile-list-body').text,
                'col_shareholding_percent': soup.find('td', class_='col-shareholding-percent text-right').find('div', class_='mobile-list-body').text
                }
            rows.append(data)
            
        df=pd.DataFrame(rows)
        df['stock_code']=ashare_code
        df['trade_date']=trade_date
        df['col_shareholding']=df['col_shareholding'].str.replace(',','').astype(int)
        df['col_shareholding_percent']=df['col_shareholding_percent'].str.replace('%','').astype(float)
        df = df.reset_index(drop=True)
        # print(f'finished: {trade_date}: {ashare_code}')
        return df

    def spyder_main(self, hkshare_code_lst, ashare_code_lst, trade_date):
        df = pd.DataFrame()
        df_lst=[]
        for i in range(len(hkshare_code_lst)):
            hkshare_code = hkshare_code_lst[i]
            ashare_code = ashare_code_lst[i]
            tempdf = self.get_ccass_hold_detail(hkshare_code, ashare_code,
                                                trade_date)
            # print(tempdf)
            # df = pd.concat([df, tempdf])
            df_lst.append(tempdf)
        df=pd.concat(df_lst)
        df = df.reset_index(drop=True)
        self.lock.acquire()
        self.data_lst.append(df)
        self.lock.release()
        return


def main(start_date, end_date, n_threads=10):
# start_date='20240101'
# end_date='20240110'
# n_threads=8
    date_lst = pd.date_range(start_date, end_date, freq='D')
    date_lst = [x.strftime(r'%Y%m%d') for x in date_lst]

    # sm.get_trading_calendar(Query(-365))

    print(f'date_lst:{date_lst}')
    if not os.path.exists('ccass_hold_detail'):
        os.mkdir('ccass_hold_detail')


    f=h5py.File(f'holdings_{start_date}_{end_date}.h5','w')

    for trade_date in date_lst:
        myspyder = MySpyder(maxtries=50)
        stock_code_df = myspyder.get_hsgt_stock(trade_date)
        print('get_stock_code_df')
        hkshare_code_lst_total = stock_code_df['hkshare_code'].tolist()
        ashare_code_lst_total = stock_code_df['ashare_code'].tolist()
        hkshare_code_lst_thread = divide_lst(hkshare_code_lst_total, n_threads)
        ashare_code_lst_thread = divide_lst(ashare_code_lst_total, n_threads)
        iter_zip = zip(hkshare_code_lst_thread, ashare_code_lst_thread)
        # 创建线程并启动
        threads = []
        for hkshare_code_lst, ashare_code_lst in iter_zip:
            thread = threading.Thread(target=myspyder.spyder_main,
                                        kwargs={
                                            'hkshare_code_lst': hkshare_code_lst,
                                            'ashare_code_lst': ashare_code_lst,
                                            'trade_date': trade_date
                                        })
            threads.append(thread)
            thread.start()
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        df = pd.concat(myspyder.data_lst)
        f.create_dataset(trade_date,data=df.astype('S').values)
        print(f'finished: {trade_date}')
    
    #store the columns name and the data types for future restoration
    f.create_dataset('col_name',data=[i.encode('utf-8') for i in list(df.columns)])
    f.create_dataset('dtypes',data=list(df.dtypes.astype('S')))
    f.close()
    
if __name__ == '__main__':
    start_time = datetime.datetime.now()
    # main(start_date='20230908', end_date='20230908', n_threads=10)
    main(start_date='20231013', end_date='20231231', n_threads=10)
    end_time = datetime.datetime.now()
    print(f'Total time cost: {(end_time-start_time).seconds}s')
    
    # 5days for 38min
    # 2.5 months for 6 hours 