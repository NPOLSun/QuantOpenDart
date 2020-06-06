import OpenDartReader
import pandas as pd
import numpy as np
import sqlite3
import datetime

import requests
# API key 따로관리
import myAPI

def 특정코드check하기(code):
    result = dart.finstate_all(code, 2019, '11014')
    result.to_csv('code_test.csv', encoding='ms949')


def KRX코드DB저장():
    # KRX CSV 파일을 OPEN해서 정보 저장
    market_list = ['stockMkt', 'kosdaqMkt']
    market_list_index = ['KOSPI', 'KOSDAQ']
    for i, market in enumerate(market_list):
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&pageIndex=1&currentPageSize=5000&comAbbrv=&beginIndex=&orderMode=3&orderStat=D&isurCd=&repIsuSrtCd=&searchCodeType=&marketType=' \
              + market + '&searchType=13&industry=&fiscalYearEnd=all&comAbbrvTmp=&location=all'

        stock_df = pd.read_html(url, header=0)[0]
        stock_df['종목코드'] = stock_df['종목코드'].map(lambda x: f'{x:0>6}')
        code_list = stock_df['종목코드']

        con = sqlite3.connect('./DATA/{}_code_KRX.db'.format(market_list_index[i]))
        code_list.to_sql('code_list', con, if_exists="replace")


# 코드, 연도, 분기를 입력하면 dataframe으로 나오는 코드
def financial_data(code, year, quarter):
    if quarter==1:
        reprt_code = '11013'
    elif quarter==2:
        reprt_code = '11012'
    elif quarter==3:
        reprt_code = '11014'
    elif quarter==4:
        reprt_code = '11011'
    else:
        raise Exception('분기를 제대로 입력하세요')
    result = dart.finstate_all(code, year, reprt_code)

    return result

def data_collection_by_acc_id(code, item_list, item_sj_nm, item_account_id, year, quarter):
    data = financial_data(code, year, quarter)
    dict = {}

    try:
        data = data_to_standard(data)
    except:
        pass

    try:
        data['account_nm'] = data['account_nm'].str.replace(' ','')
        for i in range(len(item_list)):
            try:
                if item_sj_nm[i]=='포괄손익계산서':
                    value = data[((data['sj_nm'] == item_sj_nm[i]) | (data['sj_nm'] == '손익계산서')) & (data['account_id'] == item_account_id[i])]['thstrm_amount'].iloc[0]
                    dict[item_list[i]] = value
                else:
                    value = data[(data['sj_nm'] == item_sj_nm[i]) & (data['account_id'] == item_account_id[i])]['thstrm_amount'].iloc[0]
                    dict[item_list[i]] = value

            except:
                dict[item_list[i]] = None
        df_data = pd.DataFrame.from_dict(dict, orient='index')
    except:
        for i in range(len(item_list)):
            dict[item_list[i]] = None
        df_data = pd.DataFrame.from_dict(dict, orient='index')

    col_name = str(year) + '-' + str(quarter)
    df_data.columns = [col_name]
    return df_data

# data_to_standard 에서 반복작업이 들어가서 함수화 -- 계정명 변경하기 도움
def df_names_change(prev_list, std, df):
    for before in prev_list:
        df['account_id'] = df['account_id'].replace(before, std)
    return df

def data_to_standard(df):
    #유동자산
    df = df_names_change(['ifrs_CurrentAssets'], 'ifrs-full_CurrentAssets', df)

    #비유동자산
    df = df_names_change(['ifrs_NoncurrentAssets'], 'ifrs-full_NoncurrentAssets', df)

    #자산총계
    df = df_names_change(['ifrs_Assets'], 'ifrs-full_Assets', df)

    #유동부채
    df = df_names_change(['ifrs_CurrentLiabilities'], 'ifrs-full_CurrentLiabilities', df)

    #부채총계
    df = df_names_change(['ifrs_Liabilities'], 'ifrs-full_Liabilities', df)

    #자본금
    df = df_names_change(['ifrs_IssuedCapital'], 'ifrs-full_IssuedCapital', df)

    #자본총계
    df = df_names_change(['ifrs_Equity'], 'ifrs-full_Equity', df)

    #매출액
    df = df_names_change(['ifrs_Revenue'], 'ifrs-full_Revenue', df)
    df[(df['account_nm'] == '매출액') & ((df['sj_nm'] == '포괄손익계산서') | (df['sj_nm'] == '손익계산서'))]['account_id'] = 'ifrs-full_Revenue'

    #매충총이익
    df = df_names_change(['ifrs_GrossProfit'], 'ifrs-full_GrossProfit', df)
    df[(df['account_nm']=='매출총이익') & ((df['sj_nm']=='포괄손익계산서') | (df['sj_nm'] =='손익계산서'))]['account_id'] = 'ifrs-full_GrossProfit'

    #영업이익
    df[(df['account_nm'] == '영업이익') & ((df['sj_nm'] == '포괄손익계산서') | (df['sj_nm'] == '손익계산서'))]['account_id'] = 'dart_OperatingIncomeLoss'

    #당기순이익
    df = df_names_change(['ifrs_ProfitLoss'], 'ifrs-full_ProfitLoss', df)
    df[(df['account_nm'] == '당기순이익') & ((df['sj_nm'] == '포괄손익계산서') | (df['sj_nm'] == '손익계산서'))]['account_id'] = 'ifrs-full_ProfitLoss'

    # 영업활동현금흐름
    df = df_names_change(['ifrs_CashFlowsFromUsedInOperatingActivities'], 'ifrs-full_CashFlowsFromUsedInOperatingActivities', df)

    return df

# 오늘 날짜의 분기를 뽑아내는 함수
def today_quarter(today_month):
    return np.floor(today_month/3.1)+1

def make_y_q_list(year_now, quarter_now, quarters):
    prv_yrs_req = (quarter_now - quarters) // 4
    year_start = year_now + prv_yrs_req
    quarter_start = (quarter_now - quarters) % 4 + 1

    year = year_start
    quarter = quarter_start
    y_q_list = []
    while ((year != year_now) | (quarter != quarter_now)):
        y_q_list.append(str(year) + '-' + str(quarter))
        if quarter > 3:
            year += 1
            quarter = 1
        else:
            quarter += 1
    y_q_list.append(str(year_now) + '-' + str(quarter_now))
    return y_q_list



# 업데이트 필요할때만 가끔씩 하자
# KRX코드DB저장()


# 재무제표 저장에 필요한 내용

item_list = ['유동자산', '비유동자산', '자산총계', '유동부채', '부채총계', '자본금', '자본총계', '매출액', '매출총이익', '영업이익', '당기순이익', '영업활동현금흐름']
item_sj_nm=['재무상태표', '재무상태표', '재무상태표', '재무상태표', '재무상태표', '재무상태표', '재무상태표', '포괄손익계산서', '포괄손익계산서', '포괄손익계산서', '포괄손익계산서', '현금흐름표']
item_account_id = ['ifrs-full_CurrentAssets', 'ifrs-full_NoncurrentAssets', 'ifrs-full_Assets', 'ifrs-full_CurrentLiabilities', 'ifrs-full_Liabilities', 'ifrs-full_IssuedCapital','ifrs-full_Equity', 'ifrs-full_Revenue', 'ifrs-full_GrossProfit', 'dart_OperatingIncomeLoss', 'ifrs-full_ProfitLoss', 'ifrs-full_CashFlowsFromUsedInOperatingActivities']

# 계산 필요한 내용들
# 부채비율, PER, PSR, PBR, PCR, GP/A, ROE

# 재무상태표 긁기

# OPEN DART READER API 정보 입력 및 dart 인스턴스 생성
# API key 불러오기
api_key = myAPI.getAPIkey()
dart = OpenDartReader(api_key)


for market in ['KOSPI']:
    con_market = sqlite3.connect("./DATA/{}_code_KRX.db".format(market))
    code_list = pd.read_sql("SELECT * FROM code_list", con_market, index_col='index')
    con_market.close()
    code_list = list(code_list["종목코드"])

# 현 날짜 기준 최근 n분기 year-quarter list 산출
quarters = 6 # 최소 5 이상은 넣자 / 연초까지는 좀.. 되게?
today = datetime.date.today()
#Year-Quarter List
y_q_list = make_y_q_list(today.year, int(today_quarter(today.month)), quarters)

con_db = sqlite3.connect("./DATA/22.db")
for code in code_list:
    counter = 0;
    for year_quarter in y_q_list:
        year = int(year_quarter[0:4])
        quarter = int(year_quarter[-1])

        df_data_temp = data_collection_by_acc_id(code, item_list, item_sj_nm, item_account_id, year, quarter)
        if counter == 0:
            df_data = df_data_temp.copy()
            counter +=1
        else:
            df_data = pd.concat([df_data, df_data_temp], axis=1)
        if quarter==4:
            col = str(year) + '-4'
            # 손익계산서 4분기값은 연간총 합이므로, 1~3분기껄 빼줘야한다.
            corr_list = ['매출액', '매출총이익', '영업이익', '당기순이익', '영업활동현금흐름']
            try:
                for corr in corr_list:
                    if ((df_data[str(year)+'-1'].loc[corr]==None) | (df_data[str(year)+'-2'].loc[corr]==None) | (df_data[str(year)+'-3'].loc[corr]==None)):
                        df_data[col][corr] = None
                    else:
                        part_sum = int(df_data[str(year)+'-1'][corr]) + int(df_data[str(year)+'-2'][corr]) + int(df_data[str(year)+'-3'][corr]) # 1~3분기 합을 구한 후
                        df_data[col][corr] = str(int(df_data[col][corr]) - part_sum) # 원래 값에서 빼준다
            except:
                pass


    df_data.to_sql(code, con_db, if_exists="replace")
    print(code)

con_db.close()



