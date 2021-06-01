from pprint import pprint

import httplib2
import apiclient  # .discovery
from oauth2client.service_account import ServiceAccountCredentials

# Файл, полученный в Google Developer Console
CREDENTIALS_FILE = 'creds.json'
# ID Google Sheets документа (можно взять из его URL)
spreadsheet_id = '1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ'

# Авторизуемся и получаем service — экземпляр доступа к API
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

# Получаем список листов, их Id и название
spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
sheetList = spreadsheet.get('sheets')
for sheet in sheetList:
    print(sheet['properties']['sheetId'], sheet['properties']['title'])

sheetId = sheetList[0]['properties']['sheetId']

import pandas as pd
import numpy as np

# Получаем данные из таблиц
# Наш произвольный перод с 2020-11-23 12:49:02 по 2020-11-28 14:51:36
ranges_transactions = ["transactions!A1:D29001"] #
results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheet_id,
                                     ranges = ranges_transactions,
                                     valueRenderOption = 'FORMATTED_VALUE',
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute()

sheet_values_transactions = results['valueRanges'][0]['values']

data_transactions = pd.DataFrame(sheet_values_transactions[1:])
data_transactions.columns = sheet_values_transactions[0]
data_transactions['created_at'] = pd.to_datetime(data_transactions['created_at'])

ranges_clients = ["clients!A1:c75767"] #
results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheet_id,
                                     ranges = ranges_clients,
                                     valueRenderOption = 'FORMATTED_VALUE',
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute()

sheet_values_clients = results['valueRanges'][0]['values']
data_clients = pd.DataFrame(sheet_values_clients[1:])
data_clients.columns = sheet_values_clients[0]

ranges_managers = ["managers!A1:C14"] #
results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheet_id,
                                     ranges = ranges_managers,
                                     valueRenderOption = 'FORMATTED_VALUE',
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute()
sheet_values_managers = results['valueRanges'][0]['values']
data_managers = pd.DataFrame(sheet_values_managers[1:])
data_managers.columns = sheet_values_managers[0]

ranges_leads = ["leads!A1:F3338"] #
results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheet_id,
                                     ranges = ranges_leads,
                                     valueRenderOption = 'FORMATTED_VALUE',
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute()
sheet_values_leads = results['valueRanges'][0]['values']

data_leads = pd.DataFrame(sheet_values_leads[1:])
data_leads.columns = sheet_values_leads[0]
data_leads['created_at'] = pd.to_datetime(data_leads['created_at'])
data_leads = data_leads.sort_values('created_at')
# Наш произвольный перод с 2020-11-23 12:49:02 по 2020-11-28 14:51:36
short_data_leads = data_leads[3000:]

# количество заявок

#data_leads.groupby('d_utm_source')['l_client_id'].count()
short_data_leads['d_utm_source'].value_counts()

# количество мусорных заявок (на основании заявки не создан клиент)
short_data_leads[
    short_data_leads['l_client_id'] == '00000000-0000-0000-0000-000000000000'
]['d_utm_source'].value_counts()



# количество заявок
# Уникальные пользователи за наш период
data_lead_unique = short_data_leads[
    short_data_leads.l_client_id.isin(
        short_data_leads['l_client_id'].unique()
    )
]
# Проверяем, нет ли этих уникальных пользователей в других датах
new_users = data_lead_unique[
    ~short_data_leads.l_client_id.isin(
    data_leads[:3000]['l_client_id'].unique()
    )
]
# Обращаемся к таблице transactions, проверяем, нет ли этих уникальных пользователей в других датах
new_users = new_users[
    ~new_users.l_client_id.isin(
        data_transactions[
            data_transactions['created_at'] < np.datetime64('2020-11-23 12:53:10') # Дата с которой мы начинаем отчет
        ]['l_client_id'].unique()
    )
]

# количество новых заявок (не было заявок и покупок от этого клиента раньше)

new_users['d_utm_source'].value_counts()

# количество покупателей (кто купил в течение недели после заявки)
time_range = data_lead_unique.merge(

    data_transactions['l_client_id']

).sort_values(by=['l_client_id'])['created_at'] - data_transactions.merge(

    data_lead_unique['l_client_id']

).sort_values(by=['l_client_id'])['created_at']


buyers = data_lead_unique.merge(

    data_transactions['l_client_id']

).sort_values(by=['l_client_id'])[time_range < pd.Timedelta("7 days")]

buyers['d_utm_source'].value_counts()

# Количество новых покупателей (кто купил в течение недели после заявки, и не покупал раньше)
new_buyers = buyers[
    ~buyers.l_client_id.isin(
        data_transactions[
            data_transactions['created_at'] < np.datetime64('2020-11-23 12:53:10') # Дата с которой мы начинаем отчет
        ]['l_client_id'].unique()
    )
]
new_buyers['d_utm_source'].value_counts()

def to_data_group_by(no_preprocessed_data):
    data = no_preprocessed_data['d_utm_source'].value_counts()
    return list(np.where(np.array(data.index) == '', 'No data', np.array(data.index)).astype('str')),list(np.array(data.values).astype('str'))

# Запись в файл

# ID Google Sheets документа (можно взять из его URL)
spreadsheet_id = '1mjbal25K18rctAv5NBrXgc01U6NUYLeZONSy2rI1FmE'

# Авторизуемся и получаем service — экземпляр доступа к API
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

# # Пример записи в файл
values = service.spreadsheets().values().batchUpdate(
    spreadsheetId=spreadsheet_id,
    body={
        "valueInputOption": "USER_ENTERED",
        "data": [

            {"range": "A1:B1",
             "majorDimension": "ROWS",
             "values": [["Dimension: канал привлечения заявки"]]},
            {"range": "A3:B3",
             "majorDimension": "ROWS",
             "values": [["Количество заявок"]]},

            {"range": "A4:B12",
             "majorDimension": "COLUMNS",
             "values": to_data_group_by(short_data_leads)},

            {"range": "C3:D3",
             "majorDimension": "ROWS",
             "values": [["Количество мусорных заявок"]]},

            {"range": "C4:D11",
             "majorDimension": "COLUMNS",
             "values": to_data_group_by(
                 short_data_leads[short_data_leads['l_client_id'] == '00000000-0000-0000-0000-000000000000'])},

            {"range": "E3:F3",
             "majorDimension": "ROWS",
             "values": [["Количество новых заявок"]]},

            {"range": "E4:F12",
             "majorDimension": "COLUMNS",
             "values": to_data_group_by(new_users)},

            {"range": "G3:H3",
             "majorDimension": "ROWS",
             "values": [["Количество покупателей"]]},

            {"range": "G4:H9",
             "majorDimension": "COLUMNS",
             "values": to_data_group_by(buyers)},

            {"range": "I3:J3",
             "majorDimension": "ROWS",
             "values": [["Количество новых покупателей"]]},

            {"range": "I4:J7",
             "majorDimension": "COLUMNS",
             "values": to_data_group_by(new_buyers)}
        ]
    }
).execute()