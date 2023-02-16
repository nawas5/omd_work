import os
import re
import pandas as pd
from datetime import datetime, timedelta
# from sqlalchemy import create_engine

# Подключение к базе данных
# engine = create_engine('postgresql://olv_master:xSxuQ{pC\_a6:S#p@host:5432/olv_master_base')

# Получение списка файлов Excel из папки
folder_path = "C:/Users/stoma/OneDrive/Документы/GitHub/omd_work/mvp_digital/data_new_2/"
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Создание пустого DataFrame для хранения данных
all_data = pd.DataFrame()

# Переименуем столбцы - столбец id возьмем как primary key
new_columns = ['adv_list',
               'art3_list',
               'ad_placement',
               'use_type',
               'subbr_list',
               'day',
               'ad_network',
               'br_list',
               'mod_list',
               'media_product',
               'ad_source_type',
               'art2_list',
               'id',
               'ad_player',
               'media_resource',
               'ad_server',
               'media_holding',
               'first_issue_date',
               'art4_list',
               'ad_video_utility',
               'ots'
               ]

old_columns = ["advertiserListNames",
               "productCategoryL3ListNames",
               "adPlacementName",
               "useTypeName",
               "productSubbrandListNames",
               "researchDate",
               "adNetworkName",
               "productBrandListNames",
               "productModelListNames",
               "crossMediaProductName",
               "adSourceTypeName",
               "productCategoryL2ListNames",
               "adMonitoringId",
               "adPlayerName",
               "crossMediaResourceName",
               "adServerName",
               "crossMediaHoldingName",
               "firstIssueDate",
               "productCategoryL4ListNames",
               "adVideoUtilityName",
               "stat.ots"
               ]

def clear_list(column):
    translation = str.maketrans("", "", "[]''")
    cleaned_string = column.translate(translation)
    return cleaned_string

def create_date(file_name):
    date_str = re.sub('[a-zA-Z.]', '', file_name)
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    week_obj = date_obj - timedelta(days=date_obj.weekday())  # начало недели
    week_str = week_obj.strftime('%Y-%m-%d')  # неделя в формате 'yyyy-mm-dd'
    month_str = date_obj.replace(day=1).strftime('%Y-%m-%d')  # месяц в формате 'yyyy-mm-dd'
    return week_str, month_str
def columns_rename(data):
    list_columns = list(filter(lambda x: 'List' in x, data.columns))
    data[list_columns] = data[list_columns].applymap(clear_list)
    data = data.rename(columns={old_column: new_column for old_column, new_column in zip(old_columns, new_columns)})
    return data


# Цикл для загрузки данных из всех файлов
for file in csv_files:
    # Полный путь к файлу
    file_path = os.path.join(folder_path, file)
    # Загрузка данных из файла Excel в DataFrame
    data = pd.read_csv(file_path, sep=';', encoding='cp1251')
    # Добавление новых данных в DataFrame
    data[['week', 'month']] = create_date(file)
    all_data = pd.concat([all_data, data])

result_data = columns_rename(all_data)
# Загрузка данных в базу данных с установкой столбца 'id' в качестве первичного ключа (primary key)
result_data.to_sql('mvp_digital_rus', con=engine, if_exists='replace', index=True, index_label='id')

# Закрытие соединения с базой данных
engine.dispose()