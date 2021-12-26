'''
Program for converting xls and xlsx files
that comes from 1C program
'''

import os
import shutil
from zipfile import ZipFile
from datetime import datetime
from sys import exit

import pandas as pd
from tabulate import tabulate
from dateutil.parser import parse

cwd = os.getcwd()

print(f'{datetime.now().strftime("%H:%M:%S")}: make dir')

tmp_folder = '../source_files/tmp/'
os.makedirs(tmp_folder, exist_ok=True)

# Set of available currencies than used in 1C
currency_set = {"AUD", "BGN", "KRW", "HKD", "DKK", "USD", "PLN", "EUR",
                "JPY", "CAD", "HRK", "MXN", "NZD", "ILS", "NOK", "SGD",
                "ZAR", "RON", "HUF", "GBP", "CZK", "SEK", "CHF", "CNY",
                "XDR", "XAU", "XPD", "XPT", "XAG", 'RUB'
                }


def xslx_processing(xlsx_file):
    base = os.path.splitext(os.path.basename(xlsx_file))[0]

    new_file_name = base + '.xlsx'

    # Распаковываем excel как zip в нашу временную папку
    with ZipFile("../source_files/" + xlsx_file) as excel_container:
        excel_container.extractall(tmp_folder)

    # Переименовываем файл с неверным названием
    wrong_file_path = os.path.join(tmp_folder, 'xl', 'SharedStrings.xml')
    correct_file_path = os.path.join(tmp_folder, 'xl', 'sharedStrings.xml')
    os.rename(wrong_file_path, correct_file_path)

    # delete mergeCell from sheet1.xml

    worksheet_folder = tmp_folder + "/xl/worksheets/"

    for xml_file in os.listdir(worksheet_folder):
        if xml_file.endswith(".xml"):

            print(f'{datetime.now().strftime("%H:%M:%S")}: start with {xml_file}')

            with open(worksheet_folder + xml_file, "r", encoding='utf-8') as w:
                lines = w.readlines()
            with open(worksheet_folder + xml_file, "w", encoding='utf-8') as w:
                for line in lines:
                    if "mergeCell" not in line:
                        w.write(line)

    # Удаляем файл с таким же имененм в папке с результатами
    if os.path.exists("../result_files/" + new_file_name):
        os.remove("../result_files/" + new_file_name)
    # Запаковываем excel обратно в zip и переименовываем в исходный файл
    shutil.make_archive(base, 'zip', tmp_folder)
    os.rename(base + '.zip', "../result_files/" + new_file_name)


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


for xlsx_file in os.listdir("../source_files/"):

    print(f'{datetime.now().strftime("%H:%M:%S")}: start with {xlsx_file}')
    base = os.path.splitext(os.path.basename(xlsx_file))[0]
    if xlsx_file.endswith(".xls"):
        df = pd.read_excel("../source_files/" + xlsx_file, header=None)

        short_df = df.head(30).copy(deep=True)
        print(df.columns)
        print(tabulate(short_df, tablefmt='psql'))
        b = 0  # index for loop break: check for header, date and currencies columns
        header_raw = 0
        currencies_columns = set()
        currencies_values_columns = set()
        my_tb_start = []
        for i in range(short_df.shape[0]):  # iterate over rows
            for j in range(df.shape[1]):  # iterate over columns
                value = short_df.at[i, j]  # get cell value

                if "Документ" in str(value):
                    if header_raw == 0:
                        header_raw = i
                        b = b + 1
                        print(f"header_raw: {header_raw}")
                        if b == 3:
                            break
                if str(value) in currency_set:
                    currencies_columns.add(j)
                    currencies_values_columns.add(j+1)
                    if len(currencies_columns) >= 2:
                        b = b + 1
                        print(f"currencies_columns: {currencies_columns}")
                        print(f"currencies_values_columns: {currencies_values_columns}")
                        if b == 3:
                            break
                if is_date(str(value)):
                    if len(my_tb_start) == 0:
                        my_tb_start = [i, j]
                        b = b + 1
                        print(f"my_tb_start: {my_tb_start}")
                        if b == 3:
                            break

            if b == 3:
                break

        data_df = df[my_tb_start[0]:].copy(deep=True)   # copy data to data_df dataframe from first date in columns
        data_df.columns = short_df.iloc[header_raw]     # add headers to data_df dataframe
        currencies_columns_list = list(currencies_columns)  # take a list from set
        currencies_columns_list.sort()
        print(currencies_columns_list)
        data_df.columns.values[currencies_columns_list[0]] = 'Сума в грн дебет'
        data_df.columns.values[currencies_columns_list[1]] = 'Сума в грн кредит'
        #df.iloc[:, 0]
        data_df = data_df.reset_index(drop=True)
        data_df.drop(['Показник'], axis=1, inplace=True)
        data_df_even = data_df.iloc[::2]
        print(data_df_even.columns)
        data_df_odd = data_df.iloc[1::2]
        data_df_odd = data_df_odd.reset_index(drop=True)
        data_df_even = data_df_even.reset_index(drop=True)

        data_df_even.insert(currencies_columns_list[0], "Валюта дебет", data_df_odd['Сума в грн дебет'], True)
        data_df_even.insert(currencies_columns_list[0] + 1, "Сума у вал. дебет",
                            data_df_odd.iloc[:, data_df_odd.columns.get_loc('Сума в грн дебет') + 1], True)

        data_df_even.insert(currencies_columns_list[1] + 2, "Валюта кредит",
                            data_df_odd['Сума в грн кредит'], True)
        data_df_even.insert(currencies_columns_list[1] + 3, "Сума у вал. кредит",
                            data_df_odd.iloc[:, data_df_odd.columns.get_loc('Сума в грн кредит') + 1], True)

        data_df_even.insert(data_df_even.shape[1], "Сальдо у валюті",
                            data_df_odd.iloc[:, data_df_odd.shape[1] - 1], True)

        data_df_even.columns.values[data_df_even.shape[1] - 2] = 'Сальдо в грн'

        data_df_even.dropna(axis='columns', how='all', inplace=True)
        print(tabulate(data_df_even.head(30), tablefmt='psql'))
        print(tabulate(data_df_odd.head(30), tablefmt='psql'))
        print(data_df_odd.columns)
        data_df_even.to_excel("../result_files/" + base + '.xlsx', index=False, header=True)
    elif xlsx_file.endswith(".xlsx"):
        xslx_processing(xlsx_file)

print(f'{datetime.now().strftime("%H:%M:%S")}: delete tmp dir')

# try:
#    shutil.rmtree(tmp_folder)
# except OSError as e:
#    print("Error: %s - %s." % (e.filename, e.strerror))


print(f'{datetime.now().strftime("%H:%M:%S")}: done!')

input('Press ENTER to exit')
exit()
