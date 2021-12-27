"""
Program for converting xls and xlsx files
that comes from 1C program
"""

import os
import shutil
from zipfile import ZipFile
import time
from sys import exit
import argparse
import logging

import pandas as pd
from tabulate import tabulate
from dateutil.parser import parse

# Create a custom logger
logger = logging.getLogger(__name__)


def set_logger(loc_logger, log_level):

    logger = loc_logger
    logger.setLevel(logging.DEBUG)

    # Create handlers
    c_handler = logging.StreamHandler()
    c_handler.setLevel(log_level)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(message)s')
    c_handler.setFormatter(c_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)


def parce_args():
    parser = argparse.ArgumentParser(prog='1C reports parcer', description='Process xls and xlsx reports from 1C program.')
    parser.add_argument('-l', '--log_level',
                        nargs='?',
                        default='INFO',
                        dest='log_level',
                        choices=['ERROR', 'INFO', 'DEBUG', 'WARNING'],
                        help='Log levels: ERROR, INFO, DEBUG, WARNING. Default is INFO')
    args = parser.parse_args()
    return args

logger.info('make dir ../source_files/tmp/')

tmp_folder = '../source_files/tmp/'
os.makedirs(tmp_folder, exist_ok=True)

# Set of available currencies than used in 1C
currency_set = {"AUD", "BGN", "KRW", "HKD", "DKK", "USD", "PLN", "EUR",
                "JPY", "CAD", "HRK", "MXN", "NZD", "ILS", "NOK", "SGD",
                "ZAR", "RON", "HUF", "GBP", "CZK", "SEK", "CHF", "CNY",
                "XDR", "XAU", "XPD", "XPT", "XAG", 'RUB'
                }


def xlsx_processing(xlsx_file):
    base = os.path.splitext(os.path.basename(xlsx_file))[0]

    new_file_name = base + '.xlsx'

    # Распаковываем excel как zip в нашу временную папку
    logger.info(f'start with file {xlsx_file}')
    with ZipFile("../source_files/" + xlsx_file) as excel_container:
        logger.debug(f'start unpack file {xlsx_file}')
        excel_container.extractall(tmp_folder + '/' + base)
        logger.debug(f'finish unpack file {xlsx_file}')

    # Переименовываем файл с неверным названием
    wrong_file_path = os.path.join(tmp_folder, base, 'xl', 'SharedStrings.xml')
    correct_file_path = os.path.join(tmp_folder, base, 'xl', 'sharedStrings.xml')
    r = os.rename(wrong_file_path, correct_file_path)
    logger.debug(r)

    # delete mergeCell from sheet1.xml

    worksheet_folder = tmp_folder + '/' + base + "/xl/worksheets/"

    for xml_file in os.listdir(worksheet_folder):
        if xml_file.endswith(".xml"):
            logger.info(f'start with {xml_file}')

            with open(worksheet_folder + xml_file, "r", encoding='utf-8') as w:
                lines = w.readlines()
            with open(worksheet_folder + xml_file, "w", encoding='utf-8') as w:
                for line in lines:
                    if "mergeCell" not in line:
                        w.write(line)

    try:
        if os.path.exists("../result_files/" + new_file_name):
            os.remove("../result_files/" + new_file_name)
        # Запаковываем excel обратно в zip и переименовываем в исходный файл
        shutil.make_archive(base, 'zip', os.path.join(tmp_folder, base))
        os.rename(base + '.zip', "../result_files/" + new_file_name)
    except Exception as e:
        print("Error: %s." % e)

    dataframe_processing("../result_files/" + new_file_name, "../result_files/" + new_file_name)


def dataframe_processing(source_file, result_file):
    df = pd.read_excel(source_file, header=None)

    short_df = df.head(30).copy(deep=True)
    logger.debug(f'df.columns: {df.columns}')
    b = 0  # index for loop break: check for header, date and currencies columns
    header_raw = 0
    currencies_columns = set()
    my_tb_start = []
    for i in range(df.shape[0]):  # iterate over rows
        for j in range(df.shape[1]):  # iterate over columns
            value = df.at[i, j]  # get cell value

            if "Документ" in str(value) and header_raw == 0:
                header_raw = i
                b = b + 1
                logger.debug(f"header_raw: {header_raw}")
                if b == 3:
                    break

            if str(value) in currency_set and len(currencies_columns) < 2:
                currencies_columns.add(j)
                if len(currencies_columns) == 2:
                    b = b + 1
                    if b == 3:
                        break

            if is_date(str(value)) and len(my_tb_start) == 0:
                my_tb_start = [i, j]
                b = b + 1
                logger.debug(f"my_tb_start: {my_tb_start}")
                if b == 3:
                    break

        if b == 3:
            break

    data_df = df[my_tb_start[0]:].copy(
        deep=True)  # copy all columns form original data frame to data_df dataframe
    # starting with first row that contains date in cell
    data_df.columns = short_df.iloc[header_raw]  # add headers to data_df dataframe
    currencies_columns_list = list(currencies_columns)  # take a columns list from set
    currencies_columns_list.sort()
    logger.debug(f'currencies_columns_list: {currencies_columns_list}')
    data_df.columns.values[currencies_columns_list[0]] = 'Сума в грн дебет/Валюта дебет'
    data_df.columns.values[currencies_columns_list[0] + 1] = 'Сума у вал. дебет'
    data_df.columns.values[currencies_columns_list[1]] = 'Сума в грн кредит/Валюта кредит'
    data_df.columns.values[currencies_columns_list[1] + 1] = 'Сума у вал. кредит'
    data_df.columns.values[data_df.shape[1] - 1] = 'Сальдо в грн/Сальдо у валюті'

    data_df = data_df.reset_index(drop=True)
    data_df.drop(['Показник'], axis=1, inplace=True, errors='ignore')
    data_df.drop(['Показатель'], axis=1, inplace=True, errors='ignore')
    data_df.dropna(axis=1, how='all')
    data_df_even = data_df.iloc[::2]  # copy all even elements to new dataframe
    data_df_odd = data_df.iloc[1::2]  # copy all odd elements to new dataframe
    data_df_odd = data_df_odd.reset_index(drop=True)
    data_df_even = data_df_even.reset_index(drop=True)

    '''Create result dataframe on the base of even data'''
    data_df_even.insert(currencies_columns_list[0], "Валюта дебет",
                        data_df_odd['Сума в грн дебет/Валюта дебет'], True)
    data_df_even.insert(currencies_columns_list[0] + 1, "Сума у вал. дебет",
                        data_df_odd.iloc[:, data_df_odd.columns.get_loc('Сума в грн дебет/Валюта дебет') + 1],
                        True)

    data_df_even.insert(currencies_columns_list[1] + 2, "Валюта кредит",
                        data_df_odd['Сума в грн кредит/Валюта кредит'], True)
    data_df_even.insert(currencies_columns_list[1] + 3, "Сума у вал. кредит",
                        data_df_odd.iloc[:, data_df_odd.columns.get_loc('Сума в грн кредит/Валюта кредит') + 1],
                        True)

    data_df_even.insert(data_df_even.shape[1], "Сальдо у валюті",
                        data_df_odd.iloc[:, data_df_odd.shape[1] - 1], True)

    data_df_even.columns.values[data_df_even.shape[1] - 2] = 'Сальдо в грн'

    data_df_even.insert(0, "N", pd.DataFrame(1, index=range(data_df_even.shape[0]), columns=list('N'))['N'],
                        True)

    data_df_even.dropna(axis='columns', how='all', inplace=True)
    logger.debug(tabulate(data_df_even.head(10), tablefmt='psql'))
    data_df_even = data_df_even.rename(columns={'Сума в грн дебет/Валюта дебет': "Сума в грн дебет",
                                                'Сума в грн кредит/Валюта кредит': "Сума в грн кредит"})
    logger.debug(data_df_even.columns)
    logger.debug(tabulate(data_df_odd.head(10), tablefmt='psql'))
    logger.debug(data_df_odd.columns)

    rename_xlsx_file(result_file, data_df_even)


def xls_processing(xls_file):
    logger.info(f'start with file {xls_file}')
    dataframe_processing("../source_files/" + xls_file, "../result_files/" + base + '.xlsx')


def rename_xlsx_file(file_name, df):
    data_df_even = df
    try:
        data_df_even.to_excel(file_name, index=False, header=True)
    except Exception as e:
        logger.error(e)
        time.sleep(0.1)
        if input(f'Please, close file {file_name}\nAnd try again. Please, type [Y] for retry or any other for cancel:') == 'Y':
            rename_xlsx_file(file_name, data_df_even)
        else:
            logger.error(f'break with {file_name}')


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


def delete_tmp_folder(tmp_dir):
    logger.info(f'Delete tmp dir {tmp_dir}')

    try:
        shutil.rmtree(tmp_dir)
    except OSError as e:
        logger.error("Error: %s - %s." % (e.filename, e.strerror))

    logger.debug(f'Deleted tmp dir')


if __name__ == '__main__':

    args = parce_args()

    if args.log_level:
        log_level = args.log_level
    else:
        log_level = 'INFO'

    set_logger(logger, log_level)

    for xlsx_file in os.listdir("../source_files/"):
        base = os.path.splitext(os.path.basename(xlsx_file))[0]
        if xlsx_file.endswith(".xls"):
            xls_processing(xlsx_file)
        elif xlsx_file.endswith(".xlsx"):
            xlsx_processing(xlsx_file)

    delete_tmp_folder(tmp_folder)

    logger.info(f'Done!')

    time.sleep(0.1)
    input('Press ENTER to exit')
    exit()


