"""
Utility for processing xls, xlsx, xlsb files
that comes from 1C program
"""

import os
import shutil
from zipfile import ZipFile
import time
from sys import exit
import argparse
import logging
import xlrd
import yaml
import numpy as np
import pandas as pd
from tabulate import tabulate
from dateutil.parser import parse


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s " \
             "(%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: yellow + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


# Create a custom logger
logger = logging.getLogger(__name__)

with open('config.yaml', 'rt', encoding='utf8') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

TMP_FOLDER = str(os.path.join(config['TMP_FOLDER']))
RESULT_FILES_FOLDER = str(os.path.join(config['RESULT_FILES_FOLDER']))
SOURCE_FILES_FOLDER = str(os.path.join(config['SOURCE_FILES_FOLDER']))
CONVERTED_FILES_FOLDER = str(os.path.join(config['CONVERTED_FILES_FOLDER']))
LOG_LEVEL = config['LOG_LEVEL']


def set_logger(loc_logger, logger_level):
    loc_logger.setLevel(logging.DEBUG)

    # Create handlers
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logger_level)
    c_handler.setFormatter(CustomFormatter())

    # Add handlers to the logger
    loc_logger.addHandler(c_handler)


def parse_args():
    parser = argparse.ArgumentParser(prog='1C reports parser',
                                     description='Process xls and xlsx reports '
                                                 'from 1C program.')
    parser.add_argument('-l', '--log_level',
                        nargs='?',
                        default=LOG_LEVEL,
                        dest='log_level',
                        choices=['ERROR', 'INFO', 'DEBUG', 'WARNING'],
                        help='Log levels: ERROR, INFO, DEBUG, WARNING. '
                             'Default is INFO')
    return parser.parse_args()


# Create tmp folder if not exists
os.makedirs(TMP_FOLDER, exist_ok=True)


def xlsx_processing(xlsx_file):
    # file name without extension
    base_file_name = os.path.splitext(os.path.basename(xlsx_file))[0]

    base_name_without_spaces = base_file_name.replace(' ', '_')

    # ?????????????????????????? excel ?????? zip ?? ???????? ?????????????????? ??????????
    logger.info(f'Start with file {xlsx_file}')
    with ZipFile(os.path.join(SOURCE_FILES_FOLDER, xlsx_file)) as exl_container:
        logger.debug(f'Start unpack file {xlsx_file}')
        exl_container.extractall(TMP_FOLDER + '/' + base_name_without_spaces)
        logger.debug(f'Finish unpack file {xlsx_file}')

    # ?????????????????????????????? ???????? ?? ???????????????? ??????????????????
    wrong_file_path = os.path.join(TMP_FOLDER, base_name_without_spaces,
                                   'xl',
                                   'SharedStrings.xml')
    logger.debug(f'Wrong_file_path: {os.path.abspath(wrong_file_path)}')
    correct_file_path = os.path.join(TMP_FOLDER, base_name_without_spaces,
                                     'xl',
                                     'sharedStrings.xml_')
    logger.debug(f'Correct_file_path: {os.path.abspath(correct_file_path)}')

    # strange way for file renaming
    if os.path.exists(os.path.abspath(wrong_file_path)):
        shutil.move(os.path.abspath(wrong_file_path),
                    os.path.abspath(correct_file_path))
        shutil.move(os.path.abspath(correct_file_path),
                    os.path.abspath(correct_file_path)[:-1])

    logger.info(f'Creating new zip and renaming to {TMP_FOLDER}/{xlsx_file}')
    try:
        if os.path.exists(os.path.join(TMP_FOLDER, xlsx_file)):
            os.remove(os.path.join(TMP_FOLDER, xlsx_file))
        # ???????????????????????? excel ?????????????? ?? zip ?? ?????????????????????????????? ?? ???????????????? ????????
        shutil.make_archive(os.path.join(TMP_FOLDER, base_file_name),
                            'zip',
                            os.path.join(TMP_FOLDER, base_name_without_spaces))
        os.rename(os.path.join(TMP_FOLDER, base_file_name + '.zip'),
                  os.path.join(TMP_FOLDER, xlsx_file))

    except Exception as e:
        print("Error: %s." % e)
        return False

    try:
        dataframe_processing(os.path.join(TMP_FOLDER, xlsx_file),
                             os.path.join(RESULT_FILES_FOLDER, xlsx_file))
    except Exception as e:
        logger.error(f'Error in data processing of {TMP_FOLDER}/{xlsx_file}.'
                     f'Error is {e}')
        return False

    return True


def dataframe_processing(source_file, result_file):

    ext = os.path.splitext(os.path.basename(xlsx_file))[1]
    result_file = result_file.replace(ext, '.xlsx')

    df = read_file_to_dataframe(source_file)
    logger.info(f'DataFrame processing of {source_file}')

    short_df = df.head(30).copy(deep=True)
    # short_df_tail = df.tail(30).copy(deep=True)
    logger.debug(f'df.columns: {df.columns}')
    b = 0  # index for loop break: check for header, date and currencies columns
    header_raw = 0
    my_tb_start = []

    logger.debug('Find dataframe header and date column')
    # find data frame structure
    df = df.reset_index(drop=True)

    logger.debug('df.head(10):\n')
    logger.debug(tabulate(df.head(10), tablefmt='psql'))

    my_tb_start_j = -1

    for i in range(short_df.shape[0]):  # iterate over rows
        for j in short_df.columns:
            value = short_df.loc[i, j]

            # find header row
            if config['HEADER_DETECTOR'] == str(value) and header_raw == 0:
                header_raw = i
                b = b + 1
                logger.debug(f"header_raw: {header_raw}")
                columns_list = short_df.iloc[header_raw]
                if my_tb_start_j == -1:
                    for ind, val in enumerate(columns_list):
                        if val in config['DATE_COLUMN_IN']:
                            my_tb_start_j = ind
                            break
                if b == 2:
                    break

            # try to convert cell value to date to check the start of data frame
            if is_date(str(value)) and len(my_tb_start) == 0 and my_tb_start_j != -1 and df.columns.get_loc(j) == my_tb_start_j:
                my_tb_start = [i, j]
                b = b + 1
                logger.debug(f"my_tb_start: {my_tb_start}")
                if b == 2:
                    break
            elif is_date(str(value)) and len(my_tb_start) == 0 and my_tb_start_j == -1:
                my_tb_start = [i, j]
                b = b + 1
                logger.debug(f"my_tb_start: {my_tb_start}")
                if b == 2:
                    break

        if b == 2:
            break

    col_one_list = df[my_tb_start[1]].tolist()[-10:]

    # iterate over rows from the end
    for i, elem in enumerate(col_one_list[::-1]):
        if is_date(str(elem)):
            if pd.isna(col_one_list[len(col_one_list) - i]):
                my_tb_end = df.shape[0] - i + 1
            else:
                my_tb_end = df.shape[0] - i
            logger.debug(f"my_tb_end: {my_tb_end}")
            break

    # copy all columns form original data frame to data_df dataframe
    # starting with first row that contains date in cell
    data_df = df[my_tb_start[0]:my_tb_end].copy(
        deep=True)

    # footer_df = df[my_tb_end:].copy(
    #     deep=True)

    del df

    data_df = data_df.reset_index(drop=True)

    logger.debug('Insert empty rows')
    # Insert empty rows if not exists
    list_not_empty_rows = data_df[
        data_df[my_tb_start[1]].notna()].index.tolist()
    list_empty_rows = data_df[data_df[my_tb_start[1]].isna()].index.tolist()
    list_empty_rows = [x - 1 for x in list_empty_rows]
    list_to_insert = list(set(list_not_empty_rows) - set(list_empty_rows))
    list_to_insert.sort()
    counter = 0
    for c, i in enumerate(list_to_insert):
        counter = counter + 1
        dfs = np.split(data_df, [i + counter])
        # creating the empty data with index
        empty_row = pd.DataFrame([], index=[i])
        data_df = pd.concat([dfs[0],
                             pd.DataFrame(empty_row,
                                          columns=data_df.columns), dfs[1]],
                            ignore_index=True)
        del dfs

    data_df = data_df.reset_index(drop=True)  # reset the index

    # add headers to data_df dataframe
    data_df.columns = short_df.iloc[header_raw]

    del short_df

    COLUMNS_TO_DELETE = config['COLUMNS_TO_DELETE']

    data_df.drop(COLUMNS_TO_DELETE, axis=1, inplace=True, errors='ignore')
    data_df.dropna(axis=1, how='all')

    data_df_even = data_df.iloc[::2]  # copy all even elements to new dataframe
    data_df_odd = data_df.iloc[1::2]  # copy all odd elements to new dataframe
    data_df_odd = data_df_odd.reset_index(drop=True)
    data_df_even = data_df_even.reset_index(drop=True)

    # convert empty to nan
    data_df_odd_ = data_df_odd.replace(r'^\s*$', np.NaN, regex=True)
    num_columns_list = []
    cur_columns_list = []

    df_filter = pd.DataFrame()

    for ind, column in enumerate(data_df_odd_.columns):
        try:
            df_filter = data_df_odd_.iloc[:-1, ind].dropna(how='all')
            data_df_odd_.iloc[:, ind] = data_df_odd_.iloc[:, ind].apply(
                pd.to_numeric)

            if len(df_filter.index) != 0:
                num_columns_list.append(ind)
        except Exception:
            if len(df_filter.index) != 0 and \
                    config['COLUMNS_NOT_CURRENCY'][
                        0] not in df_filter.values and \
                    config['COLUMNS_NOT_CURRENCY'][1] not in df_filter.values:
                cur_columns_list.append(ind)

    del data_df_odd_

    int_columns_list = []
    data_df_even_ = data_df_even.replace(r'^\s*$', np.NaN, regex=True)

    for ind, column in enumerate(data_df_even_.columns):
        try:
            df_filter = data_df_even_.iloc[:-1, ind].dropna(how='all')
            df_filter_int = df_filter.astype(int)

            if len(df_filter.index) != 0 and \
                    (df_filter.apply(pd.to_numeric) % 1 == 0).all():
                int_columns_list.append(ind)
        except Exception:
            pass

        if len(int_columns_list) == 2:
            break

    del data_df_even_

    int_columns_list.sort()

    if len(int_columns_list) < 2:
        logger.error('Cannot find orders columns')

    debet_column = int_columns_list[0]
    credit_column = int_columns_list[1]

    '''Create result dataframe on the base of even data'''
    credit_shift = 0

    if len(cur_columns_list) > 0:
        data_df_even.insert(debet_column + 2,
                            config['COLUMN_NAMES']['currency_deb'],
                            # "???????????? ??????????",
                            data_df_odd.iloc[:, cur_columns_list[0]],
                            True)
        credit_shift = credit_shift + 1

        data_df_even.insert(debet_column + 2 + credit_shift,
                            # "???????? ?? ??????. ??????????",
                            config['COLUMN_NAMES']['sum_currency_deb'],
                            data_df_odd.iloc[:, num_columns_list[0]],
                            True)

        credit_shift = credit_shift + 1

        data_df_even.insert(credit_column + 2 + credit_shift,
                            # "???????????? ????????????",
                            config['COLUMN_NAMES']['currency_credit'],
                            data_df_odd.iloc[:, cur_columns_list[1] if len(cur_columns_list) > 1 else cur_columns_list[0]],
                            True)

        credit_shift = credit_shift + 1

        data_df_even.insert(credit_column + 2 + credit_shift,
                            # "???????? ?? ??????. ????????????",
                            config['COLUMN_NAMES']['sum_currency_credit'],
                            data_df_odd.iloc[:, num_columns_list[1]],
                            True)
    elif len(num_columns_list) > 0:
        data_df_even.insert(debet_column + 2,
                            config['COLUMN_NAMES']['count'],
                            data_df_odd.iloc[:, num_columns_list[0]],
                            True)
        credit_shift = credit_shift + 1

        if len(num_columns_list) > 1:
            data_df_even.insert(credit_column + 2 + credit_shift,
                                # "???????? ?? ??????. ????????????",
                                config['COLUMN_NAMES']['count'],
                                data_df_odd.iloc[:, num_columns_list[1]],
                                True)

    # '???????? ?? ?????? ??????????'
    data_df_even.columns.values[debet_column + 1] = config['COLUMN_NAMES'][
        'sum_hrn_deb']
    # '???????? ?? ?????? ????????????'
    data_df_even.columns.values[credit_column + 1 + credit_shift] = config['COLUMN_NAMES']['sum_hrn_credit']

    data_df_even.insert(data_df_even.shape[1],
                        config['COLUMN_NAMES']['saldo_currency'],
                        # "???????????? ?? ????????????",
                        data_df_odd.iloc[:, data_df_odd.shape[1] - 1], True)

    data_df_even.columns.values[data_df_even.shape[1] - 2] = config['COLUMN_NAMES']['saldo_hrn']  # '???????????? ?? ??????'

    data_df_even.insert(0, "N",
                        pd.DataFrame(1, index=range(data_df_even.shape[0]),
                                     columns=list('N'))['N'],
                        True)

    data_df_even.dropna(axis='columns', how='all', inplace=True)

    logger.debug('data_df_even.head(10):')
    logger.debug(tabulate(data_df_even.head(10), tablefmt='psql'))
    logger.debug('data_df_even.columns:')
    logger.debug(data_df_even.columns)
    logger.debug('data_df_odd.head(10):')
    logger.debug(tabulate(data_df_odd.head(10), tablefmt='psql'))
    logger.debug('data_df_odd.dtypes:')
    logger.debug(data_df_odd.dtypes)
    logger.debug('data_df_odd.columns:')
    logger.debug(data_df_odd.columns)

    logger.debug(f'num_columns_list={num_columns_list}')
    logger.debug(f'cur_columns_list={cur_columns_list}')

    debet = config['COLUMN_NAMES']['debet']
    credit = config['COLUMN_NAMES']['credit']
    operation = config['COLUMN_NAMES']['operation']


    data_df_even.rename(
        columns={config['COLUMN_NAMES']['sum_hrn_deb']: 'sum_hrn_deb',
                 config['COLUMN_NAMES']['sum_hrn_credit']: 'sum_hrn_credit'},
        inplace=True)
    df_oper = pd.read_csv('categories.conf', sep=";", header=None,
                          names=[debet, credit, "Sign", operation]
                          )

    data_df_even['Sign'] = '+'
    #
    data_df_even[debet] = data_df_even[debet].astype(int)
    data_df_even[credit] = data_df_even[credit].astype(int)

    data_df_even['sum_hrn_deb'] = pd.to_numeric(data_df_even['sum_hrn_deb'],
                                                errors='coerce')
    data_df_even['sum_hrn_credit'] = pd.to_numeric(
        data_df_even['sum_hrn_credit'], errors='coerce')

    data_df_even['Sign'] = data_df_even['Sign'].astype(str)
    df_oper[debet] = df_oper[debet].astype(int)
    df_oper[credit] = df_oper[credit].astype(int)
    df_oper['Sign'] = df_oper['Sign'].astype(str)

    data_df_even.loc[data_df_even.sum_hrn_deb < 0, "Sign"] = "-"

    data_df_even = pd.merge(data_df_even,
                            df_oper,
                            how='left',
                            left_on=[debet, credit, "Sign"],
                            right_on=[debet, credit, "Sign"])

    data_df_even[operation] = np.where(
        ((data_df_even[debet] == 632) & (data_df_even[operation].isna())) | (
                (data_df_even[credit] == 632) &
                (data_df_even[operation].isna())), config['CARD_NOT_EXISTS'],
        data_df_even[operation])

    data_df_even.drop('Sign', axis=1, inplace=True, errors='ignore')

    data_df_even.rename(
        columns={'sum_hrn_deb': config['COLUMN_NAMES']['sum_hrn_deb'],
                 'sum_hrn_credit': config['COLUMN_NAMES']['sum_hrn_credit']},
        inplace=True)

    # Divide strings in columns by \n character

    for column in data_df_even:
        try:
            if data_df_even[column].dtypes == object and isinstance(data_df_even.iloc[1][column], str):
                new_df = data_df_even[column].str.split('\n', expand=True)
                if len(new_df.columns) > 1:
                    ind = data_df_even.columns.get_loc(column)
                    title = column
                    data_df_even.drop(column, axis=1, inplace=True, errors='ignore')
                    add_i = 0
                    for col in new_df:
                        data_df_even.insert(ind + add_i, title + '_' + str(add_i), new_df[col].values)
                        add_i += 1
        except ValueError:
            pass

    rename_xlsx_file(result_file, data_df_even)


def xls_processing(xls_file):
    base = os.path.splitext(os.path.basename(xlsx_file))[0]
    logger.info(f'start with file {xls_file}')
    dataframe_processing(os.path.join(SOURCE_FILES_FOLDER, xls_file),
                         os.path.join(RESULT_FILES_FOLDER,
                                      base + os.path.splitext(xls_file)[1]))
    return True


def read_file_to_dataframe(filename):

    ext = os.path.splitext(os.path.basename(filename))[1]

    logger.info('Reading dataframe. It takes a time. Please wait.')

    df = pd.DataFrame()

    if ext == '.xlsb':
        df = pd.read_excel(filename, engine='pyxlsb')
    else:
        first_type_successful = 0
        try:
            # works with strange old format of excel
            wb = xlrd.open_workbook(filename, encoding_override='cp1251')
            df = pd.read_excel(wb)
            first_type_successful = 1
        except xlrd.XLRDError:
            pass
        except Exception as e:
            logger.error(f'Exception type is: {e.__class__.__name__}. '
                         f'Error is {e}')

        if first_type_successful == 0:
            try:
                df = pd.read_excel(filename, header=None)
            except Exception as e:
                logger.error(f'Exception type is: {e.__class__.__name__}. '
                             f'Error is {e}')

    return df


def rename_xlsx_file(file_name, df):
    data_df_even = df
    try:
        data_df_even.to_excel(file_name, index=False, header=True)
        logger.info(f'Done with {file_name}')
    except Exception as e:
        logger.error(e)
        time.sleep(0.1)
        if input(f'Please, close file {file_name}\nAnd try again. Please, '
                 f'type [Y] for retry or any other for cancel:') == 'Y':
            rename_xlsx_file(file_name, data_df_even)
        else:
            logger.error(f'Break with {file_name}')


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
    """
    Clean folder recursively
    :param tmp_dir:
    :return:
    """
    logger.debug(f'Cleaning of tmp dir {tmp_dir}')

    for filename in os.listdir(tmp_dir):
        file_path = os.path.join(tmp_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logger.error('Failed to delete %s. Reason: %s' % (file_path, e))


def remove_source_file(file_to_remove):
    if config['MOVE_SOURCE'] is True:
        logger.info(
            f'Move {os.path.join(SOURCE_FILES_FOLDER, file_to_remove)} '
            f'to {os.path.join(CONVERTED_FILES_FOLDER, file_to_remove)}')

        shutil.move(os.path.join(SOURCE_FILES_FOLDER, file_to_remove),
                    os.path.join(CONVERTED_FILES_FOLDER, file_to_remove))


if __name__ == '__main__':
    args = parse_args()

    log_level = args.log_level

    set_logger(logger, log_level)

    for xlsx_file in os.listdir(SOURCE_FILES_FOLDER):
        #try:
        if xlsx_file.endswith(".xls"):
            if xls_processing(xlsx_file):
                remove_source_file(xlsx_file)
        elif xlsx_file.endswith(".xlsx") or xlsx_file.endswith(".xlsb"):
            if xlsx_processing(xlsx_file):
                remove_source_file(xlsx_file)
        #except Exception as ex:
            #logger.error(ex)


    delete_tmp_folder(TMP_FOLDER)

    logger.info('DONE!')

    time.sleep(0.1)
    print('\a')
    input('Press ENTER to exit')
    exit()
