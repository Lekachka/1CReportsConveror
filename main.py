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

cwd = os.getcwd()

print(f'{datetime.now().strftime("%H:%M:%S")}: make dir')

tmp_folder = '../source_files/tmp/'
os.makedirs(tmp_folder, exist_ok=True)


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


for xlsx_file in os.listdir("../source_files/"):

    print(f'{datetime.now().strftime("%H:%M:%S")}: start with {xlsx_file}')
    base = os.path.splitext(os.path.basename(xlsx_file))[0]
    if xlsx_file.endswith(".xls"):
        df = pd.read_excel("../source_files/" + xlsx_file, header=None)
        df.to_excel("../result_files/" + base + '.xlsx', index=False, header=False)
    elif xlsx_file.endswith(".xlsx"):
        xslx_processing(xlsx_file)


print(f'{datetime.now().strftime("%H:%M:%S")}: delete tmp dir')

try:
    shutil.rmtree(tmp_folder)
except OSError as e:
    print("Error: %s - %s." % (e.filename, e.strerror))


print(f'{datetime.now().strftime("%H:%M:%S")}: done!')

input('Press ENTER to exit')
exit()
