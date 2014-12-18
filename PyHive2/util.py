import os
import fnmatch
import re
import datetime
import time
import random
import shutil

from pandas import DataFrame


def list_files(root, pattern):
    """ get file list matching regex pattern

    :param root: string
        root file path
    :param pattern: string
        file name regex pattern
    :return: list
        file list matching regex pattern
    """
    result = []
    for base, dirs, files in os.walk(root):
        matchedFiles = fnmatch.filter(files, pattern)
        result.extend(os.path.join(base, f) for f in matchedFiles)

    return result


def unlink(path):
    """remove file

    :param path: string
        local file system path
    """
    shutil.rmtree(path)


def trim(x):
    """ trim string

    :param x: string
    :return: string
        trimed string
    """
    return re.findall("^\s*(.*?)\s*$", str(x))[0]


def search_with_regex(value, regex, group):
    """ get group value matching regex

    :param value: string
    :param regex: string
    :param group: int
    :return: string
        group value matching regex
    """
    matched = re.search(regex, value)
    return matched.group(group)


def replace_with_regex(regex, value, org):
    """ replace string matching regex with replacement string

    :param regex: string
        regex string
    :param value: string
        the replacement sequence of string values.
    :param org: string
        the sequence of char values to be replaced.
    :return: string
        replaced string
    """
    if isinstance(org, basestring):
        return re.sub(regex, value, org)
    else:
        result = []
        for s in org:
            result.extend([re.sub(regex, value, s)])
        return result


def split_with_regex(regex, value):
    """ split with regex

    :param regex: string regex
    :param value:
        the sequence of char values to be splited.
    :return: list
        splited string list
    """
    return re.split(regex, value)


def convert_data_frame(dataFrameModel):
    """ convert data_frame_model into pandas.DataFrame

    :param dataFrameModel: data_frame_model
    :return: pandas.DataFrame
    """
    colNames = dataFrameModel.getColumnNames()
    colTypes = dataFrameModel.getColumnTypes()
    colCnt = dataFrameModel.getColumnCount()

    rows = []
    while dataFrameModel.next() == True:
        row = []
        for i in range(colCnt):

            if colTypes[i] == 'string':
                row.extend([dataFrameModel.getStringValue(i)])
            elif colTypes[i] == 'double':
                row.extend([dataFrameModel.getDoubleValue(i)])
            elif colTypes[i] == 'long':
                row.extend([dataFrameModel.getLongValue(i)])
            elif colTypes[i] == 'int' or colTypes[i] == 'bigint':
                row.extend([dataFrameModel.getIntValue(i)])
            else:
                row.extend([dataFrameModel.getStringValue(i)])

        rows.append(row)

    df = DataFrame(rows, columns=colNames)

    return df


def gen_random_key(size=5):
    """ make random key

    :param size: long
        the size of random key
    :return: string
        random key string
    """
    ftime = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
    randomV = str(gen_random_with_n_digits(size))

    return "_".join([ftime, randomV])


def gen_random_with_n_digits(n):
    """ make random N digit

    :param n: digit count
    :return: int
        N digit value
    """
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return random.randint(range_start, range_end)


def isduplicated(list):
    """ check where to contain duplicated value.

    :param list: list
    :return: boolean
        where to contain duplicated value.
    """
    return len(list) != len(set(list))


def convert_int_to_boolean(value):
    """ convert int type value into boolean value

    :param value: int
    :return: boolean
        if value is 0, return False.
        Otherwise, return True
    """
    if value == 0:
        return False
    else:
        return True