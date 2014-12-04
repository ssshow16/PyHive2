import os
import fnmatch
import re
import datetime
import time
import random
import shutil

from pandas import DataFrame


def listFiles(root, pattern):
    result = []
    for base, dirs, files in os.walk(root):
        matchedFiles = fnmatch.filter(files, pattern)
        result.extend(os.path.join(base, f) for f in matchedFiles)
    return result


def unlink(path):
    shutil.rmtree(path)


def trim(x):
    return re.findall("^\s*(.*?)\s*$", str(x))[0]


def searchWithRegex(value, regex, group):
    matched = re.search(regex, value)
    return matched.group(group)


def replaceWithRegex(regex, value, org):
    if isinstance(org, basestring):
        return re.sub(regex, value, org)
    else:
        result = []
        for s in org:
            result.extend([re.sub(regex, value, s)])
        return result


def splitWithRegex(regex, value):
    return re.split(regex, value)


def convertDataFrame(dataFrameModel):
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


def randomKeyGen(size=5):
    ftime = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
    randomV = str(randomWithNDigits(size))

    return "_".join([ftime, randomV])


def randomWithNDigits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return random.randint(range_start, range_end)


def isduplicated(list):
    return len(list) != len(set(list))


def convertIntToBoolean(value):
    if value == 0:
        return False
    else:
        return True