import os
import fnmatch
import re

def listFiles(root, pattern):
    result = []
    for base, dirs, files in os.walk(root):
        matchedFiles = fnmatch.filter(files, pattern)
        result.extend(os.path.join(base, f) for f in matchedFiles)
    return result

def getColumnNames(rs):
    metaData = rs.getMetaData()
    colCnt = metaData.getColumnCount()

    return [metaData.getColumnName(i+1) for i in range(colCnt)]

def getColumnTypes(rs):
    metaData = rs.getMetaData()
    colCnt = metaData.getColumnCount()

    return [metaData.getColumnTypeName(i+1) for i in range(colCnt)]

def getColumnCount(rs):
    metaData = rs.getMetaData()
    return metaData.getColumnCount()

def getColumnValue(rs, idx, type):
    if type == 'string':
        return rs.getString(idx)
    elif type == 'int':
        return rs.getInt(idx)
    elif type == 'double':
        return rs.getDouble(idx)
    else:
        return rs.getString(idx)

def trim(x):
    return re.findall("^\s*(.*?)\s*$",str(x))[0]

def searchWithRegex(value,regex,group):
    matched = re.search(regex,value)
    return matched.group(group)