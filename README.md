PyHive2
================

  PyHive2 is an Python package facilitating distributed computing via HIVE query.
  PyHive2 allows easy usage of HQL(Hive SQL) in Python, and allows easy usage of Python objects and Python functions in Hive.

## Install PyHive2
1. Requirements
    - ant (in order to build java files)
2. Installing PyHive2
    - Download source code: <code>git clone https://github.com/ssshow16/PyHive2.git</code>
    - Change your working directory: <code>cd PyHive2</code>
    - Set the environment variables HIVE_HOME and HADOOP_HOME:
        ```
        export HIVE_HOME=/path/to/your/hive/directory
        export HADOOP_HOME=/path/to/your/hadoop/directory
        ```
    - Build java files using ant: <code>ant build</code>
    - Build PyHive2: <code>python setup.py build</code>
    - Install PyHive2: <code>python setup.py install</code>

## Setting Environment Variable
PyHive2 use three Environment Variable like the following.
```
export HIVE_HOME=/path/to/your/hive/directory
export HADOOP_HOME=/path/to/your/hadoop/directory
export HADOOP_CONF_DIF=/path/to/your/hadoop/conf/directory
```

## Loading PyHive and connecting to Hive
1. launch python
```
> python
```

2. import PyHive2 and connect to hive
```
from PyHive2 import pyhive
conn = pyhive.connect()
```

## Tutorials
- [PyHive2 user guide](https://github.com/ssshow16/PyHive2/wiki/User-Guide)

## Requirements
- Java 1.6
- Python 2.6
- Hadoop 0.20.x (x >= 1)
- Hive 0.8.x (x >= 0)
