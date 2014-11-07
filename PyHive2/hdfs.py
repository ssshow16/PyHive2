__author__ = 'bruceshin'

import j2p
import numpy as np
import pandas as pd

def dfsDiskUsage(connection, path="/"):
    fsu = j2p.FileSystemUtils()
    du = fsu.du(path, connection.getFsDefault(),"bruceshin")

    return jMapToDataFrame(du)

def jMapToDataFrame(map):
    keys = map.keySet()

    dict = {}

    for key in keys:
        valus = map.get(key)
        dict[key] = np.array(valus)

    df = pd.DataFrame(dict)
    return df
