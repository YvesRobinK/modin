import modin.pandas as pd
import os

import time

os.environ["MODIN_ENGINE"] = "dask"

time.sleep(5)
x = pd.read_csv('organizations-100.csv')
time.sleep(5)
print(x)



