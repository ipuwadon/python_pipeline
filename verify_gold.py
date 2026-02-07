import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("data/year=2026/month=02/07022026103353.parquet")
print(df.head())
print(df.info())
print(len(df))

#df.plot(x='date', y=['close', 'ma_7d', 'ma_30d'], figsize=(12,6))
#plt.title("BTC Price va Moving Averages")
#plt.show()