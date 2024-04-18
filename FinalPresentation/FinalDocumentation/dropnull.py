import pandas as pd

# load the 4 dirty csvs from yahoo finance into dataframes
thales = pd.read_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\dirtyThales_Stock.csv")
france = pd.read_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\dirtyFRA_Index.csv")
sp500 = pd.read_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\dirtySP500_INDEX.csv")
euro = pd.read_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\dirtyEURO_Index.csv")

# remove nulls from dirty dataframes
clean_thales = thales.dropna()
clean_france = france.dropna()
clean_sp500 = sp500.dropna()
clean_euro = euro.dropna()

# export clean dataframes to become clean csv"s without panda"s index column
clean_thales.to_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\clean_thales.csv", index=False)
clean_france.to_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\clean_france.csv", index=False)
clean_sp500.to_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\clean_sp500.csv", index=False)
clean_euro.to_csv("E:\\5-Data Analytics Winter 2024\\DBAS3090 - Applied Data Analytics\\Project\\clean_euro.csv", index=False)