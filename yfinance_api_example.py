import yfinance as yf
dat = yf.Tickers("MSFT AAPL MSTR")
isin = "IE00BMFKG444"
print("Searching for", isin)
quote = list(yf.Search(isin).quotes)
final = [i for i in quote if not str(i['symbol']).startswith(isin)][0]['symbol']
print("Found",   final)
dat = yf.Ticker(final)
print(dat.info)
print(dat.history(period='1y'))
