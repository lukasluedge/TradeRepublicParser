import pandas as pd
import yfinance as yf
from datetime import datetime, date, timedelta
import math
from time import sleep

#Fehlerquellen
    #nur Tag nicht minute des Kaufs bekannt 
    #manchmal sogar nicht der richtige Tag zum rechnen verwendet, weil die API nicht alle Werte hat

def getStockTicker(StockID:str) -> str:
    print("Searching for:", StockID)
    quote = list(yf.Search(StockID).quotes)
    final = [i for i in quote if not str(i['symbol']).startswith(StockID)][0]['symbol']
    print("Found:",   final)
    return final
    
def getStockName(StockID:str) -> str:
    print("Searching for:", StockID)
    quote = list(yf.Search(StockID).quotes)
    final = quote[0]['longname']
    print("Found:",   final)
    return final
    
def getISINtoTickerdict(StockIDs:list[str]) -> dict:
    ISINtoTicker = {}
    for id in StockIDs:
        ISINtoTicker[id] = getStockTicker(id)
    return ISINtoTicker

def getISINtoNamedict(StockIDs:list[str]) -> dict:
    ISINtoName = {}
    for id in StockIDs:
        ISINtoName[id] = getStockName(id)
    return ISINtoName

def getHistoricData(ticker:str, length:str) -> pd.DataFrame:
    data = yf.Ticker(ticker)
    df = data.history(period=length, auto_adjust=False)
    if df.empty:
        print("Failed to download:", ticker)
        return None
    return df


def extractTrades(df:pd.DataFrame) -> pd.DataFrame:

    df["Datum"] = pd.to_datetime(df["Datum"])
    #Portfolio transaktionen extrahieren
    df = df[df["Tag"].isin(["Handel", "Prämie"])]
    
    #Prämien Umwandeln
    df.loc[df["Tag"] == "Prämie", "Zahlungsbeteiligter"] = "Portfolio Transaction: IE00BMFKG444"
    df["Zahlungsbeteiligter"] = df["Zahlungsbeteiligter"].str.replace("Portfolio Transaction: ", "")
    
    #nur wichtige Spalten weiter nehmen
    df = df[["Datum", "Zahlungsbeteiligter", "Betrag"]]
    
    return df


def getYearsFromStart(start_day, satrt_month, start_year) -> int:
    start_date = date(start_year, satrt_month, start_day)
    today = date.today()
    
    # Berechne die Anzahl der Tage zwischen den beiden Daten
    delta_days = (today - start_date).days
    
    # Berechne die Anzahl der Jahre und runde auf
    years = math.ceil(delta_days / 365)
    
    return years


def downloadStockData(stockTickers:dict, outputFolder:str, failedFile:str = "failed.txt") -> None:
    failedStocks = []
    numYears = getYearsFromStart(1, 1, 2021)
    
    for ticker in stockTickers.items():
        ticker = ticker[1]
        stockDF = getHistoricData(ticker, f"{numYears}y")
        if stockDF is not None:
            stockDF.to_csv(str(outputFolder) + str(ticker) + ".csv", index=True)
            print("Successfully downloaded:", ticker)
        else:
            failedStocks.append(ticker)
            
    with open(failedFile, 'w') as file:
        for i in failedStocks:
            file.write(i + "\n")

def getStockDf(ticker:str, downloadFolder:str) -> pd.DataFrame:
    stockData = pd.read_csv(downloadFolder + ticker + ".csv")
    stockData["Date"] = pd.to_datetime(stockData["Date"].str[:10])
    stockData = stockData.set_index("Date")
    
    return stockData

def getRollingQuantity(downloadFolder:str, pivot_df:pd.DataFrame, ISINtoTicker:dict) -> pd.DataFrame:
    for stock in pivot_df.columns:
        stockData = getStockDf(ISINtoTicker[stock], downloadFolder)
        
        for date, row in pivot_df.iterrows():
            stockDate = date
        
            if row[stock] != 0:
                offset = 1
                while stockDate not in stockData.index:
                    # Finde das nächste Datum, das kleiner oder gleich dem gesuchten Datum ist
                    stockDate = (stockDate - timedelta(days=offset)) if offset % 2 == 0 else (stockDate + timedelta(days=offset))
                    offset += 1

                    
                spentMoney = row[stock] * -1
                stockPrice = ( stockData.at[stockDate, "Low"] + stockData.at[stockDate, "High"]) / 2
                numShares = spentMoney / stockPrice
                pivot_df.at[date, stock] = numShares
    
    #fehlende Daten auffüllen
    all_dates = pd.date_range(start=pivot_df.index.min(), end=date.today(), freq='D')
    pivot_df = pivot_df.reindex(all_dates, fill_value=0)  
    
    #rolling sum bilden              
    rollingSumDF = pivot_df.rolling(window=len(pivot_df), min_periods=1).sum()
    
    return rollingSumDF

def resolveStockPrice(df:pd.DataFrame, downloadFolder:str, ISINtoTicker:dict) -> pd.DataFrame:
    #laden aller stockDFs
    stockDataDict = {}
    for stock in df.columns:
        stockDataDict[stock] = getStockDf(ISINtoTicker[stock], downloadFolder)
    for row_index, (date, row) in enumerate(df.iterrows()):
        for stock in df.columns:
            if row[stock] != 0:
                stockData = stockDataDict[stock]
                stockDate = date
                offset = 1
                while stockDate not in stockData.index:
                    # Finde das nächste Datum, das kleiner oder gleich dem gesuchten Datum ist
                    stockDate = (stockDate - timedelta(days=offset)) if offset % 2 == 0 else (stockDate + timedelta(days=offset))
                    offset += 1

                stockPrice = ( stockData.at[stockDate, "Low"] + stockData.at[stockDate, "High"]) / 2
                amount = df.at[date, stock]
                df.at[date, stock] = stockPrice * amount
                
        print(f"resolvingStockPrice progress: {row_index/len(df):.2%}", end="\r")
                
    return df
    
    

def main(updateTrades:bool = False, stockDataPull:bool = False, calculatePortfolie:bool = False):
    #!! Ausführen aus TradeRepublicParser/ 
    globalTR        = "..\\..\\Non_pipeline_files\\globalTR.csv"
    TradeMatrix     = "..\\..\\Output_Data\\tradeMatrix.csv"
    quantatyMatrix  = "..\\..\\Output_Data\\quantityMatrix.csv"
    downloadFolder  = "..\\..\\Downloaded_Data\\Kursdaten\\"
    
    
    
    if updateTrades:
        df = pd.read_csv(globalTR)
        df["Datum"] = pd.to_datetime(df["Datum"])
        
        df = extractTrades(df)
    
        pivot_df = df.pivot_table(index="Datum", columns="Zahlungsbeteiligter", values="Betrag", aggfunc="sum", fill_value=0)
        pivot_df.to_csv(TradeMatrix, index=True)
    else:
        
        pivot_df = pd.read_csv(TradeMatrix)
        pivot_df["Datum"] = pd.to_datetime(pivot_df["Datum"])
        pivot_df = pivot_df.set_index("Datum")
    
    ISINs = list(pivot_df.columns)
    ISINtoTicker = getISINtoTickerdict(ISINs)
    ISINtoName = getISINtoNamedict(ISINs)

    
    if stockDataPull:
        downloadStockData(ISINtoTicker, downloadFolder)
        
    
        
    rollingSumDF = getRollingQuantity(downloadFolder, pivot_df, ISINtoTicker)
    rollingSumDF = resolveStockPrice(rollingSumDF, downloadFolder, ISINtoTicker)
    rollingSumDF = rollingSumDF.rename(columns=ISINtoName)
    rollingSumDF.to_csv(quantatyMatrix, index=True)
    print(rollingSumDF)

        
        
        
        
        
        

    
            
    

        
    
    
            
            
            
if __name__ == "__main__":
    main(stockDataPull=False)
    