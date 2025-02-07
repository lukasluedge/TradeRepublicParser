import pandas as pd
import yfinance as yf
from datetime import datetime, date
import math

outputFolder = r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\Kursdaten\\"

def getStockTicker(StockID:str) -> str:
    print("Searching for:", StockID)
    quote = list(yf.Search(StockID).quotes)
    final = [i for i in quote if not str(i['symbol']).startswith(StockID)][0]['symbol']
    print("Found:",   final)
    return final
    

def getHistoricData(ticker:str, length:str) -> pd.DataFrame:
    print("Ticker:", ticker)
    data = yf.Ticker(ticker)
    df = data.history(period=length)
    if df.empty:
        print("Fehler beim Downloaden der Daten")
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

    
    
if __name__ == "__main__":
    globalTR = "C:\\Tickets\\Finanzen\\new_setup\\Non_pipeline_files\\globalTR.csv"
    TradeMatrix = "C:\\Tickets\\Finanzen\\new_setup\\Output_Data\\tradeMatrix.csv"
    failedFile = "C:\\Tickets\\Finanzen\\new_setup\\Code\\TradeRepublicParser\\failed.txt"
    
    
    
    df = pd.read_csv(globalTR)
    df["Datum"] = pd.to_datetime(df["Datum"])
    
    df = extractTrades(df)
   
    pivot_df = df.pivot_table(index="Datum", columns="Zahlungsbeteiligter", values="Betrag", aggfunc="sum", fill_value=0)
    

    # pivot_df.to_csv(TradeMatrix, index=True)
    
    StockIDs = pivot_df.columns

    
    failedStocks = []
    numYears =getYearsFromStart(1, 1, 2021)
    for id in StockIDs:
        ticker = getStockTicker(id)
        stockDF = getHistoricData(ticker, f"{numYears}y")
        if stockDF is not None:
            stockDF.to_csv(str(outputFolder) + str(ticker) + ".csv", index=True)
            print("Successfully downloaded:", ticker)
        else:
            failedStocks.append(ticker)

        
    
    
    with open(failedFile, 'w') as file:
        for i in failedStocks:
            file.write(i + "\n")
    