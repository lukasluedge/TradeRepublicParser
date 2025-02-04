import fitz  # PyMuPDF
import pandas as pd

pdf_path = r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\Kontoauszug_neu.pdf"  # Pfad zu deinem PDF
output_csv = r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\kontoauszug.csv"  # Ziel-CSV-Datei
output_excel =  r"C:\Tickets\Finanzen\new_setup\Input_Data\TR_Daten.xlsx"
debugFile =  r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\debug.csv"
globalFile = r"C:\Tickets\Finanzen\new_setup\Non_pipeline_files\globalTR.csv"

DEBUG = True

Tags = ["Kartentransaktion", "Überweisung", "SEPA", "Handel", "Prämie", "Zinszahlung", "Erträge", "Empfehlung", "Gebühren", "Steuern", "Kapitalmaßnahme"]
TagsBase = ["Kartentransaktion", "Überweisung", "SEPA"]
TagsBaseGlobal = ["Prämie", "Zinszahlung", "Erträge", "Empfehlung", "Gebühren", "Steuern", "Kapitalmaßnahme"]


def getPdfData(path:str) -> list[list[str]]:
    data = [[""]]
    with fitz.open(pdf_path) as pdf:
        for page in pdf:
            data.append(["***************"])
            text = page.get_text("text")  # Gesamten Text extrahieren
            lines = text.split("\n")  # Zeilenweise aufsplitten
            for line in lines:
                data.append([line])  # Jede Zeile als eigene Liste speichern
    return data

def findBetragPos(data:list[list[str]], start) -> int:
    element = ""
    while r"€" not in element:
        if start > 0:
            start += 1
            element = str(data[start][0])
        else:
            break
           # print(element)
    return start

def getDate(data:list[list[str]], pos:int) -> str:
    Datum = ""
    if len(data[pos-2][0].split()) == 2:
        Datum = data[pos-2][0] + data[pos-1][0]
    else:
        Datum = data[pos-3][0] + data[pos-2][0] + data[pos-1][0]
        
    return Datum.replace(".","").replace(" ", "-")

def formatDate(date:str) -> str:
    day = date[:2]
    month = date[3:-5].lower()
    year = date[-4:]
    monthDict = {
        "jan": "01",
        "feb": "02",
        "märz": "03",
        "apr": "04",
        "mai": "05",
        "juni": "06",
        "juli": "07",
        "aug": "08",
        "sept": "09",
        "okt": "10",
        "nov": "11",
        "dez": "12",
    }

    return year + "-" + monthDict[month] + "-" + day

def parseBaseCase(data:list[list[str]], pos:int) -> tuple[str,str]:
    saldo = float(data[findBetragPos(data, pos)+1][0].replace(".", "").replace(",", ".").replace("€", ""))
    #find last saldo (in the future because we are in reverse chronological order)
    probe = 0
    element = "test"
    end = False
    while element not in Tags:
        probe += 1
        element = data[pos - probe][0].strip()
        if pos-probe <= 0:
            lastSaldo1 = 0
            end = True
            break
    if not end:
        lastSaldo1 = float(data[findBetragPos(data, pos - probe)+1][0].replace(".", "").replace(",", ".").replace("€", ""))
    diff = saldo - lastSaldo1  #we are traversing the file in reverse chronological order
    sign = ""
    if diff > 0:
        sign = ""
    elif diff < 0:
        sign = "-"
    else:
        print("*********ERROR 0€ Transaction detected", probe, data[pos + probe][0].strip(), findBetragPos(data, pos - probe), pos)
        
    BetragPos = findBetragPos(data, pos)
    Betrag = sign + data[BetragPos][0].replace(r"€", "")
    Zahlungsbeteiligter = "".join([e[0] for e in data[pos+1:BetragPos]])
    
    return Zahlungsbeteiligter, Betrag
        



def parseTrade(data:list[list[str]], pos:int) -> tuple[str,str]:
    Zahlungsbeteiligter = "Portfolio Transaction: " + data[pos + 1][0].split()[4]
    direction = data[pos + 1][0].split()[3]
    
    Betrag = data[findBetragPos(data, pos)][0].replace("€", "")
    if direction == "Kauf" or data[pos + 1][0].split()[2] == "execution" or data[pos + 1][0].split()[0] == "Buy":
        Betrag = "-" + Betrag
    elif direction == "Verkauf" or data[pos + 1][0].split()[0] == "Sell":
        pass
    else:
        print("*********ERORR with parsing Handel direction! got direction:",direction)
    
    return Zahlungsbeteiligter, Betrag



    
#general parsing of the pdf using fitz (does not retain table structure)
data = getPdfData(pdf_path)


#setup parsing of "data"
df = pd.DataFrame(columns=["Datum", "Zahlungsbeteiligter", "Betrag"])
globalDF = pd.DataFrame(columns = ["Datum", "Zahlungsbeteiligter", "Betrag", "Saldo_Download"])
row = 0
globalRow = 0
Saldo = 0

for pos in range(len(data)-1,1, -1):
    foundFlag = 0
    globalFlag = 0
    
    Datum = getDate(data, pos)
    Zahlungsbeteiligter = ""
    Betrag = ""
    
    #parsing
    element = data[pos][0].strip()
    if element in TagsBase:
        Zahlungsbeteiligter, Betrag = parseBaseCase(data, pos)  
        foundFlag = 1
        
    if element == "Handel":
        
        Zahlungsbeteiligter, Betrag = parseTrade(data, pos)
        globalFlag = 1
        
    if element in TagsBaseGlobal:
        Zahlungsbeteiligter, Betrag = parseBaseCase(data, pos)  
        globalFlag = 1
        
        
    #writing in dataframes
    if foundFlag:
        df.loc[row] = [formatDate(Datum), Zahlungsbeteiligter, Betrag]
        row +=1
        
    if globalFlag or foundFlag:
        Saldo = float(data[findBetragPos(data, pos)+1][0].replace(".", "").replace(",", ".").replace("€", ""))
        globalDF.loc[globalRow] = [formatDate(Datum), Zahlungsbeteiligter, Betrag, Saldo]
        globalRow += 1
       
        
#format Betrag
df["Betrag"] = df["Betrag"].str.replace(".", "", regex=False)  # Entferne Tausenderpunkte
df["Betrag"] = df["Betrag"].str.replace(",", ".", regex=False).astype(float)  # Ersetze Komma & wandle in Float

globalDF["Betrag"] = globalDF["Betrag"].str.replace(".", "", regex=False)  # Entferne Tausenderpunkte
globalDF["Betrag"] = globalDF["Betrag"].str.replace(",", ".", regex=False).astype(float)  # Ersetze Komma & wandle in Float
print(globalDF["Betrag"].sum())

df["Datum"] = pd.to_datetime(df["Datum"])

#write kontoauszug
df.to_csv(output_csv, index=False, encoding="utf-8")
print(f"CSV gespeichert unter: {output_csv}")

df.to_excel(output_excel, index=False)
print(f"Excel gespeichert unter: {output_excel}")

#write global file (every transaction of Trade Republic)
globalDF.to_csv(globalFile, index = False)
print(f"global CSV gespeichert unter: {globalFile}")

if DEBUG:
    #write the debug file to create the correct parser
    dataDF = pd.DataFrame(data)
    dataDF.to_csv(debugFile, index = False)
    print(f"debug CSV gespeichert unter: {debugFile}")
    # print(data)
    # print(*data, sep="\n")


