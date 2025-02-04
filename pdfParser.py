import fitz  # PyMuPDF
import pandas as pd

pdf_path = r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\Kontoauszug.pdf"  # Pfad zu deinem PDF
output_csv = r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\kontoauszug.csv"  # Ziel-CSV-Datei
output_excel =  r"C:\Tickets\Finanzen\new_setup\Input_Data\TR_Daten.xlsx"
debugFile =  r"C:\Tickets\Finanzen\new_setup\Downloaded_Data\debug.csv"
globalFile = r"C:\Tickets\Finanzen\new_setup\Non_pipeline_files\globalTR.csv"

def findBetragPos(data, start) -> int:
    element = ""
    while r"€" not in element:
        if start > 0:
            start += 1
            element = str(data[start][0])
           # print(element)
    return start

def formatDate(date):
    day = date[:2]
    month = date[3:-5].lower()
    year = date[-4:]
    match month:
        case "jan":
            month = "01"
        case "feb":
            month = "02"
        case "märz":
            month = "03"
        case "apr":
            month = "04"
        case "mai":
            month = "05"
        case "juni":
            month = "06"
        case "juli":
            month = "07"
        case "aug":
            month = "08"
        case "sept":
            month = "09"
        case "okt":
            month = "10"
        case "nov":
            month = "11"
        case "dez":
            month = "12"
    return year + "-" + month + "-" + day
        
    
    

data = []

with fitz.open(pdf_path) as pdf:
    for page in pdf:
        data.append(["***************"])
        text = page.get_text("text")  # Gesamten Text extrahieren
        lines = text.split("\n")  # Zeilenweise aufsplitten
        for line in lines:
            data.append([line])  # Jede Zeile als eigene Liste speichern

# In DataFrame konvertieren (Spaltennamen ggf. anpassen)
# df = pd.DataFrame(data)
df = pd.DataFrame(columns=["Datum", "Zahlungsbeteiligter", "Betrag"])
globalDF = pd.DataFrame(columns = ["Datum", "Zahlungsbeteiligter", "Betrag"])
row = 0
globalRow = 0

for pos in range(len(data)-1,1, -1):
    foundFlag = 0
    globalFlag = 0
    if len(data[pos-2][0].split()) == 2:
        Datum = data[pos-2][0] + data[pos-1][0]
    else:
        Datum = data[pos-3][0] + data[pos-2][0] + data[pos-1][0]
    Datum = Datum.replace(".","").replace(" ", "_")
    Zahlungsbeteiligter = ""
    Betrag = ""
    element = data[pos][0].strip()
    if element == "Kartentransaktion":
        
        #Betrag = data[pos + 2][0].replace(r"\xa0€", "€")
        BetragPos = findBetragPos(data, pos)
        Betrag = "-" + data[BetragPos][0].replace(r"€", "")
        Zahlungsbeteiligter = "".join([e[0] for e in data[pos+1:BetragPos]])
        # print(Datum, Zahlungsbeteiligter, Betrag, sep=" | ")
        foundFlag = 1
    if element == "Überweisung":
        
        if data[pos + 2][0][0] in ["0","1","2","3","4","5","6","7","8","9"]:
            #Betrag = data[pos + 2][0].replace(r"\xa0€", "€")
            BetragPos = findBetragPos(data, pos)
            Betrag = data[BetragPos][0].replace(r"€", "")
            Zahlungsbeteiligter = "".join([e[0] for e in data[pos+1:BetragPos]])
            # print(Datum, Zahlungsbeteiligter, Betrag, sep=" | ")
        else:
            #Betrag = data[pos + 3][0].replace(r"\xa0€", "€")
            BetragPos = findBetragPos(data, pos)
            Betrag = data[BetragPos][0].replace(r"€", "")
            Zahlungsbeteiligter = "".join([e[0] for e in data[pos+1:BetragPos]])
            # print(Datum, Zahlungsbeteiligter, Betrag, sep=" | ")
        foundFlag = 1
    if  element == "SEPA":
        Zahlungsbeteiligter = data[pos+1][0]
        Betrag = "-" + data[pos+2][0].replace("€","")
        foundFlag = 1
        
    if element == "Handel":
        Zahlungsbeteiligter = "Portfolio Transaction: " + data[pos + 1][0].split()[4]
        direction = data[pos + 1][0].split()[3]
        
        Betrag = data[findBetragPos(data, pos)][0].replace("€", "")
        if direction == "Kauf" or data[pos + 1][0].split()[2] == "execution" or data[pos + 1][0].split()[0] == "Buy":
            pass
        elif direction == "Verkauf" or data[pos + 1][0].split()[0] == "Sell":
            Betrag = "-" + Betrag
        else:
            print("*********ERORR with parsing Handel direction! got direction:",direction)
        
        globalFlag = 1
        
    if foundFlag:
        df.loc[row] = [formatDate(Datum), Zahlungsbeteiligter, Betrag]
        row +=1
        
    if globalFlag:
        globalDF.loc[globalRow] = [formatDate(Datum), Zahlungsbeteiligter, Betrag]
        globalRow += 1
    
df["Betrag"] = df["Betrag"].str.replace(".", "", regex=False)  # Entferne Tausenderpunkte
df["Betrag"] = df["Betrag"].str.replace(",", ".", regex=False).astype(float)  # Ersetze Komma & wandle in Float
df["Datum"] = pd.to_datetime(df["Datum"])
print(df["Betrag"].sum())
df.to_csv(output_csv, index=False, encoding="utf-8")
df.to_excel(output_excel, index=False)


dataDF = pd.DataFrame(data)
dataDF.to_csv(debugFile, index = False)
globalDF.to_csv(globalFile, index = False)
# print(data)
# print(*data, sep="\n")

# print(f"CSV gespeichert unter: {output_csv}")
