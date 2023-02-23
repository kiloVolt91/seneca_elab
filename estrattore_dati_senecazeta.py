## SCRIPT DI SELEZIONE ED ORDINE DEI DATI REGISTRATI DAL DATALOGGER CEDA-SENECA. 
## VERIFICARE LA CORRETTEZZA DEI NOMI E LA CONGRUENZA DELLA COSTANTE DI INTEGRAZIONE ADOPERATA. 
## INSERIRE I .CSV DA ANALIZZARE ALL'INTERNO DELLA CARTELLA "curve_seneca"

import pandas as pd
import os
from datetime import datetime
import numpy as np
from matplotlib import pyplot as plt
from init import init_path
import configparser

config = configparser.ConfigParser()
config.read(init_path)
origin_path = config['user']['origin_path_ex_seneca']
output_path = config['user']['output_data_path_seneca']


anno = int(input('Inserire anno in formato YYYY: ')) #inserire un controllo

#SELEZIONARE LA CORRETTA COSTANTE DI INTEGRAZIONE IN BASE ALLA TIPOLOGIA DI DATI SCARICATI. IN CASO DI DATI GIA' CONVERTITI
#NON è NECESSARIO EFFETTUARE ALCUNA CONVERSIONE.
## ANALOGAMENTE, SELEZIONARE IN MODO UNIVOCO IL NOME DEL FILE

costante_integrazione = 1 # NESSUNA CONVERSIONE
#ALTRE COSTANTI: 2000 - CONTATORE DI SCAMBIO; 800 - CONTATORE DI PRODUZIONE, 1 - NESSUNA CONVERSIONE

#nome_file = 'energia-attiva'#+str(anno)
#nome_file = 'registro-280-immissione'
nome_file = 'reg-180-energia-attiva-p'
#nome_file = 'reg-280-energia-attiva-p'
#nome_file = 'registro-180-prelievo-en'
#nome_file = 'energia-attiva'

df_seneca = pd.DataFrame()
os.chdir(origin_path)

filepath = os.path.join (origin_path, nome_file+'.csv')
if os.path.exists(filepath) == True:     
    df_seneca = pd.read_csv(filepath, sep = ';', index_col=False, decimal = ",")
else:
    print ('File non trovato: ')
nome_colonna_tempo = df_seneca.columns[0]
nome_colonna_energia = df_seneca.columns[1]
df_seneca = df_seneca[ df_seneca[nome_colonna_energia] != 0] #Eliminazione dei valori nulli


export_path = output_path + '/riepilogo_seneca'+str(anno)+'.csv'


df_seneca.to_csv(export_path, index=False, decimal =',', sep=';')

##COSTRUZIONE DEL VETTORE CONTENENTE LE DATE DI RIFERIMENTO PER LA SELEZIONE DEI DATI
mesi= ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']  
month = { "01" : 31 , "02" : 28 , "03" : 31 , "04" : 30 , "05" : 31 , "06" : 30 , "07" : 31 , "08" : 31 , "09" : 30 , "10" : 31 , "11" : 30 , "12" : 31}

str_ora_inizio = '00:00:00'
str_ora_fine = '00:00:15'

valori_iniziali=[]
valori_finali=[]
valori_iniziali_mensili =[]
valori_finali_mensili = []
valori_iniziali.append(str(anno-1)+'-'+str('12')+'-'+str('31')+' '+str_ora_inizio)
for m in mesi:
    valori_iniziali_mensili.append(str(anno)+'-'+str(m)+'-'+str(1).zfill(2)+' '+str_ora_inizio)
    if int(m) < 12:
        valori_finali_mensili.append(str(anno)+'-'+str(int(m)+1).zfill(2)+'-'+str(1).zfill(2)+' '+str_ora_fine)
        
    for d in range (1, month[m]+1):
        valori_iniziali.append(str(anno)+'-'+str(m)+'-'+str(d).zfill(2)+' '+str_ora_inizio)
        valori_finali.append(str(anno)+'-'+str(m)+'-'+str(d).zfill(2)+' '+str_ora_fine)

valori_finali.append(str(anno+1)+'-'+str('01')+'-'+str('01')+' '+str_ora_fine) 
valori_finali_mensili.append(str(anno+1)+'-'+str('01')+'-'+str(1).zfill(2)+' '+str_ora_fine)

lgti = len(valori_iniziali)
lgtm =len(valori_iniziali_mensili)

## Dataframe con produzione giornaliera


df = df_seneca.copy()
df_en_giornaliera = pd.DataFrame()
delta_energia=[]
datatime_inizio = []
datatime_fine = []
data_inizio_ts = str(anno-1)+'-12-31 23:59:30'
today = str(datetime.today())
i=0

while i<lgti:
    while df.head(1).iloc[0,0] > valori_finali[i]:
        i=i+1
        
    
    if today < valori_finali[i]:
        break
        
    data_fine_ts = valori_finali[i]
    selezione_dati = df.loc [(df[nome_colonna_tempo] < data_fine_ts ) & (df[nome_colonna_tempo] >= data_inizio_ts), [nome_colonna_tempo,nome_colonna_energia]]
    primo_vettore = selezione_dati.head(1)
    ultimo_vettore = selezione_dati.tail(1)
    
    if primo_vettore.iloc[0,0] != ultimo_vettore.iloc[0,0]:
        datatime_inizio.append(primo_vettore.iloc[0,0])
        datatime_fine.append(ultimo_vettore.iloc[0,0])
        delta_energia.append((ultimo_vettore.iloc[0,1] - primo_vettore.iloc[0,1])*costante_integrazione)    
        
    data_inizio_ts = ultimo_vettore.iloc[0,0]
    i=i+1
    #print(i)
df_en_giornaliera['Energia [kWh]'] = delta_energia
df_en_giornaliera['DataTime Iniziale']=datatime_inizio
df_en_giornaliera['DataTime Finale'] = datatime_fine

export_path = output_path + '/riepilogo_seneca_giornaliero'+str(anno)+'.csv'

df_en_giornaliera.to_csv(export_path, index=False, decimal =',', sep=';')


## Dataframe con produzione mensile

df = df_seneca.copy()
df_en_mensile = pd.DataFrame()
delta_energia=[]
datatime_inizio = []
datatime_fine = []
data_inizio_ts = str(anno-1)+'-12-31 23:59:30'
today = str(datetime.today())
i=0

while i<lgtm:
    if today < valori_finali_mensili[i]:
        break
        
    data_fine_ts = valori_finali_mensili[i]
    selezione_dati = df.loc [(df[nome_colonna_tempo] < data_fine_ts ) & (df[nome_colonna_tempo] >= data_inizio_ts), [nome_colonna_tempo, nome_colonna_energia]]
    primo_vettore = selezione_dati.head(1)
    ultimo_vettore = selezione_dati.tail(1)
    
    if selezione_dati.empty == False:
        if primo_vettore.iloc[0,0] != ultimo_vettore.iloc[0,0]:
            datatime_inizio.append(primo_vettore.iloc[0,0])
            datatime_fine.append(ultimo_vettore.iloc[0,0])
            delta_energia.append((ultimo_vettore.iloc[0,1] - primo_vettore.iloc[0,1])*costante_integrazione)    
        data_inizio_ts = ultimo_vettore.iloc[0,0]
    i=i+1
    
df_en_mensile['Energia [kWh]'] = delta_energia
df_en_mensile['DataTime Iniziale']=datatime_inizio
df_en_mensile['DataTime Finale'] = datatime_fine

export_path = output_path + 'riepilogo_seneca_mensile'+str(anno)+'.csv'

df_en_mensile.to_csv(export_path, index=False, decimal =',', sep=';')

print('Fine Blocco')