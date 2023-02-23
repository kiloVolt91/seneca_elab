##  ANALISI DATI SENECAZETA
## CREAZIONE DI CLUSTER DI DATI RAGGRUPPATI PER MENSILITà

import os
import pandas as pd
from datetime import datetime
import configparser
from init import init_path

config = configparser.ConfigParser()
config.read(init_path)

anno = input('Inserire anno in formato YYYY: ')
path = config['user']['origin_path_an_seneca']
cwd = os.getcwd()
nome_file = 'riepilogo_seneca_giornaliero'+anno

os.chdir(path)
dirs = []
for name in os.listdir(path):
    dirs.append(name)
print('cartelle disponibili: ', dirs)
i_dir = int(input('Inserisci il numero di posizione della cartella: '))
selezione = dirs[i_dir-1]
print('Hai selezionato la seguente cartella: ', selezione) 

os.chdir(selezione)

if os.path.exists(path+'/'+selezione+'/' + nome_file + '.csv') == True:     
    df_day = pd.read_csv(path+'/'+selezione+'/' + nome_file + '.csv', sep = ';', index_col=False, decimal = ",")
else:
    print ('File non trovato: ')


os.chdir('..')
cwd = os.getcwd()



def time_to_ms (time):
    a=time
    a=datetime.strptime(a, '%Y-%m-%d %H:%M:%S')
    a = a.timestamp()*1000
    return a




tempo=[]
for i in range (0,df_day.shape[0]):
    time_f=df_day.iloc[i,2]
    time_i=df_day.iloc[i,1]
    delta_tempo = (time_to_ms(time_f)-time_to_ms(time_i))
    delta_tempo = delta_tempo/(1000*3600)
    tempo.append(delta_tempo)


df_day['intervallo_misura']=tempo

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

lista=[]
nome_mese = ('Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno', 'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre')
data_inizio_ts = '2021-12-31 23:59:30'
today = str(datetime.today())
i=0
nome_colonna_energia = df_day.columns[0]
nome_colonna_tempo_in = df_day.columns[1]
nome_colonna_tempo_fin = df_day.columns[2]
df_mese = pd.DataFrame()

for i in range(0,len(valori_iniziali_mensili)):
    data_fine_ts = valori_finali_mensili[i]
    data_inizio_ts = valori_iniziali_mensili[i]
    df_iter = df_day.loc[(df_day[nome_colonna_tempo_fin] < data_fine_ts ) & (df_day[nome_colonna_tempo_in] >= data_inizio_ts), [nome_colonna_energia,'intervallo_misura']]

    df_iter.rename(columns={'intervallo_misura':'intervallo_misura_'+str(i+1)}, inplace = True)
    df_mese = pd.concat([df_mese,df_iter.reset_index().drop(columns=['index']).rename(columns={nome_colonna_energia:nome_mese[i]})], axis=1)

##creare un indice dell'intervallo temporale di ciascuna misura per escludere outlier

range_val_inf = 23.5
range_val_sup = 24.5
df_analisi = pd.DataFrame()

for j in range (0,len(df_mese.columns),2):
    nome_colonna_energia = df_mese.columns[j]
    nome_colonna_intervallo = df_mese.columns[j+1]
    df_iter = df_mese.loc[(df_mese[nome_colonna_intervallo] <= range_val_sup ) & (df_mese[nome_colonna_intervallo] >= range_val_inf), [nome_colonna_energia]]
    df_iter.reset_index().drop(columns=['index'])
    df_analisi = pd.concat([df_analisi, df_iter], axis=1)

riepilogo=pd.DataFrame()
riepilogo['totale']=df_analisi.sum()
riepilogo['massimi']=df_analisi.max()
riepilogo['mediana']=df_analisi.median()
riepilogo['mean']=df_analisi.mean()
riepilogo['dev_std']=df_analisi.std()


riepilogo.to_csv(r''+path+'/riepilogo_anno'+str(anno)+'.csv', index=False, decimal =',', sep=';')
print(riepilogo)
print('Fine Operazioni')