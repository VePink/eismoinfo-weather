import datetime
timestamp=str(datetime.datetime.now().strftime("%Y%m%d"))
filename='EIW_'+timestamp
dest_dir='C:/Users/Ve/Documents/GitHub/eismoinfo-weather/LOGS/'

print('---------- SCRAPE JSON -----------')
import urllib.request, json
with urllib.request.urlopen("http://eismoinfo.lt/weather-conditions-service") as url:
    measurment_list = json.loads(url.read().decode())

count = 0
for measurment in measurment_list:
    print('Getting data of station '+str(count)+' ...')
    if len(measurment) == 32:
        measurment['perspejimai'] = [] #add field with empty list to match sutructure of entries with lenght 33.
    count += 1

# test if all measurments contain same amount of attributes. Terminate process if not.
it = iter(measurment_list)
the_len = len(next(it))
if not all(len(l) == the_len for l in it):
     raise ValueError('not all measurments contain same amount of attributes!')

from collections import defaultdict
result = defaultdict(list)

for d in measurment_list: 
    for key, value in d.items():
        result[key].append(value)

import pandas as pd
new_df = pd.DataFrame.from_dict(result)

print('---------- DATAFRAME CLEANUP / RESTRUCTURE -----------')
# drop excessive/redundant fields\

new_df.drop(columns=['numeris', # road number, always static
                    'irenginys', #name of station, always static
                    'pavadinimas', # name of the road, always static
                    'kilometras', # M-value along road, always static
                    'ilguma', #LKS94 coordinates, duplicates WGS84. Always static
                    'platuma', #LKS94 coordinates, duplicates WGS84. Always static
                    'lat', #Always static
                    'lng', #Always static
                    'perspejimai',# warnings. Does not provide new/intresting data. 
                    'surinkimo_data_unix'
                    ],inplace=True)

# improve column names                
new_df.rename(columns = {'surinkimo_data': 'timestamp', 
                        'id': 'station_UID',
                        'oro_temperatura': 'air_temp_C',
                        'vejo_greitis_vidut': 'wind_spd_avg_ms',
                        'krituliu_tipas': 'prcp_type',
                        'krituliu_kiekis': 'prcp_amount_mm',
                        'dangos_temperatura': 'road_temp_C',
                        'matomumas': 'visibility_m',
                        'rasos_taskas': 'dew_pnt_C',
                        'kelio_danga': 'road_surface',
                        'uzsalimo_taskas': 'freezing_pnt_C',
                        'vejo_greitis_maks': 'wind_spd_max_ms',
                        'vejo_kryptis': 'wind_dir',
                        'sukibimo_koeficientas': 'road_grip_koef',
                        'konstrukcijos_temp_007':'undrgrnd_007_temp_C',
                        'konstrukcijos_temp_020':'undrgrnd_020_temp_C',
                        'konstrukcijos_temp_050':'undrgrnd_050_temp_C',
                        'konstrukcijos_temp_080':'undrgrnd_080_temp_C',
                        'konstrukcijos_temp_110':'undrgrnd_110_temp_C',
                        'konstrukcijos_temp_130':'undrgrnd_130_temp_C',
                        'konstrukcijos_temp_140':'undrgrnd_140_temp_C',
                        'konstrukcijos_temp_170':'undrgrnd_170_temp_C',
                        'konstrukcijos_temp_200':'undrgrnd_200_temp_C'
                        },inplace=True)


new_df.replace(to_replace='"', value='', inplace=True)
new_df.replace(to_replace='Klaida', value='', inplace=True) #Replace "Error" with empty.

print('---------- WEATHER CONDITIONS TO CODED VALUES -----------')
di_wind_dir={
    'Šiaurės': 0, 
    'Šiaurės rytų': 45, 
    'Rytų': 90, 
    'Pietryčių': 135, 
    'Pietų': 180, 
    'Pietvakarių': 225,
    'Vakarų': 270, 
    'Šiaurės vakarų': 315
    }
new_df["wind_dir"].replace(di_wind_dir, inplace=True)

di_road_surface={
    'Sausa': 0, 
    'Drėgna': 1, 
    'Šlapia': 2, 
    'Apsnigta': 3, 
    'Apledėjusi': 4, 
    'Pažliugęs sniegas': 5
    }
new_df["road_surface"].replace(di_road_surface, inplace=True)

di_prcp_type={
    'Nėra': 0,
    'Dulksna': 1,
    'Krituliai': 2,
    'Rūkas': 3,
    'Migla': 4,
    'Lietus, silpnas': 5,
    'Lietus, vidutinis': 6,
    'Lietus, stiprus': 7,
    'Kruša': 8,
    'Lietus': 9,
    'Sniegas, silpnas': 10,
    'Lietus su sniegu': 11,
    'Sniegas, vidutinis': 12,
    'Sniegas, stiprus': 13
    }
new_df["prcp_type"].replace(di_prcp_type, inplace=True)

print(len(new_df))
new_df = new_df[new_df['timestamp']>= str(datetime.datetime.now().strftime("%Y-%m-%d")) +" "+ '00:00']
print("**************************")
print(len(new_df))

print('---------- DATAFRAME SAVE AS CSV -----------')
import os.path
if os.path.isfile(dest_dir+filename+'.csv'):
    existing_df=pd.read_csv(dest_dir+filename+'.csv', index_col=None, header=0)  
    combined_df = pd.concat([new_df, existing_df], ignore_index=True).drop_duplicates().reset_index(drop=True)
    combined_df.to_csv(dest_dir+filename+'.csv', index=False)
else:
    new_df.to_csv(dest_dir+filename+'.csv', index=False)



# Below are notes for making EXE package with pyinstaller from PY code.

# cd C:\Users\Ve\Documents\GitHub\eismoinfo-weather\EIW
# pyinstaller ./EIW_scraper.py --onefile