

```python
from bs4 import BeautifulSoup
import time
import requests
import pandas as pd
import re
from tqdm import tqdm
import datetime
import numpy as np
import multiprocessing
from pebble import ProcessPool
from concurrent.futures import TimeoutError





#######################################
# define helper functions
#######################################



def get_soup(url, echo=False):
    
    if echo:
        print('scraping:', url)
    
    r  = requests.get(url)

    data = r.text
    soup = BeautifulSoup(data, "lxml")
    return soup





def parallelize_function(url_list, func):
    
    start_ts = datetime.datetime.now()
  
    num_cores = multiprocessing.cpu_count()-1  #leave one free to not freeze machine
    print('processing function on', num_cores, 'cores...')
    pool = multiprocessing.Pool(num_cores)
    results = pool.map(func, url_list)
    pool.close()
    pool.join()
    
    print('Finished parallell processing after', datetime.datetime.now() - start_ts, 'seconds')
    
    return results




def parallelize_function_timeout(urls, func):
    with ProcessPool() as pool:
        future = pool.map(func, urls, timeout=90)

        iterator = future.result()
        result = pd.DataFrame()
        while True:
            try:
                result = result.append(next(iterator))
            except StopIteration:
                break
            except TimeoutError as error:
                print("scraping took longer than %d seconds" % error.args[1])
        return result



def scrape_meta_chunk(url, return_soup=False):

    
    soup = get_soup(url)

    try:
        
        data = pd.DataFrame(
            [
            [location['data-result-id'], 
             location.getText().split(', ')[-1], 
             location.getText().split(', ')[-2], 
             location.getText().split(', ')[-3] if len(location.getText().split(', ')) == 3 else '', 
             str(datetime.datetime.now())] 
            for location in 
            soup.findAll("button", {"class": "button-link link-internal result-list-entry__map-link", "title": "Auf der Karte anzeigen"})], columns=['id','city_county', 'city_quarter', 'street', 'scraped_ts']
        )
        #data = pd.DataFrame({'id':expose_list,'city_county':city_county, 'city_quarter': city_quarter, 'street':street, 'scraped_ts': str(datetime.datetime.now())})
        
    except Exception as err:
        print(url, err)
        data = pd.DataFrame()
    if return_soup:
        return data, soup
    else:
        return data



def scrape_details_chunk(realty):
        
    expose_id = []
    title = []
    realty_type = []
    floor = []
    square_m = []
    storage_m = []
    num_rooms = []
    num_bedrooms = []
    num_baths = []
    num_carparks = []
    year_built = []
    last_refurb= []
    quality = []
    heating_type = []
    fuel_type = []
    energy_consumption = []
    energy_class = []
    net_rent = []
    gross_rent = []
    
    
    #for realty in chunk:
        
    url = 'https://www.immobilienscout24.de/expose/' + realty
    
    soup= get_soup(url)

    expose_id.append(realty)
    title.append(soup.find('title').getText())

    if soup.find('dd', {'class':"is24qa-typ grid-item three-fifths"}):
        realty_type.append(soup.find('dd', {'class':"is24qa-typ grid-item three-fifths"}).getText().replace(' ', ''))
    else:
        realty_type.append(0)

    if soup.find('dd', {'class':"is24qa-etage grid-item three-fifths"}):
        floor.append(soup.find('dd', {'class':"is24qa-etage grid-item three-fifths"}).getText())
    else:
        floor.append(0)

    if soup.find('dd', {'class':"is24qa-wohnflaeche-ca grid-item three-fifths"}):
        square_m.append(soup.find('dd', {'class':"is24qa-wohnflaeche-ca grid-item three-fifths"}).getText().replace(' ', '').replace('m²', ''))
    else:
        square_m.append(0)

    if soup.find('dd', {'class':"is24qa-nutzflaeche-ca grid-item three-fifths"}):
        storage_m.append(soup.find('dd', {'class':"is24qa-nutzflaeche-ca grid-item three-fifths"}).getText().replace(' ', '').replace('m²', ''))
    else:
        storage_m.append(0)

    if soup.find('dd', {'class':"is24qa-zimmer grid-item three-fifths"}):
        num_rooms.append(soup.find('dd', {'class':"is24qa-zimmer grid-item three-fifths"}).getText().replace(' ', ''))
    else:
         num_rooms.append(0)

    if soup.find('dd', {'class':"is24qa-schlafzimmer grid-item three-fifths"}):
        num_bedrooms.append(soup.find('dd', {'class':"is24qa-schlafzimmer grid-item three-fifths"}).getText().replace(' ', ''))
    else:
        num_bedrooms.append(0)

    if soup.find('dd', {'class':"is24qa-badezimmer grid-item three-fifths"}):
        num_baths.append(soup.find('dd', {'class':"is24qa-badezimmer grid-item three-fifths"}).getText().replace(' ', ''))
    else:
        num_baths.append(0)

    if soup.find('dd', {'class':"is24qa-garage-stellplatz grid-item three-fifths"}):
        num_carparks.append(re.sub('[^0-9]', '', soup.find('dd', {'class':"is24qa-garage-stellplatz grid-item three-fifths"}).getText()))
    else:
        num_carparks.append(0)

    if soup.find('div', {'class':"is24qa-kaltmiete is24-value font-semibold"}):
        net_rent.append(re.sub('[^0-9]', '', soup.find('div', {'class':"is24qa-kaltmiete is24-value font-semibold"}).getText()))
    else:
        net_rent.append(0)

    if soup.find('dd', {'class':"is24qa-baujahr grid-item three-fifths"}):
        year_built.append(soup.find('dd', {'class':"is24qa-baujahr grid-item three-fifths"}).getText())
    else:
        year_built.append(0)

    if soup.find('dd', {'class':"is24qa-modernisierung-sanierung grid-item three-fifths"}):
        last_refurb.append(soup.find('dd', {'class':"is24qa-modernisierung-sanierung grid-item three-fifths"}).getText())
    else:
        last_refurb.append(0)

    if soup.find('dd', {'class':"is24qa-qualitaet-der-ausstattung grid-item three-fifths"}):
        quality.append(soup.find('dd', {'class':"is24qa-qualitaet-der-ausstattung grid-item three-fifths"}).getText().strip())
    else:
        quality.append(0)

    if soup.find('dd', {'class':"is24qa-heizungsart grid-item three-fifths"}):
        heating_type.append(soup.find('dd', {'class':"is24qa-heizungsart grid-item three-fifths"}).getText().strip())
    else:
        heating_type.append(0)

    if soup.find('dd', {'class':"is24qa-wesentliche-energietraeger grid-item three-fifths"}):
        fuel_type.append(soup.find('dd', {'class':"is24qa-wesentliche-energietraeger grid-item three-fifths"}).getText().strip())
    else:
        fuel_type.append(0)

    if soup.find('dd', {'class':"is24qa-endenergiebedarf grid-item three-fifths"}):
        energy_consumption.append(soup.find('dd', {'class':"is24qa-endenergiebedarf grid-item three-fifths"}).getText().strip())
    else:
        energy_consumption.append(0)

    if soup.find('dd', {'class':"is24qa-energieeffizienzklasse grid-item three-fifths"}):
        energy_class.append(soup.find('dd', {'class':"is24qa-energieeffizienzklasse grid-item three-fifths"}).getText().strip())
    else:
        energy_class.append(0)

    if soup.find('dd', {'class':"is24qa-gesamtmiete grid-item three-fifths font-bold"}):
        gross_rent.append(soup.find('dd', {'class':"is24qa-gesamtmiete grid-item three-fifths font-bold"}).getText().strip().replace(' €',''))
    else:
        gross_rent.append(0)

    results = pd.DataFrame({
    'id': expose_id,
    'title': title, 
    'realty_type': realty_type, 
    'floor': floor,
    'square_m': square_m,
    'storage_m': storage_m, 
    'num_rooms': num_rooms, 
    'num_bedrooms': num_bedrooms,
    'num_baths': num_baths,
    'num_carparks': num_carparks, 
    'year_built': year_built, 
    'last_refurb': last_refurb,
    'quality': quality,
    'heating_type': heating_type, 
    'fuel_type': fuel_type, 
    'energy_consumption': energy_consumption,
    'energy_class': energy_class,
    'net_rent': net_rent, 
    'gross_rent': gross_rent,
    'scraped_ts': str(datetime.datetime.now())
    })
    
    return results





```


```python
#######################################
# Crawl first page:
#######################################
url = 'https://www.immobilienscout24.de/Suche/S-T/P-1/Wohnung-Miete/Umkreissuche/Hamburg/-/1840/2621814/-/-/30'


realty_meta_df, soup = scrape_meta_chunk(url, return_soup=True)
num_pages = len(soup.find_all('option'))
print('scraped', len(realty_meta_df), 'realties on first page')
print('found', num_pages, 'pages to scrape')
```

    scraped 20 realties on first page
    found 101 pages to scrape



```python
#######################################
# Crawl next pages:
#######################################


realty_meta_urls = ['https://www.immobilienscout24.de/Suche/S-T/P-' + str(page) + '/Wohnung-Miete/Umkreissuche/Hamburg/-/1840/2621814/-/-/30' for page in range(2, num_pages + 1)]
realty_meta_df = realty_meta_df.append(parallelize_function_timeout(realty_meta_urls, scrape_meta_chunk))
#realty_meta_df = pd.concat(realty_meta, ignore_index=True)
print('Done')
 
    
    

    

```

    Done



```python
#######################################
# scrape each expose page:
#######################################




expose_id = []
title = []
realty_type = []
floor = []
square_m = []
storage_m = []
num_rooms = []
num_bedrooms = []
num_baths = []
num_carparks = []
year_built = []
last_refurb= []
quality = []
heating_type = []
fuel_type = []
energy_consumption = []
energy_class = []
net_rent = []
gross_rent = []


realty_details = pd.DataFrame(columns=[
    'expose_id',
    'title', 
    'realty_type', 
    'floor',
    'square_m',
    'storage_m', 
    'num_rooms', 
    'num_bedrooms',
    'num_baths',
    'num_carparks', 
    'year_built', 
    'last_refurb',
    'quality',
    'heating_type', 
    'fuel_type', 
    'energy_consumption',
    'energy_class',
    'net_rent', 
    'gross_rent'
])




# multi core scraping
#######################################

realties = list(set(list(realty_meta_df.id)))
print('scraping', len(realties), 'realties...')
realty_details_df = parallelize_function_timeout(realties, scrape_details_chunk)
#realty_details_df = pd.concat(realty_details, ignore_index=True)
realty_details_df = realty_details_df.drop_duplicates()
realty_details_df = realty_details_df.reset_index(drop=True)
print('scraped', len(realty_details_df), 'realties.')









```

    scraping 2020 realties...
    scraped 2020 realties.



```python
realty_meta_df['id'] = realty_meta_df['id'].astype(int)
realty_details_df['id'] = realty_details_df['id'].astype(int)

final_data = realty_meta_df.drop('scraped_ts',axis = 1).merge(realty_details_df, on='id', suffixes = ['_meta', '_details'])
```


```python
final_data.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>city_county</th>
      <th>city_quarter</th>
      <th>street</th>
      <th>title</th>
      <th>realty_type</th>
      <th>floor</th>
      <th>square_m</th>
      <th>storage_m</th>
      <th>num_rooms</th>
      <th>...</th>
      <th>year_built</th>
      <th>last_refurb</th>
      <th>quality</th>
      <th>heating_type</th>
      <th>fuel_type</th>
      <th>energy_consumption</th>
      <th>energy_class</th>
      <th>net_rent</th>
      <th>gross_rent</th>
      <th>scraped_ts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>110713974</td>
      <td>Hamburg</td>
      <td>Altona-Nord</td>
      <td>Glückel-von-Hameln-Straße  2</td>
      <td>neue Wohnung - neues Glück</td>
      <td>Etagenwohnung</td>
      <td>0</td>
      <td>75,14</td>
      <td>0</td>
      <td>2</td>
      <td>...</td>
      <td>2019</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1150</td>
      <td>1.370,66</td>
      <td>2019-05-14 13:30:01.252678</td>
    </tr>
    <tr>
      <th>1</th>
      <td>110714010</td>
      <td>Hamburg</td>
      <td>Altona-Nord</td>
      <td>Susanne-von-Paczensky-Straße  9</td>
      <td>Grandiose 4-Zimmerwohung! ***Erstbezug***</td>
      <td>Etagenwohnung</td>
      <td>0</td>
      <td>99,39</td>
      <td>0</td>
      <td>4</td>
      <td>...</td>
      <td>2019</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1440</td>
      <td>1.828,13</td>
      <td>2019-05-14 13:29:57.649693</td>
    </tr>
    <tr>
      <th>2</th>
      <td>110247048</td>
      <td>Hamburg</td>
      <td>Altona-Nord</td>
      <td>Susanne-von-Paczensky-Straße  7</td>
      <td>Traumhafte Penthousewohnung! ***Erstbezug***</td>
      <td>Etagenwohnung</td>
      <td>0</td>
      <td>166,02</td>
      <td>0</td>
      <td>5</td>
      <td>...</td>
      <td>2019</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>2500</td>
      <td>3.090,15</td>
      <td>2019-05-14 13:29:32.219968</td>
    </tr>
    <tr>
      <th>3</th>
      <td>110247042</td>
      <td>Hamburg</td>
      <td>Altona-Nord</td>
      <td>Susanne-von-Paczensky-Straße  11</td>
      <td>Geniale 3-Zimmerwohnung! ***Erstbezug***</td>
      <td>Etagenwohnung</td>
      <td>0</td>
      <td>97,65</td>
      <td>0</td>
      <td>3</td>
      <td>...</td>
      <td>2019</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1470</td>
      <td>1.854,57</td>
      <td>2019-05-14 13:29:44.507045</td>
    </tr>
    <tr>
      <th>4</th>
      <td>110714047</td>
      <td>Hamburg</td>
      <td>Altona-Nord</td>
      <td>Eva-Rühmkorf-Straße  8</td>
      <td>***Hervorragende 3-Zimmerwohnung*** Erstbezug!</td>
      <td>Etagenwohnung</td>
      <td>0</td>
      <td>86,04</td>
      <td>0</td>
      <td>3</td>
      <td>...</td>
      <td>2019</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1300</td>
      <td>1.648,85</td>
      <td>2019-05-14 13:29:47.011108</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 23 columns</p>
</div>


