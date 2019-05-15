#!/usr/bin/env python
# coding: utf-8

# In[2]:


from bs4 import BeautifulSoup
import time
import requests
from fake_useragent import UserAgent
import random
import pandas as pd
import re
from tqdm import tqdm
import sqlite3
import datetime
import numpy as np
import multiprocessing




#######################################
# get already known Expose IDs from DB
#######################################

conn = sqlite3.connect("sqlite/realty.db")
c = conn.cursor()
try:
    c.execute("SELECT distinct expose_id FROM realty_meta;")
    x = c.fetchall()
    known_expose_ids = [i[0] for i in x]
except:
    known_expose_ids = []
conn.close()




#######################################
# define helper functions
#######################################

def get_proxies():
    from lxml.html import fromstring
    import requests

    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies_http = []
    proxies_https = []
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[7]/text()') == ['no']:
            if i.xpath('.//td[5]/text()')==['elite proxy']:# and i.xpath('.//td[3]/text()') == ['DE']:
                #Grabbing IP and corresponding PORT
                http = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies_http.append(http)
        if i.xpath('.//td[7]/text()') == ['yes']:
            if i.xpath('.//td[5]/text()')==['elite proxy']:# and i.xpath('.//td[3]/text()') == ['DE']:
                #Grabbing IP and corresponding PORT
                https = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies_https.append(https)

    return proxies_http, proxies_https




def get_soup(ua, url, proxies, echo=False):
    bad_proxies = []
    headers = {'User-Agent': ua.random}
    proxy = proxies[random.randrange(len(proxies))]
    if echo:
        print('scraping:', url)
        
    try:
        r  = requests.get(url, headers=headers, proxies=proxy)

    except requests.exceptions.ProxyError:
        if echo:
            print('trying different Proxy')
            
        bad_proxies.append(proxy)
        proxy = proxies[random.randrange(len(proxies))]
        try:
            r  = requests.get(url, headers=headers, proxies=proxy)
        except:
            try:
                r  = requests.get(url, headers=headers)
            except:
                pass
    except:
        try:
            r  = requests.get(url, headers=headers)
        except:
            pass

    data = r.text
    soup = BeautifulSoup(data, "lxml")
    return soup, bad_proxies




def parallelize_function(url_list, func):
    
    start_ts = datetime.datetime.now()
  
    num_cores = multiprocessing.cpu_count()-1  #leave one free to not freeze machine
    print('processinc function on', num_cores, 'cores...')
    pool = multiprocessing.Pool(num_cores)
    results = pool.map(func, url_list)
    pool.close()
    pool.join()
    
    print('Finished parallell processing after', datetime.datetime.now() - start_ts, 'seconds')
    
    return results


"""
    expose_list = list(set([link.get('href').replace('/expose/', '') for link in soup.find_all('a') if "/expose/" in link.get('href')]))
    adresses = [location.getText() for location in soup.findAll("button", {"class": "button-link link-internal result-list-entry__map-link", "title": "Auf der Karte anzeigen"})]

    city_county = [add.split(', ')[-1] for add in adresses]
    city_quarter = [add.split(', ')[-2] for add in adresses]
    street = [add.split(', ')[-3] if len(add.split(', ')) == 3 else ''  for add in adresses]
"""

def scrape_meta_chunk(url, return_soup=False):
    print(url)
    # scrape next url
    soup, bad_proxies = get_soup(ua, url, proxies)

    # remove bad proxies from list
    if len(bad_proxies) > 0:
        try:
            proxies.remove(bad_proxies)
        except:
            pass

    
    try:
        
        data = pd.DataFrame([[location['data-result-id'], location.getText().split(', ')[-1], location.getText().split(', ')[-2], location.getText().split(', ')[-3] if len(location.getText().split(', ')) == 3 else ''] for location in soup.findAll("button", {"class": "button-link link-internal result-list-entry__map-link", "title": "Auf der Karte anzeigen"})], columns=['id','city_county', 'city_quarter', 'street' ])
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
    print(url)
    soup, bad_proxies = get_soup(ua, url, proxies)

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

    if len(bad_proxies) > 0:
        try:
            proxies.remove(bad_proxies)
        except:
            pass
        
    results = pd.DataFrame({
    'expose_id': expose_id,
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




proxies_http, proxies_https = get_proxies()
proxies = [{'http': http} for http in proxies_http] + [{'https': https} for https in proxies_https]

ua = UserAgent(fallback='Mozilla/5.0 (compatible; Googlebot/2.1; ')


# In[2]:


#######################################
# scrape proxies and User Agents lists
#######################################

proxies_http, proxies_https = get_proxies()
proxies = [{'http': http} for http in proxies_http] + [{'https': https} for https in proxies_https]

ua = UserAgent(fallback='Mozilla/5.0 (compatible; Googlebot/2.1; ')





#######################################
# Crawl first page:
#######################################
url = 'https://www.immobilienscout24.de/Suche/S-T/P-1/Wohnung-Miete/Umkreissuche/Hamburg/-/1840/2621814/-/-/30'


realty_meta, soup = scrape_meta_chunk(url,return_soup=True)
num_pages = len(soup.find_all('option'))
print('scraped', len(realty_meta), 'realties on first page')
print('found', num_pages, 'pages to scrape')


# In[3]:


#######################################
# Crawl next pages:
#######################################


realty_meta_urls = ['https://www.immobilienscout24.de/Suche/S-T/P-' + str(page) + '/Wohnung-Miete/Umkreissuche/Hamburg/-/1840/2621814/-/-/30' for page in range(2, num_pages + 1)]
realty_meta = parallelize_function(realty_meta_urls, scrape_meta_chunk)
realty_meta_df = pd.concat(realty_meta, ignore_index=True)
print('Done')
 
    
    

    
    
# drop known exposes:
#######################################


print('removing', len(realty_meta_df) - len(realty_meta_df.loc[~realty_meta_df.id.isin(known_expose_ids)]), 'already scraped Exposes from list.')
realty_meta_df = realty_meta_df.loc[~realty_meta_df.id.isin(known_expose_ids)]
realty_meta_df = realty_meta_df.drop_duplicates()
realty_meta_df = realty_meta_df.reset_index(drop=True)





# In[4]:


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
realty_details = parallelize_function(realties, scrape_details_chunk)
realty_details_df = pd.concat(realty_details, ignore_index=True)

print('scraped', len(realty_details_df), 'realties.')









# In[ ]:


realty_details_df_backup = realty_details_df


# In[ ]:


# convert data types
#######################################
realty_details_df.square_m = realty_details_df.square_m.fillna('999').apply(lambda x: re.sub('[^0-9]','', str(x).replace(',','.')))
realty_details_df.num_rooms = realty_details_df.num_rooms.fillna('999').apply(lambda x: x.replace(',','.'))
realty_details_df.year_built = realty_details_df.year_built.fillna('999').apply(lambda x: re.sub('[^0-9]','',str(x)))
realty_details_df.last_refurb = realty_details_df.last_refurb.fillna('999').apply(lambda x: re.sub('[^0-9]','',str(x)))
realty_details_df.energy_consumption = realty_details_df.energy_consumption.fillna('999').apply(lambda x: re.sub('[^0-9]','',x.replace(',','.')))
realty_details_df.net_rent = realty_details_df.net_rent.fillna('999').apply(lambda x: re.sub('[^0-9]','',x.replace(',','.')))
realty_details_df.gross_rent = realty_details_df.gross_rent.fillna('999').apply(lambda x: re.sub('[^0-9]','',x.replace(',','.')))
realty_details_df.scraped_ts = realty_details_df.fillna('999').scraped_ts

realty_meta_df.scraped_ts = pd.to_datetime(realty_meta_df.scraped_ts)


#cols = realty_details_df.columns.drop('expose_id')
#realty_details_df[cols] = realty_details_df[cols].apply(pd.to_numeric, errors='ignore')


# In[ ]:


realty_details_df.columns.drop('expose_id')


# In[ ]:


cols = ['expose_id', 'square_m', 'storage_m',       'num_rooms', 'num_bedrooms', 'num_baths', 'num_carparks', 'year_built', 'energy_consumption','net_rent', 'gross_rent']
realty_details_df[cols] = realty_details_df[cols].apply(pd.to_numeric, errors='ignore')
realty_details_df.scraped_ts = pd.to_datetime(realty_details_df.scraped_ts)


# In[ ]:


realty_details_df.dtypes


# In[ ]:


#######################################
# write to sqlite3
#######################################


print('Writing results to DB.')
conn = sqlite3.connect("sqlite/realty.db")


realty_meta_df.to_sql('realty_meta', conn, if_exists ='append')
realty_details_df.to_sql('realty_details', conn, if_exists ='append')

conn.close()
print('done.')
