#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import random
import pandas as pd
import re
import sqlite3
import datetime
import multiprocessing
from pebble import ProcessPool
from concurrent.futures import TimeoutError



#######################################
# get already known Expose IDs from DB
#######################################

conn = sqlite3.connect("/home/cichons/immo_scraper/sqlite/realty.db")
c = conn.cursor()
try:
    c.execute("SELECT distinct id FROM realties;")
    x = c.fetchall()
    known_expose_ids = [i[0] for i in x]
except:
    known_expose_ids = []
conn.close()

print('Currently there are ', len(known_expose_ids), 'realties stored in DB.')



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


realty_meta_df, soup = scrape_meta_chunk(url,return_soup=True)
num_pages = len(soup.find_all('option'))
print('scraped', len(realty_meta_df), 'realties on first page')
print('found', num_pages, 'pages to scrape')


# In[3]:


#######################################
# Crawl next pages:
#######################################


realty_meta_urls = ['https://www.immobilienscout24.de/Suche/S-T/P-' + str(page) + '/Wohnung-Miete/Umkreissuche/Hamburg/-/1840/2621814/-/-/30' for page in range(2, num_pages + 1)]
realty_meta_df = realty_meta_df.append(parallelize_function_timeout(realty_meta_urls, scrape_meta_chunk))
#realty_meta_df = pd.concat(realty_meta, ignore_index=True)
print('Done')
 
    
    

    


# In[4]:


# drop known exposes:
#######################################



print('removing', len(realty_meta_df.loc[pd.to_numeric(realty_meta_df.id).isin(known_expose_ids)]), 'already scraped Exposes from list.')
realty_meta_df = realty_meta_df.loc[~pd.to_numeric(realty_meta_df.id).isin(known_expose_ids)] #realty_meta_df.loc[~realty_meta_df.id.isin(known_expose_ids)]
realty_meta_df = realty_meta_df.drop_duplicates()
realty_meta_df = realty_meta_df.reset_index(drop=True)
print(len(realty_meta_df), 'realties left.')




# In[5]:


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









# In[6]:


realty_meta_df['id'] = realty_meta_df['id'].astype(int)
realty_details_df['id'] = realty_details_df['id'].astype(int)

final_data = realty_meta_df.drop('scraped_ts',axis = 1).merge(realty_details_df, on='id', suffixes = ['_meta', '_details'])


# In[7]:


final_data.head()


# In[8]:


print(len(final_data))


# In[9]:


#######################################
# write to sqlite3
#######################################


print('Writing results to DB.')
conn = sqlite3.connect("/home/cichons/immo_scraper/sqlite/realty.db")

final_data.to_sql('realties', conn, if_exists ='append')

conn.close()
print('done.')

