# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:42:14 2020

@author: Yohan Reyes
"""

# =============================================================================
# %% PATHS
# =============================================================================

PATH_to_save = 'D:/DataBases/'

# =============================================================================
# %% URLs
# =============================================================================

#url = "http://verduras.mexicored.com.mx/"
#url = "http://frutas-y-verduras.mexicored.com.mx/"
#url = 'http://verduras-congeladas.mexicored.com.mx/'
#url = 'http://cafe.mexicored.com.mx/'
#url = 'http://carne.mexicored.com.mx/'
#url = 'http://camaras-de-seguridad.mexicored.com.mx/'
url = 'http://componentes-electronicos.mexicored.com.mx/'


# =============================================================================
# %% Imports
# =============================================================================

import numpy as np
import pandas as pd
from threading import Thread
from threading import Timer
import gc,requests,json
from datetime import datetime
from bs4 import BeautifulSoup

import time
from numba import jit

import string

# =============================================================================
# %% Functions
# =============================================================================

def downloadPage(url,verbose):

    page = requests.get(url)
    print('Status Code: '+str(page.status_code))
    if verbose:
        print(page)

    return page


def sellerScrapeData(seller):
    links = seller.find_all('a')
    link = links[0].get('href')

    '''
    about = seller.find_all(class_="list-group-item list-group-activity")
    about = about[0]
    about = str(about)
    '''
    
    sub_page = downloadPage(link,True)
    sub_page = BeautifulSoup(sub_page.content, 'html.parser')
    
    contact = sub_page.find_all(class_="card-block")
    about = contact[0]
    about = about.find('p').getText()
    about = about.replace('\r\n', '.')
    
    contact = contact[1]
    contact = contact.find('address').getText()
    contact = contact.split('\n')
    contact_ = []
    
    title = sub_page.select('h1.title')[0].text.strip()

    for i0 in range(len(contact)):
        if len(contact[i0].strip())>1:
            contact_.append(contact[i0].strip())
    return title, contact_, about

def htmlParser(url):
    page = downloadPage(url,True)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    sellers = soup.find_all(class_="card provider featured")
    sellers_ = soup.find_all(class_="card provider")
    
    contacts = pd.DataFrame([],columns = ['Address','Colonia','ZIP','Municipality','Country','Phone1','Phone2','About'])
    
    street_address = []
    colonia = []
    ZIP_number = []
    municipality = []
    country = []
    phone1 = []
    phone2 = []
    
    abouts = []
    
    for seller in sellers:
        i0 = 0
        title, contact, about = sellerScrapeData(seller)
        if (len(contact[0]) == 5) and(contact[0].isdecimal()):
            ZIP_number.append(contact[0])
            street_address.append(None)
            i0 = 1
        else:
            street_address.append(contact[0])
            
    #    street_address = street_address.translate(str.maketrans('', '', string.punctuation))
        if (len(contact[1]) == 5) and(contact[2].isdecimal()):
            ZIP_number.append(contact[1 + i0])
            colonia.append(None)
            i0 = 1
        else:
            colonia.append(contact[1])
            
    #    colonia = colonia.translate(str.maketrans('', '', string.punctuation))
        if len(contact) > 6:
            ZIP_number.append(contact[2])
            
            municipality.append(contact[3])
        #    municipality = municipality.translate(str.maketrans('', '', string.punctuation))
            country.append(contact[4])
        #    country = country.translate(str.maketrans('', '', string.punctuation))
            phone1.append(contact[5])
            try:
                phone2.append(contact[6])
            except:
                phone2.append(None)
                
        elif len(contact) <= 6:
            
            municipality.append(contact[2])
        #    municipality = municipality.translate(str.maketrans('', '', string.punctuation))
            country.append(contact[3])
        #    country = country.translate(str.maketrans('', '', string.punctuation))
            phone1.append(contact[4])
            try:
                phone2.append(contact[5])
            except:
                phone2.append(None)
    
        abouts.append(about)
        
    
    for seller in sellers_:
        i0 = 0
        title, contact, about = sellerScrapeData(seller)
        if (len(contact[0]) == 5) and(contact[0].isdecimal()):
            ZIP_number.append(contact[0])
            street_address.append(None)
            i0 = 1
        else:
            street_address.append(contact[0])
            
    #    street_address = street_address.translate(str.maketrans('', '', string.punctuation))
        if (len(contact[1]) == 5) and(contact[1].isdecimal()):
            ZIP_number.append(contact[1 + i0])
            colonia.append(None)
            i0 = 1
        else:
            colonia.append(contact[1])
            
    #    colonia = colonia.translate(str.maketrans('', '', string.punctuation))
        if len(contact) > 6:
            ZIP_number.append(contact[2])
            
            municipality.append(contact[3])
        #    municipality = municipality.translate(str.maketrans('', '', string.punctuation))
            country.append(contact[4])
        #    country = country.translate(str.maketrans('', '', string.punctuation))
            phone1.append(contact[5])
            try:
                phone2.append(contact[6])
            except:
                phone2.append(None)
                
        elif len(contact) <= 6:
            
            municipality.append(contact[2])
        #    municipality = municipality.translate(str.maketrans('', '', string.punctuation))
            country.append(contact[3])
        #    country = country.translate(str.maketrans('', '', string.punctuation))
            phone1.append(contact[4])
            try:
                phone2.append(contact[5])
            except:
                phone2.append(None)
                
        abouts.append(about)
    
    contacts['Address'] = street_address
    contacts['Colonia'] = colonia
    contacts['ZIP'] = ZIP_number
    contacts['Municipality'] = municipality
    contacts['Country'] = country
    contacts['Phone1'] = phone1
    contacts['Phone2'] = phone2
    contacts['About'] = abouts
    
    return contacts

def htmlParser2(url):
    page = downloadPage(url,True)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    sellers = soup.find_all(class_="card provider featured")
    sellers_ = soup.find_all(class_="card provider")
    
    contacts = pd.DataFrame([],columns = ['Company Name','Address','About'])
    contact_ = []
    
    street_address = []
    colonia = []
    ZIP_number = []
    municipality = []
    country = []
    phone1 = []
    phone2 = []
    
    abouts = []
    titles = []
    
    for seller in sellers:
        i0 = 0
        title, contact, about = sellerScrapeData(seller)
        contact = '.'.join(contact)
        abouts.append(about)
        contact_.append(contact)
        titles.append(title)
    
    for seller in sellers_:
        i0 = 0
        title, contact, about = sellerScrapeData(seller)
        contact = '.'.join( contact )
        abouts.append(about)
        contact_.append(contact)
        titles.append(title)
    
    contacts['Company Name'] = titles
    contacts['Address'] = contact_
    contacts['About'] = abouts
    
    return contacts


def paginationNumbers(url):
    page = downloadPage(url,True)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    pagination = soup.find_all(class_ = "page-item")
    pagination.pop(0)
    pagination.pop(-1)
    
    pages = []
    
    for pagination_temp in pagination:
        temp = pagination_temp.find('a')
        temp = temp.get('href')
        pages.append(temp)
    
    page = downloadPage(pages[-1],True)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    pagination = soup.find_all(class_ = "page-item")
    pagination.pop(0)
    pagination.pop(-1)
    
    #pages = []
    
    while len(pagination) > 0:
        for pagination_temp in pagination:
            try:
                temp = pagination_temp.find('a')
                temp = temp.get('href')
                pages.append(temp)
            except:
                a = 0
        
        page = downloadPage(pages[-1],True)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        pagination = soup.find_all(class_ = "page-item")
        pagination.pop(0)
        pagination.pop(-1)
    
    return pages

# =============================================================================
# %% Settings
# =============================================================================





# =============================================================================
# %% Download Pages
# =============================================================================

pages = paginationNumbers(url)
contacts = htmlParser2(url)

for page in pages:
    contacts_ = htmlParser2(page)
    contacts = pd.concat( [contacts,contacts_], axis = 0)


# =============================================================================
# %% Save
# =============================================================================

name = url.split('//')[1]
name = name.split('.')[0]
name = name.replace('-','_')

contacts.to_csv(PATH_to_save+name+'.csv',index=False)

# =============================================================================
# %% END
# =============================================================================









