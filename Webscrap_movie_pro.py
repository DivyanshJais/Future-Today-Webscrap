import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import logging
import os
import re
from openpyxl import load_workbook
import time
import random

IMDB_FILE="Final IDS.xlsx"
DATA_FILE="Movie Model 3 - IMDb Data Scraping using IMDb Pro Links.xlsx"
SUMMARY_FILE="Movie Model 3 - Movies Summary and Synopsis.xlsx"
LANGUAGE_FILE="Movie Model 3 - Language.xlsx"
CHECKPOINT_TXT="Checkpoint.txt"
BATCH_SIZE = 20

movie_imdb=pd.read_excel(IMDB_FILE)
movie_imdb=movie_imdb['IMDb ID'].tolist()

if os.path.exists(DATA_FILE):
    print("Scraping File exists!")
    final_df = pd.read_excel(DATA_FILE)
else:
    print("Creating Scraping File")
    final_df=pd.DataFrame(columns=[
        'IMDb_ID','Original_URL','Movie_Meter','Production_Company','DIrector','Year','US_Release_Dates',
        'Star1','StarMeter1','Star2','StarMeter2','Star3','StarMeter3','Star4','StarMeter4','Star5',
        'StarMeter5','Star6','StarMeter6','IMDB_Rating','IMDBVOTES','Text','Genre','Distributor',
        'Budget','Opening_weekend','Gross_US_Canada','Gross_World','Awards','Age_base_rating','Runtime'
    ])

if os.path.exists(SUMMARY_FILE):
    print("Summary file exists")
    summary_df = pd.read_excel(SUMMARY_FILE)
else:
    print("creating summary file")
    summary_df=pd.DataFrame(columns=['IMDb_ID','Original_URL','Title','Summary','Synopsis'])

if os.path.exists(LANGUAGE_FILE):
    print("Language file exists")
    summary_df = pd.read_excel(LANGUAGE_FILE)
else:
    print("creating Language file")
    language_df=pd.DataFrame(columns=['IMDb_ID','Language'])

file='Scrap_error.log'
logging.basicConfig(
    filename=file, filemode='a', level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
    )

def load_checkpoint(path):
    if os.path.exists(path):
        with open(path) as f:
            return set(line.strip() for line in f if line.strip())
    return set()

completed_ids=load_checkpoint(CHECKPOINT_TXT)

headers={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Accept-Language":"en-US,en;q=0.9",
    "Cookie":'session-id=147-5177312-4071448; ubid-main=133-1142720-7649640; _gcl_au=1.1.1929078785.1766732901; uu=eyJpZCI6InV1ZDM3ODUwMmYyZjYzNDViOWExY2IiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfX0=; _au_1d=AU1D-0100-001767688437-WK1S35GE-OHRR; _ga=GA1.1.730489173.1767699946; __gads=ID=960a3cd0b73d5922:T=1767688426:RT=1767701991:S=ALNI_MY26HZEYRj6jStJckXyzM9qIR7Xeg; __gpi=UID=000012ae0ceb2927:T=1767688426:RT=1767701991:S=ALNI_MaMCsehNft-j4qNYFLaChCtB-wENQ; __eoi=ID=a8f967f59b9977b2:T=1767688426:RT=1767701991:S=AA-Afjbs3Q6dfCvGj8vio5taEG5O; _ga_FVWZ0RM4DH=GS2.1.s1767704054$o2$g0$t1767704054$j60$l0$h0; x-main=JR?yFucooj@zoGIBUgnoUvuuA7SH1ecqtIjbRXKzAiSFEebQtEw@MNxlqAkXnxWS; at-main=Atza|gQBK0KtcAwEBAM-fFGEqrreZ-5mrvCFAa_HOxU18fDWihh_0Vj2-oUzeuUq_QrTFUqUxbHQ4HsGBEYBUkuZNSbIFkwyGJMab7KvXqZgIhluCD-19q7Pn5SjMRVpZrHl4gIy8HJ6juxY3M7xjvXNXqh4rg9fFR7Nb4WAEYtTbEs5sFGww_lFxq2SFuY-a8yftmPCWf2gHWw9oJHDj7wfPPnRGHFYVQFefNQL1ZcyR36k0ENWTmYnGUPbzi3qj0UskW9vKlHx4qVQSf1VAVCmCCGmyNIuZPGqBOrdONnH8JGm6E5dWgSO2a1X3DIS0JQc86dt7hbJtTZqO_lDPxndYSLUUu3SknuICpvemzA; sess-at-main=Ze52xR6KMPkNaN3elKGKcqQFYaUBmJtm/8P53CZEupw=; session-id-time=2082787201l; ci=eyJhZ2VTaWduYWwiOiJBRFVMVCIsImlzR2RwciI6ZmFsc2V9; csm-hit=tb:QWAYQ0YFA6JMZMVJ35V4+s-55Z5VT20ZEW37CJ58ZXC|1769495216593&t:1769495216593&adb:adblk_no; session-token=WpqGotn1acuvHxXNh/xQf1YsXZU3k1BUw7pjYCgNoSEvxiJVxnN+k7KbV6H0ENrFLWFYkGtH+m/hkwCu56Artg47zRlDA6ucLhsXp+EWaxUCX0URYVc0BC02pI/WNBMKu8JFN6yHHBBhfcJyuxBVMDk3OQ3KjzDMzb7ptBnBsUd84hHrV4r6+2/rM1FdG2wM25iUfyt0S7ZjIiMWzvBxqwor1upv0GQ6CQzG4tG0Tv9e40ct3b8OrDs8W+OsKShCfBpX/iV5H5ons1cnWkj0XtDrQUtwHtxy3IfXZFibiplMcpBrie4B5EcCdE5lUOJY5wPtb1wqSAm8gbgWsDhCMAVu1Jah6c0Uy49jEpZlGNQObbEfpTLI+mhh+2b1VR5u'
    }

# headers2={
#     "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
#     "Accept-Language":"en-US,en;q=0.9"
#     }

def extract(tag,strip=True):
    return tag.get_text(strip=strip) if tag else None

def mark_completed(id):
    with open(CHECKPOINT_TXT,"a") as f:
        f.write(id + "\n")

final_df.set_index('IMDb_ID', inplace=True, drop=False)
summary_df.set_index('IMDb_ID', inplace=True, drop=False)
language_df.set_index('IMDb_ID', inplace=True, drop=False)
buffer_final=[]
buffer_summary=[]
buffer_language=[]

def flush_to_disk():
    global final_df, summary_df, language_df, buffer_final, buffer_summary, buffer_language

    buffer_df = pd.DataFrame(buffer_final)
    buffer_df.set_index('IMDb_ID', inplace=True, drop=False)

    buffer_summary_df = pd.DataFrame(buffer_summary)
    buffer_summary_df.set_index('IMDb_ID', inplace=True, drop=False)

    buffer_language_df=pd.DataFrame(buffer_language)
    buffer_language_df.set_index('IMDb_ID', inplace=True, drop=False)

    final_df=pd.concat(
        [final_df[~final_df.index.isin(buffer_df.index)], buffer_df]
    )
    summary_df=pd.concat(
        [summary_df[~summary_df.index.isin(buffer_summary_df.index)], buffer_summary_df]
    )
    language_df=pd.concat(
        [language_df[~language_df.index.isin(buffer_language_df.index)], buffer_language_df]
    )

    final_df.to_excel(DATA_FILE, index=False)
    summary_df.to_excel(SUMMARY_FILE, index=False)
    language_df.to_excel(LANGUAGE_FILE, index=False)

    buffer_final.clear()
    buffer_summary.clear()
    buffer_language.clear()


for id in movie_imdb:
    if id in completed_ids:
        continue
    
    logging.info(f"Starting IMDb ID: {id}")
    try:
        base_url='https://pro.imdb.com/title/{}'
        #base_url2='https://pro.imdb.com/title/{}/details'
        url=base_url.format(id)
        url2=f'https://pro.imdb.com/title/{id}/details'
        response=requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup=BeautifulSoup(response.text, "html.parser")
        row={
            'IMDb_ID':id,'Original_URL':str(url),'Movie_Meter':None,'Production_Company':None,
            'DIrector':None,'Year':None,'US_Release_Dates':None,'Star1':None,'StarMeter1':None,
            'Star2':None,'StarMeter2':None,'Star3':None,'StarMeter3':None,'Star4':None,
            'StarMeter4':None,'Star5':None,'StarMeter5':None,'Star6':None,'StarMeter6':None,
            'IMDB_Rating':None,'IMDBVOTES':None,'Text':None,'Genre':None,'Distributor':None,
            'Budget':None,'Opening_weekend':None,'Gross_US_Canada':None,'Gross_World':None,
            'Awards':None,'Age_base_rating':None,'Runtime':None
        }
        row2={
            'IMDb_ID': id,'Original_URL':url2, 'Title': None, 'Summary': None, 'Synopsis': None
        }
        row3={
            'IMDb_ID': id, 'Language': None
        }
        #Title
        title=soup.find('span', class_='a-size-extra-large')
        row['Text']=extract(title)
        row2['Title']=extract(title)
        
        #Genre
        genre=soup.find('span', id='genres')
        row['Genre']=extract(genre)

        #Awards, Age-base-rating, Runtime
        award=soup.find('span', class_='awards_summary_text')
        row['Awards']=extract(award, False)
        age_base=soup.find('span', id='certificate')
        row['Age_base_rating']=extract(age_base)
        runtime=soup.find('span',id='running_time')
        row['Runtime']=extract(runtime)

        #Movie-meter
        meter=soup.find('div', id='ranking_graph_container')
        meter_text=None
        if meter:
            movie_meter=meter.find('span', class_='a-size-medium aok-align-center')
            meter_text = extract(movie_meter)
        if meter_text and re.fullmatch(r'[\d,]+', meter_text):
            row['Movie_Meter'] = int(meter_text.replace(',', ''))
        else:
            row['Movie_Meter'] = None

        #Release Year
        release_year=soup.find('a', string=re.compile(r'\b[A-Za-z]{3}\s\d{1,2},\s\d{4}\b'))
        date_text=extract(release_year, False)
        row['US_Release_Dates']=(pd.to_datetime(date_text, errors='coerce').date() if date_text else None)
        row['Year']= int(date_text[-4:]) if date_text else None

        #Director
        director_block=soup.find('div', id='director_summary')
        if director_block:
            row['DIrector']=extract(director_block.find('a'))
        else:
            directors_block=soup.find('div', id='directors_summary')
            if directors_block:
                directors=[extract(d) for d in directors_block.find_all('a')]
                row['DIrector']=",".join(directors) if directors else None

        #Rating
        rating_block = soup.find('div', id='rating_breakdown')
        if rating_block:
            spans = rating_block.find_all('span')

            # IMDb Rating: strictly numeric like 7.8, 6.0, 10
            for s in spans:
                text = extract(s)
                if not text:
                    continue

                # Accept only valid numeric ratings
                if re.fullmatch(r'\d{1,2}(\.\d)?', text.strip()):
                    row['IMDB_Rating'] = float(text)
                    break

            # IMDb Votes: explicitly look for "votes"
            for s in spans:
                text = extract(s)
                if not text:
                    continue

                m = re.search(r'([\d,]+)\s+votes', text.lower())
                if m:
                    row['IMDBVOTES'] = int(m.group(1).replace(',', ''))
                    break

        #Box Office
        box=soup.find('div', id='box_office_summary')
        box_office=box.find_all('div', class_='a-column a-span5 a-text-right a-span-last') if box else []
        def money(i):
            try:
                return int(extract(box_office[i]).replace('$','').replace(',',''))
            except:
                return None
        
        row['Budget']=money(0)
        row['Opening_weekend']=money(1)
        row['Gross_US_Canada']=money(2)
        row['Gross_World']=money(3)

        #Contacts
        contacts_block = soup.find('div', id='contacts')
        contacts = contacts_block.find_all('div', class_='a-column a-span12') if contacts_block else []
        if len(contacts)==2:
            row['Production_Company']=extract(contacts[0])
            row['Distributor']=extract(contacts[1])

        #Stars
        cast=soup.find('table', id='title_cast_sortable_table')
        rows=cast.find_all('tr', attrs={'data-cast-listing-index': True}) if cast else []
        stars=[]
        for tr in rows:
            tds=tr.find_all('td')
            name=extract(tds[0].find('span',class_='a-size-base-plus'))
            rank=extract(tds[1])
            try:
                rank=int(rank.replace(',',''))
            except ValueError:
                continue
            stars.append((name,rank))
        top_stars=sorted(stars, key=lambda x: x[1])
        for i in range(6):
            if i<len(top_stars):
                row[f'Star{i+1}']=top_stars[i][0]
                row[f'StarMeter{i+1}']=top_stars[i][1]
            else:
                row[f'Star{i+1}']=None
                row[f'StarMeter{i+1}']=None
        
        #Summary part
        html = requests.get(url2, headers=headers, timeout=15)
        html.raise_for_status()
        soup2=BeautifulSoup(html.text, "html.parser")
        summ=soup2.find('div', id='plot_summaries')
        summ_block=summ.find_all('div', class_='a-section a-spacing-medium') if summ else []
        summaries=[]
        for block in summ_block:
            summaries.append(extract(block))
        row2['Summary']= "\n".join(summaries) if summaries else None

        #Synopsis
        syn=soup2.find('div', id='synopsis')
        synopsis=syn.find('div', class_='a-expander-content') if syn else None
        row2['Synopsis']=extract(synopsis)

        #Language
        lang=soup2.find('div', id='release_details')
        lang_items=lang.find_all('tr', class_='release_details_item') if lang else []
        if len(lang_items)==2:
            language=lang_items[1].find('td', class_='a-color-secondary')

        row3['Language']=extract(language)

        buffer_final.append(row)
        buffer_summary.append(row2)
        buffer_language.append(row3)
        mark_completed(id)
        logging.info(f"Completed IMDb ID: {id}")
        print('Run Succesfully')

    except Exception as e:
        logging.error(f"Error scraping IMDb ID: {id}", exc_info=True)
        print(e)
        continue
    time.sleep(random.uniform(1, 3))
    if len(buffer_final)>= BATCH_SIZE:
        flush_to_disk()

if buffer_final:
    flush_to_disk()

all_ids = set(movie_imdb)
completed_ids = load_checkpoint(CHECKPOINT_TXT)

if completed_ids == all_ids:
    os.remove(CHECKPOINT_TXT)
    print("All IMDb IDs scraped successfully. Checkpoint removed.")
else:
    missing = all_ids - completed_ids
    print(f"{len(missing)} IMDb IDs not scraped.")
    print(missing)


print(final_df)