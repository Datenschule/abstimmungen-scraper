# coding: utf-8
import os
import logging
import urllib
from bs4 import BeautifulSoup
import unicodedata
import string
import shutil
import re
import xlrd
import pandas
import pandas.io.sql as pd_sql
import sqlite3 as sql

log = logging.getLogger(__name__)
DATA_PATH = os.environ.get('DATA_PATH', 'data')
OUT_PATH = ''

BASE_URL = 'https://www.bundestag.de'
INDEX_URL = BASE_URL + '/ajax/filterlist/de/parlament/plenum/abstimmung/liste/-/462112?limit=30&offset='

votes = []
votes_complete = []

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = removeNonAscii(value)
    value = re.sub(r'[./;,]', '', value)
    return value

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

if os.path.exists('data/excel'):
    shutil.rmtree('data/excel/')
os.makedirs('data/excel')

offset = 0

while True:
    r = urllib.request.urlopen('{}{}'.format(INDEX_URL, offset))
    soup = BeautifulSoup(r, "html5lib")
    rows = soup.select('tbody tr')
    if len(rows) > 0:
        print('Scraping and downloading Excel files for page {}'.format(offset / 30))

        for index, row in enumerate(rows):
            vote = {}
            title = row.select_one("td[data-th='Dokument'] p").get_text().split(':')

            if (len(title) > 1):
                vote['date'] = title[0].strip()
                vote['title'] = title[1].strip()
            elif (len(title) > 0):
                vote['title'] = title[0].strip()
                print("WARNING: no date found for item {}".format(offset + index))
            else:
                print("ERROR: no title for item {}".format(offset + index))

            link_elem_xls = row.select_one("td[data-th='Dokument'] li:nth-of-type(2) a")
            if (link_elem_xls):
                vote['link_xls'] = link_elem_xls['href']
                path = 'data/excel/{}-{}.xls'.format(index, slugify(vote['title']))
                urllib.request.urlretrieve(BASE_URL + vote['link_xls'], path)
                vote['path_xls'] = path
                votes.append(vote)
            else:
                print("ERROR: No Excel file found, not adding vote #{}".format(offset + index))

    else:
        break

    offset += 30

print('Merging...')
votes_individual = []

for vote in votes:
    # print("processing file " + vote['path_xls'])
    df = pandas.read_excel(vote['path_xls'])
    for index,row in df.iterrows():
        vote_parlamentary = vote.copy()
        vote_parlamentary['wahlperiode'] = row['Wahlperiode']
        vote_parlamentary['sitzungsnummer'] = row['Sitzungnr']
        vote_parlamentary['abstimmnummer'] = row['Abstimmnr']
        vote_parlamentary['id'] = "{}-{}-{}".format(row['Wahlperiode'], row['Sitzungnr'], row['Abstimmnr'])
        vote_parlamentary['fraktion'] = row['Fraktion/Gruppe']
        vote_parlamentary['name'] = row['Name']
        vote_parlamentary['vorname'] = row['Vorname']
        vote_parlamentary['titel'] = row['Titel']
        vote_parlamentary['ja'] = row['ja']
        vote_parlamentary['nein'] = row['nein']
        vote_parlamentary['Enthaltung'] = row['Enthaltung']
        vote_parlamentary['ungueltig'] = row['ungültig']
        vote_parlamentary['nichtabgegeben'] = row['nichtabgegeben']
        vote_parlamentary['Bezeichnung'] = row['Bezeichnung']
        # print(vote_parlamentary)
        votes_individual.append(vote_parlamentary)

df_votes = pandas.DataFrame(votes_individual)
print("writing CSV ...")
df_votes.to_csv("data/votes.csv")
# con = sql.connect("data/out.db")
# pd_sql.write_frame(df, "tbldata2", con)
print("finished")


