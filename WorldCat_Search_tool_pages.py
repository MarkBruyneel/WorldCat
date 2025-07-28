# Publishers tool to be able to augment data with
# publisher data from WorldCat using the OCLC Discovery API
# Author: Mark Bruyneel
#
# Date: 2025-07-25
# Version: 1.0
# Created using Python version 3.10
#
# Re-use note: Make sure to change folder names that are relevant to your computer

import pandas as pd # version 2.2.3
import os
import re
import json
import shutil
import yaml
from oauthlib.oauth2 import BackendApplicationClient # version 3.2.2
from requests.auth import HTTPBasicAuth # version 2.31.0
from requests_oauthlib import OAuth2Session
import requests # version 2.31.0
# To catch errors, use the logger option from loguru
from loguru import logger #version 0.7.2
# Also needed to get the run time of the script
from datetime import datetime #version 5.5
import time
from pathlib import Path

# Show all data in screen
pd.set_option("display.max.columns", None)
# Create year and date variable for filenames etc.
today = datetime.now()
year = today.strftime("%Y")
runday = str(datetime.today().date())
runyear = str(datetime.today().year)

# Create a date + time for file logging
now = str(datetime.now())
nowt = time.time()

logger.add(r'U:\Werk\OWO\WC_Pages_Search_test.log', backtrace=True, diagnose=True, rotation="10 MB", retention="12 months")
@logger.catch()

def main():
    # Get configuration information to connect to WorldCat Search API
    with open(r'U:\Werk\OWO\AIP\WC_Search_config.yml', 'r') as stream:
        config = yaml.safe_load(stream)

    serviceURL = config.get('worldcat_api_url')
    token_url = config.get('token_url')
    # get a token
    # scope = ['wcapi:view_brief_bib']
    scope = ['wcapi:view_bib']
    auth = HTTPBasicAuth(config.get('key'), config.get('secret'))
    client = BackendApplicationClient(client_id=config.get('key'), scope=scope)
    wskey = OAuth2Session(client=client)

    # create download folder if it doesn't exist
    Path("U:\Werk\OWO\WC_pages_test").mkdir(parents=True, exist_ok=True)

    # create a backup folder with the json files from last time and do the backup
    Path(f"U:\Werk\OWO\WC_pages_test\\backup_{runday}").mkdir(parents=True, exist_ok=True)
    sourcepath = (f"U:\Werk\OWO\WC_pages_test")
    sourcefiles = os.listdir(sourcepath)
    destinationpath = (f"U:\Werk\OWO\WC_pages_test\\backup_{runday}")
    for file in sourcefiles:
        if file.endswith(".json"):
            shutil.move(os.path.join(sourcepath, file), os.path.join(destinationpath, file))

    # Provide the file name and location for which to look up data
    csvfile = input('Please provide the location and name of the tab-delimited file.\nExample: C:\\temp\\file_data.csv or .txt file\n')
    Pubs = pd.read_csv(f'{csvfile}', sep='\t')

    # Remove added .0 from each field where it was added by importing
    Pubs['ISBN1'] = Pubs['ISBN1'].astype(str)
    Pubs['ISBN1'] = Pubs['ISBN1'].str.replace('.0', '')
    Pubs['Search_ISBN'] = Pubs['Search_ISBN'].astype(str)
    Pubs['Search_ISBN'] = Pubs['Search_ISBN'].str.replace('.0', '')
    Pubs['Pub_year'] = Pubs['Pub_year'].astype(str)
    Pubs['Pub_year'] = Pubs['Pub_year'].str.replace('.0', '')

    # Create a list of OCLC numbers to look up data for
    OCLC_list_original = Pubs['OCLC_nr'].tolist()
    length_list = len(OCLC_list_original)
    logger.debug(f'Nr. of OCLC numbers in the list: {length_list} \n')

    # Create an output folder if it doesn't exist
    Path('U:\Werk\OWO\Output').mkdir(parents=True, exist_ok=True)

    # Create Dataframe for information from WorldCat
    Pages_Book_Table = pd.DataFrame()
    Urls_Table = pd.DataFrame()

    # Get WorldCat Records for each ISBN in the list
    listitem = 0
    while listitem < length_list:
        oclcNumber = []
        PhysicalAtt = []
        DAAL = []
        DAMS = []
        oclcNo = []
        try:
            token = wskey.fetch_token(token_url=token_url, auth=auth)
            try:
                logger.debug(f'Retrieving data for Oclc number {OCLC_list_original[listitem]}, {listitem + 1} of a total of {length_list})')
                r = wskey.get(serviceURL + "/bibs?q=" + str(OCLC_list_original[listitem]) + "&groupRelatedEditions=false&openAccess&showHoldingsIndicators=true")
                r.raise_for_status()
                result = r.json()
                # keep json as backup
                with open(f'U:\Werk\OWO\WC_pages_test/{OCLC_list_original[listitem]}.json', 'w') as f:
                    f.write(json.dumps(result))
                # To get all data for the OCLC record. The numberOfRecords item in the json is unreliable!
                recno = len(result['bibRecords'])
                if recno > 1:
                    recstart = 0
                    while recstart < recno:
                        try:
                            # Test if the Physical description  field is missing
                            phd = result['bibRecords'][recstart]['description']['physicalDescription']
                        except KeyError:
                            phd = "None"
                        try:
                            # Test if the Oclc number field is missing
                            onr = int(result['bibRecords'][recstart]['identifier']['oclcNumber'])
                        except KeyError:
                            onr = "None"
                        PhysicalAtt.append(phd)
                        oclcNumber.append(onr)
                        # Test is there is any Digital Access And Locations specified
                        if 'digitalAccessAndLocations' in result['bibRecords'][recstart]:
                            unrs = len(result['bibRecords'][recstart]['digitalAccessAndLocations'])
                            if unrs < 2:
                                daali = result['bibRecords'][recstart]['digitalAccessAndLocations'][0]['uri']
                                try:
                                    DAMSi = result['bibRecords'][recstart]['digitalAccessAndLocations'][0]['materialSpecified']
                                except:
                                    DAMSi = "None"
                                onru = int(result['bibRecords'][recstart]['identifier']['oclcNumber'])
                                DAAL.append(daali)
                                DAMS.append(DAMSi)
                                oclcNo.append(onru)
                            else:
                                url_item = 0
                                while url_item < unrs:
                                    daali = result['bibRecords'][recstart]['digitalAccessAndLocations'][url_item]['uri']
                                    try:
                                        DAMSi = result['bibRecords'][recstart]['digitalAccessAndLocations'][url_item][
                                            'materialSpecified']
                                    except:
                                        DAMSi = "None"
                                    onru = int(result['bibRecords'][recstart]['identifier']['oclcNumber'])
                                    DAAL.append(daali)
                                    DAMS.append(DAMSi)
                                    oclcNo.append(onru)
                                    url_item = url_item + 1
                        else:
                            pass
                        recstart = recstart + 1
                else:
                    try:
                        # Test if the Physical description  field is missing
                        phd = result['bibRecords'][0]['description']['physicalDescription']
                    except KeyError:
                        phd = "None"
                    try:
                        # Test if the Oclc number field is missing
                        onr = int(result['bibRecords'][0]['identifier']['oclcNumber'])
                    except KeyError:
                        onr = "None"
                    # Test is there is any Digital Access And Locations specified
                    if 'digitalAccessAndLocations' in result['bibRecords'][0]:
                        unrs = len(result['bibRecords'][0]['digitalAccessAndLocations'])
                        if unrs < 2:
                            daali = result['bibRecords'][0]['digitalAccessAndLocations'][0]['uri']
                            try:
                                DAMSi = result['bibRecords'][0]['digitalAccessAndLocations'][0]['materialSpecified']
                            except:
                                DAMSi = "None"
                            onru = result['bibRecords'][0]['identifier']['oclcNumber']
                            DAAL.append(daali)
                            DAMS.append(DAMSi)
                            oclcNo.append(onru)
                        else:
                            url_item = 0
                            while url_item < unrs:
                                daali = result['bibRecords'][0]['digitalAccessAndLocations'][url_item]['uri']
                                try:
                                    DAMSi = result['bibRecords'][0]['digitalAccessAndLocations'][url_item]['materialSpecified']
                                except:
                                    DAMSi = "None"
                                onru = result['bibRecords'][0]['identifier']['oclcNumber']
                                DAAL.append(daali)
                                DAMS.append(DAMSi)
                                oclcNo.append(onru)
                                url_item = url_item + 1
                    else:
                        pass
                    PhysicalAtt.append(phd)
                    oclcNumber.append(onr)
            except requests.exceptions.HTTPError as err:
                print(err)
        except BaseException as err:
            print(err)
        oclc_Book_Table = {'OCLC_nr': oclcNumber}
        book_table = pd.DataFrame(oclc_Book_Table)
        book_table['Physical_Attributes'] = PhysicalAtt
        Pages_Book_Table = pd.concat([Pages_Book_Table, book_table], ignore_index=True)

        wc_urls_table = {'OCLC_nr': oclcNo}
        urls_table = pd.DataFrame(wc_urls_table)
        urls_table['materialSpecified'] = DAMS
        urls_table['uri'] = DAAL
        Urls_Table = pd.concat([Urls_Table, urls_table], ignore_index=True)

        listitem = listitem + 1

    # Export end result
    Pages_Book_Table.to_csv(f'U:\Werk\OWO\WC_pages_test\WorldCat_Book_attributes_list_' + runday + '.txt', sep='\t', encoding='utf-8')
    # Remove .0 from column with OCLC numbers
    # Urls_Table['OCLC_nr'] = Urls_Table['OCLC_nr'].str.replace('.0', '')
    Urls_Table.to_csv(f'U:\Werk\OWO\WC_pages_test\WorldCat_Book_attributes_urls_list_' + runday + '.txt', sep='\t', encoding='utf-8')

    # Merge the download table with the original data file
    Final = pd.merge(Pubs, Pages_Book_Table, how='outer', on=['OCLC_nr'])
    # drop rows where value in column is null
    Final = Final.dropna(subset=['Holding'])
    Final.drop(Final.columns[Final.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
    NewFinal = Final.reset_index()
    NewFinal.drop(NewFinal.columns[NewFinal.columns.str.contains('index', case=False)], axis=1, inplace=True)

    # Merge the download table with the urls_table
    Finalurls = pd.merge(NewFinal, Urls_Table, how='outer', on=['OCLC_nr'])

    Finalurls.to_csv(f'U:\Werk\OWO\WC_pages_test\WorldCat_Books_&_Pages_&_urls_list_' + runday + '.txt', sep='\t', encoding='utf-8')

    # Logging of script run:
    end = str(datetime.now())
    logger.debug('Processing started at: ' + now)
    logger.debug('Processing completed at: ' + end)
    duration_s = (round((time.time() - nowt), 2))
    if duration_s > 3600:
        duration = str(duration_s / 3600)
        logger.debug('Search took: ' + duration + ' hours.')
    elif duration_s > 60:
        duration = str(duration_s / 60)
        logger.debug('Search took: ' + duration + ' minutes.')
    else:
        duration = str(duration_s)
        logger.debug('Search took: ' + duration + ' seconds.')

if __name__ == "__main__":
    main()