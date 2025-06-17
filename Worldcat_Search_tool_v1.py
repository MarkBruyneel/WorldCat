# Publishers tool to be able to augment AIP data from Canvas with
# publisher data from WorldCat using the OCLC Discovery API
# Author: Mark Bruyneel
#
# Date: 2025-04-30
# Version: 1.2
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

logger.add(r'U:\Werk\OWO\AIP\AIP_WC_Publisher_Search_test.log', backtrace=True, diagnose=True, rotation="10 MB", retention="12 months")
@logger.catch()

def main():
    # Get configuration information to connect to WorldCat Search API
    with open('U:\Werk\OWO\AIP\WC_Search_config.yml', 'r') as stream:
        config = yaml.safe_load(stream)

    serviceURL = config.get('worldcat_api_url')
    scope = ['wcapi:view_brief_bib']
    auth = HTTPBasicAuth(config.get('key'), config.get('secret'))
    client = BackendApplicationClient(client_id=config.get('key'), scope=scope)
    wskey = OAuth2Session(client=client)

    # create download folder if it doesn't exist
    Path("U:\Werk\OWO\AIP\WC_test").mkdir(parents=True, exist_ok=True)

    # create a backup folder with the json files from last time and do the backup
    Path(f"U:\Werk\OWO\AIP\WC_test\\backup_{runday}").mkdir(parents=True, exist_ok=True)
    sourcepath = (f"U:\Werk\OWO\AIP\WC_test")
    sourcefiles = os.listdir(sourcepath)
    destinationpath = (f"U:\Werk\OWO\AIP\WC_test\\backup_{runday}")
    for file in sourcefiles:
        if file.endswith(".json"):
            shutil.move(os.path.join(sourcepath, file), os.path.join(destinationpath, file))

    # Provide the file name and location for which to look up data
    excelfile = input('Please provide the location and name of the Excel file.\nExample: C:\\temp\keyword_list.xlsx \n')
    sh_name = input('Please provide the exact sheet name that has the ISBN column: \n')
    Pubs = pd.read_excel(f'{excelfile}', sheet_name=sh_name)
    # Keep part of the list with essential data:
    Publication_list = Pubs[['ISBN', 'Publisher']].copy()

    # Keep DataFrame where ISBN is available and where Publisher is not available'
    Pubs_R_new1 = Publication_list.dropna(subset=['ISBN'])
    Pubs_R_new = Pubs_R_new1.loc[Pubs_R_new1['Publisher'].isnull()]
    # Turn column with ISBN codes into string to search later
    Pubs_R_new = Pubs_R_new.astype(str)
    # Remove added .0 from each ISBN that was added by importing
    Pubs_R_new['ISBN'] = Pubs_R_new['ISBN'].str.replace('.0', '')

    # Make a list of the ISBN codes that need to be looked up based on the Excel file
    ISBN_list_original = Pubs_R_new['ISBN'].tolist()
    # Remove the item syllabus if it occurs in the list
    try:
        ISBN_list_original.remove('syllabus')
    except ValueError:
        pass

    # Create an empty list to store unique books = to remove duplicate ISBNs
    ISBN_list = []
    # Iterate through each value in the list 'a'
    for book in ISBN_list_original:
        # Check if the value is not already in 'ISBN_list'
        if book not in ISBN_list:
            # If not present, append it to 'ISBN_list
            ISBN_list.append(book)

    # Get the data for each publication in the ISBN list
    Listsize = len(ISBN_list)
    print('\n Original number of ISBN codes in Excel file: ', Listsize, '\n')

    # Check first if the ISBN is correct or not. The ISBN strings originally were harvested
    # using Regex string searches in PDF documents.
    n = 0
    vISBN_list = []
    while n < Listsize:
        isbn_c = ISBN_list[n]
        # Remove non ISBN digits, then split into a list
        chars = list(re.sub("[- ]|^ISBN(?:-1[03])?:?", "", isbn_c))
        # Remove the final ISBN digit from `chars`, and assign it to `last`
        last = chars.pop()
        if len(chars) == 9:
            # Compute the ISBN-10 check digit
            val = sum((x + 2) * int(y) for x, y in enumerate(reversed(chars)))
            check = 11 - (val % 11)
            if check == 10:
                check = "X"
            elif check == 11:
                check = "0"
        else:
            # Compute the ISBN-13 check digit
            val = sum((x % 2 * 2 + 1) * int(y) for x, y in enumerate(chars))
            check = 10 - (val % 10)
            if check == 10:
                check = "0"

        if (str(check) == last):
            # print("\nValid ISBN: ", isbn_c)
            vISBN_list.append(isbn_c)
        else:
            # print("\nInvalid ISBN: ", isbn_c)
            pass
        n = n + 1

    valid_isbn = len(vISBN_list)
    print(f'\n Number of valid ISBN codes: {valid_isbn}\n')

    # Create an output folder if it doesn't exist
    Path('U:\Werk\OWO\AIP\Output').mkdir(parents=True, exist_ok=True)

    # Use this example ISBN list to get information from WorldCat
    Publisher_Book_Table = pd.DataFrame()

    # Get WorldCat Records for each ISBN in the list
    listitem = 0
    while listitem < valid_isbn:
        try:
            token = wskey.fetch_token(token_url=config.get('token_url'), auth=auth)
            try:
                logger.debug(f'Retrieving data from WorldCat for ISBN {vISBN_list[listitem]}, {listitem + 1} of a total of {valid_isbn} ISBNs)')
                r = wskey.get(
                    serviceURL + "/brief-bibs?q=bn:" + str(vISBN_list[listitem]) + "&groupRelatedEditions=false&showHoldingsIndicators=true")
                r.raise_for_status()
                response = r.json()
                # keep json as backup
                with open(f'U:\Werk\OWO\AIP\WC_test/{vISBN_list[listitem]}.json', 'w') as f:
                    f.write(json.dumps(response))
                # Process data in downloaded files
                BookListPublisher = []
                ISBN1_book = []
                ISBN2_book = []
                holding = []
                oclcNumber = []
                author = []
                title = []

                # To get all data for every edition
                nrRecords = len(response['briefRecords'])
                recno = 0
                while recno < nrRecords:
                    try:
                        # Test if the publisher field is missing
                        pub = response['briefRecords'][recno]['publisher']
                    except KeyError:
                        pub = "None"
                    try:
                        # Test if the Oclc number field is missing
                        onr = response['briefRecords'][recno]['oclcNumber']
                    except KeyError:
                        onr = "None"
                    try:
                        # Test if institutionHolding data is missing
                        hol1 = response['briefRecords'][recno]['institutionHoldingIndicators'][0]['holdsItem']
                        if hol1 == 0.0:
                            hol = "False"
                        elif hol1 == 1.0:
                            hol = "True"
                        else:
                            hol = hol1
                    except KeyError:
                        hol = "None"
                    try:
                        # Test if the Creator field is missing
                        au = response['briefRecords'][recno]['creator']
                    except KeyError:
                        au = "None"
                    try:
                        # Test if the title field is missing
                        tle = response['briefRecords'][recno]['title']
                    except KeyError:
                        tle = "None"
                    try:
                        # Test if a field is available with an ISBN code
                        testlenid = len(response['briefRecords'][recno]['isbns'])
                    except KeyError:
                        testlenid = 0

                    if testlenid < 2:
                        isbn_id1 = "None"
                        isbn_id2 = "None"
                    else:
                        try:
                            isbn_id1 = response['briefRecords'][recno]['isbns'][0]
                            isbn_id2 = response['briefRecords'][recno]['isbns'][1]
                        except KeyError:
                            isbn_id1 = "None"
                            isbn_id2 = "None"
                    BookListPublisher.append(pub)
                    oclcNumber.append(onr)
                    author.append(au)
                    title.append(tle)
                    holding.append(hol)
                    ISBN1_book.append(isbn_id1)
                    ISBN2_book.append(isbn_id2)
                    recno = recno + 1
            except requests.exceptions.HTTPError as err:
                print(err)
                continue
        except BaseException as err:
            print(err)
        book_data_table = {'ISBN1': ISBN1_book}
        book_table = pd.DataFrame(book_data_table)
        book_table['ISBN2'] = ISBN2_book
        book_table['Publisher'] = BookListPublisher
        book_table['Holding'] = holding
        book_table['OCLC_nr'] = oclcNumber
        book_table['Author'] = author
        book_table['Title'] = title
        book_table['Search_ISBN'] = str(vISBN_list[listitem])
        Publisher_Book_Table = pd.concat([Publisher_Book_Table, book_table], ignore_index=True)
        listitem = listitem + 1

    # Export end result
    Publisher_Book_Table.to_csv(f'U:\Werk\OWO\AIP\WC_test\WorldCat_Book_list_' + runday + '.txt', sep='\t',
                                encoding='utf-8')

    # Create an abbreviated table with just ISBN numbers and duplicates removed
    Publisher_Book_Table_abb = Publisher_Book_Table.copy()
    Publisher_Book_Table_abb = Publisher_Book_Table_abb.drop(['OCLC_nr'], axis=1)
    Publisher_Book_Table_abb = Publisher_Book_Table_abb.drop_duplicates()
    Publisher_Book_Table_abb.to_csv(f'U:\Werk\OWO\AIP\WC_test\WorldCat_Book_list_abb_' + runday + '.txt', sep='\t',
                                    encoding='utf-8')

    # Read Json files
    # Establish location and files with data. Put the filenames in a table
    # and add the date in the file name as data for a column
    path = 'U:\Werk\OWO\AIP\WC_test'

    # Get list of all files only in the given directory
    oclist = lambda x: os.path.isfile(os.path.join(path, x))
    files_list = filter(oclist, os.listdir(path))

    # Create a list of files in directory along with the size
    size_of_file = [
        (f, os.stat(os.path.join(path, f)).st_size)
        for f in files_list
    ]

    # Create a table with the list as input
    OCR_list = pd.DataFrame(size_of_file, columns=['File_name', 'File_size'])
    # Remove files that are not Json files but other files
    OCR_list = OCR_list[OCR_list['File_name'].str.contains('.json') == True]

    # Remove files from the list that are empty or close to it but put them in a list and log them
    small_filtered = OCR_list.loc[(OCR_list['File_size'] < 50)]
    failed_return = small_filtered['File_name'].tolist()
    old_substring = ".json"
    new_substring = ""
    result = list(map(lambda s: s.replace(old_substring, new_substring), failed_return))
    file = open('U:\Werk\OWO\AIP\WC_test\ISBNs_not_found.txt', 'w')
    for item in result:
        file.write(item + ", ")
    file.close()
    logger.debug(f'\nDid not find any records in WorldCat for {len(result)} ISBNs:\n {result}.\n')
    OCR_list = OCR_list[OCR_list.File_size > 50]  # 50 = bytes

    # Reset index of the table
    OCR_list.reset_index(drop=True, inplace=True)

    Recnr = OCR_list.shape[0]
    logger.debug(f'Number of Json files to process: {Recnr}\n')

    # Step 2 Use list to get data from Json files
    OCLC_Rec_data = pd.DataFrame()
    i = 0
    while i != Recnr:
        logger.debug(f'Copying and adding data from ' + OCR_list.File_name[i])
        f = open(f'U:\Werk\OWO\AIP\WC_test\\{OCR_list.File_name[i]}', 'r')
        # returns JSON object as a dicionary
        data = json.load(f)
        # Iterating through the json list to get specific items
        # First put them in item lists and then create a table by combining them
        oclcnr_list = []
        date_list = []
        year_list = []
        format = []
        for l1 in data['briefRecords']:
            oclcnr_list.append(l1['oclcNumber'])
            # Original Pub. date information
            date_list.append(l1['date'])
            # Make a copy with only the year of the date field in the Json
            try:
                # First get only numbers from the field
                s=(l1['date'])
                yr=int("".join([x for x in s if x.isdigit()]))
                # Now isolate the last 4 numbers if longer then 4
                digit_index = 4
                digit = (yr % 10 ** digit_index)
                year_list.append(digit)
            except:
                year_list.append('None')
            try:
                format.append(l1['specificFormat'])
            except KeyError:
                try:
                    # This field can be used if the specificFormat field is not there
                    format.append(l1['generalFormat'])
                except:
                    format.append('None')
        f.close()
        oclc_data = {'OCLC_nr': oclcnr_list}
        test_list = pd.DataFrame(oclc_data)
        test_list['Publication_Date'] = date_list
        test_list['Pub_year'] = year_list
        test_list['SpecificFormat'] = format
        OCLC_Rec_data = pd.concat([OCLC_Rec_data, test_list], ignore_index=True)
        i = i + 1

    # Export result as a CSV file with the date of the Python run
    OCLC_Rec_data.to_csv(f'U:\Werk\OWO\AIP\WC_test\\OCLC_Rec_data.csv', encoding='utf-8')

    # Merge the OCLC data with the API data into a single file on OCLC numbers
    WorldCat_Book_Data_full = pd.merge(Publisher_Book_Table, OCLC_Rec_data, on=['OCLC_nr'])

    # Create an abbreviated table with duplicates removed
    WorldCat_Book_Data = WorldCat_Book_Data_full.copy()
    WorldCat_Book_Data = WorldCat_Book_Data[WorldCat_Book_Data.Publication_Date != "uuuu"]
    WorldCat_Book_Data = WorldCat_Book_Data.drop_duplicates()
    WorldCat_Book_Data['OCLC_Link'] = 'https://vu.on.worldcat.org/search?queryString=' + WorldCat_Book_Data['OCLC_nr']
    WorldCat_Book_Data.to_csv(f'U:\Werk\OWO\AIP\WC_test\\WorldCat_All_Editions_data.txt', sep='\t', encoding='utf-8')

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