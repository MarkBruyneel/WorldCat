# Publishers tool to be able to augment AIP data from Canvas with
# publisher data from WorldCat using the OCLC Discovery API
# This tool uses words from the filename (and publisher) as a basis for text-based searches
# Author: Mark Bruyneel
#
# Date: 2025-06-11
# Version: 2.0
# Created using Python version 3.10
#
# Re-use note: Make sure to change folder names that are relevant to your computer

import pandas as pd  # version 2.2.3
import os
import re
import numpy as np
import json
import shutil
from difflib import SequenceMatcher
import yaml
from oauthlib.oauth2 import BackendApplicationClient  # version 3.2.2
from requests.auth import HTTPBasicAuth  # version 2.31.0
from requests_oauthlib import OAuth2Session
import requests  # version 2.31.0
# To catch errors, use the logger option from loguru
from loguru import logger  # version 0.7.2
# Also needed to get the run time of the script
from datetime import datetime  # version 5.5
import time
from pathlib import Path
import nltk  # version 3.9.1

nltk.download('stopwords')
from nltk.corpus import stopwords

# to get a list of all languages for which there are lists
# print(stopwords.fileids())
English_stop = stopwords.words('english')
Dutch_stop = stopwords.words('dutch')
# Create list of specific words to remove for this file
Special_list = ['syllabus', 'hoofdstuk', 'chapter', 'wb', 'lecture', 'notes']
# Merge all three lists into one list. This is not recommended if cleaning texts
# for too many language types. In this program English and Dutch will cover most stop words
All_stopwords = English_stop + Dutch_stop + Special_list

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

logger.add(r'U:\Werk\OWO\AIP\AIP_WC_Text_Search_test.log', backtrace=True, diagnose=True, rotation="10 MB",
           retention="12 months")


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
    Path(f"U:\Werk\OWO\AIP\WC_test\\tbackup_{runday}").mkdir(parents=True, exist_ok=True)
    sourcepath = (f"U:\Werk\OWO\AIP\WC_test")
    sourcefiles = os.listdir(sourcepath)
    destinationpath = (f"U:\Werk\OWO\AIP\WC_test\\tbackup_{runday}")
    for file in sourcefiles:
        if file.endswith(".json"):
            shutil.move(os.path.join(sourcepath, file), os.path.join(destinationpath, file))

    # Provide the file name and location for which to look up data
    excelfile = input('Please provide the location and name of the Excel file.\nExample: C:\\temp\keyword_list.xlsx \n')
    sh_name = input('Please provide the exact sheet name that has the data: \n')
    Pubs = pd.read_excel(f'{excelfile}', sheet_name=sh_name)
    # Keep part of the list with essential data:
    Publication_list = Pubs[['Material id', 'Filename', 'Title', 'ISBN', 'Publisher']].copy()

    # Keep DataFrame where ISBN is not available
    Publication_overview = Publication_list.query('ISBN != ISBN')
    # Remove ISBN column
    Publication_overview = Publication_overview.drop('ISBN', axis=1)

    # Make a copy to avoid the slice warning
    Publications = Publication_overview.copy()

    # First make a copy of the field I will edit
    Publications['Filename_copy'] = Publications['Filename']

    # Turn all string data to lower case (makes it easier to clean)
    Publications['Filename_copy'] = Publications.Filename.str.lower()

    # Remove substring values to remove unneeded characters to separate words
    # The # needs to be removed as this causes errors in the WorldCat API
    To_remove_list = ['.pdf', 'pdf', '[', ']', '{', '}', '.', ',', '#']
    To_remove_list2 = ['_', ' - ', '-', '(', ')']
    for sub in To_remove_list:
        Publications['Filename_copy'] = Publications['Filename_copy'].str.replace(sub, '')
    for subs in To_remove_list2:
        Publications['Filename_copy'] = Publications['Filename_copy'].str.replace(subs, ' ')

    # Dataframe that is needed for the comparison with the search result later.
    # This table needs to be merged with the search result table
    For_later_comparison = Publications.copy()
    For_later_comparison = For_later_comparison.drop(['Filename', 'Title', 'Publisher'], axis=1)
    For_later_comparison = For_later_comparison.rename(columns={'Material id': 'Search_MID'})

    # Remove numeric strings that are smaller than 2 characters ???
    # https://www.digitalocean.com/community/tutorials/python-remove-character-from-string
    Publications['Filename_copy'] = Publications['Filename_copy'].replace(value='', regex=r'\d{1,2}\s?')

    # Get 4-digit numbers from Filename and create a new variable with this
    Publications['Filename_copy_year'] = Publications['Filename'].str.extract(r'(19\d\d|20\d\d)', expand=True)

    # Remove the numbers from the list
    string_column = Publications['Filename_copy']
    string_column.str.extract(r'(\*[0-9])')

    # Removing stopwords using lists of existing language stop words
    Publications['Filename_copy'] = Publications['Filename_copy'].fillna("")
    Publications['Filename_copy'] = Publications['Filename_copy'].apply(
        lambda x: [item for item in x.split() if item not in All_stopwords])

    # Remove rows where the Filename_copy field only has an empty list
    Publications = Publications[Publications['Filename_copy'].str.len() != 0]
    # Generate a column containing the word count for the word list = Filename_copy
    Publications['Word_count'] = [len(c) for c in Publications['Filename_copy']]
    # Remove columns that only have one word left
    Publications = Publications[Publications['Word_count'] > 2]
    # Remove columns that only have more than 10 words
    Publications = Publications[Publications['Word_count'] < 11]

    # Rename column names for easier understanding
    Publications = Publications.rename(columns={'Filename_copy': 'Word_list', 'Filename_copy_year': 'Publication_year'})

    Publications.to_csv(f'U:\Werk\OWO\AIP\WC_test\\Test_text_search_data.txt', sep='\t', encoding='utf-16')

    # Make a list of the ISBN codes that need to be looked up based on the Excel file
    Word_lists_original = Publications['Word_list'].tolist()
    years_list_original = Publications['Publication_year'].tolist()
    Material_ID_list = Publications['Material id'].tolist()

    listsize = len(Material_ID_list) - 1

    search_string_list = []
    snr = 0
    while snr < listsize:
        new_Word_list = []
        for word in Word_lists_original[snr]:
            if len(word) < 3:
                pass
            else:
                new_Word_list.append(word)
        # Combine the words and the publication year into a search string
        res = ' AND '.join([str(x) for x in new_Word_list])
        query = res + ' AND yr:' + str(years_list_original[snr])
        # Remove the last piece if there was no corresponding year in the list = nan
        query = re.sub(' AND yr:nan', '', query)
        search_string_list.append(query)
        snr = snr + 1
    print('Search strings: ', search_string_list, '\n')
    # Next step is to use the data to search and download records
    # Create an output folder if it doesn't exist
    Path('U:\Werk\OWO\AIP\Output').mkdir(parents=True, exist_ok=True)

    # Use this example ISBN list to get information from WorldCat
    WC_text_Book_Table = pd.DataFrame()

    No_of_strings = len(search_string_list) - 1
    Nr_of_strings = len(search_string_list)
    # Get WorldCat Records for each ISBN in the list
    listitem = 0
    while listitem < No_of_strings:
        try:
            token = wskey.fetch_token(token_url=config.get('token_url'), auth=auth)
            try:
                itemlist = listitem + 1
                SearchString_Length = len(search_string_list[listitem])
                logger.debug(
                    f'Retrieving data from WorldCat for string {itemlist}, of a total of {Nr_of_strings} strings. Length is: {SearchString_Length})')
                r = wskey.get(serviceURL + "/brief-bibs?q=" + str(search_string_list[
                                                                      listitem]) + "&groupRelatedEditions=false&openAccess&showHoldingsIndicators=true")
                r.raise_for_status()
                response = r.json()
                # keep json as backup
                with open(f'U:\Werk\OWO\AIP\WC_test/{Material_ID_list[listitem]}.json', 'w') as f:
                    f.write(json.dumps(response))
                # Process data in downloaded files
                BookListPublisher = []
                ISBN1_book = []
                ISBN2_book = []
                holding = []
                oclcNumber = []
                author = []
                title = []
                MatID = []

                # To get all data for every edition
                nrRecords = len(response['briefRecords'])
                recno = 0
                while recno < nrRecords:
                    MID = str(Material_ID_list[listitem])
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
                    MatID.append(MID)
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
        book_table['Search_MID'] = MatID
        WC_text_Book_Table = pd.concat([WC_text_Book_Table, book_table], ignore_index=True)
        listitem = listitem + 1

    # Turn key Search_MID into int64 for later merge & Export end result
    WC_text_Book_Table["Search_MID"] = WC_text_Book_Table["Search_MID"].astype(np.int64)
    WC_text_Book_Table.to_csv(f'U:\Werk\OWO\AIP\WC_test\WorldCat_Text_Book_list_' + runday + '.txt', sep='\t',
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
    file = open('U:\Werk\OWO\AIP\WC_test\MaterialID_files_not_found.txt', 'w')
    for item in result:
        file.write(item + ", ")
    file.close()
    logger.debug(f'\nDid not find any records in WorldCat for {len(result)} files:\n {result}.\n')
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
                s = (l1['date'])
                yr = int("".join([x for x in s if x.isdigit()]))
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
    OCLC_Rec_data.to_csv(f'U:\Werk\OWO\AIP\WC_test\\Text_search_OCLC_Rec_data.csv', encoding='utf-8')

    # Merge the OCLC data with the API data into a single file on OCLC numbers
    WorldCat_Book_Data_full = pd.merge(WC_text_Book_Table, OCLC_Rec_data, on=['OCLC_nr'])

    # Create an abbreviated table with duplicates removed
    WorldCat_Book_Data = WorldCat_Book_Data_full.copy()
    WorldCat_Book_Data = WorldCat_Book_Data[WorldCat_Book_Data.Publication_Date != "uuuu"]
    WorldCat_Text_Search_final = WorldCat_Book_Data.drop_duplicates()
    WorldCat_Text_Search_final['OCLC_Link'] = 'https://vu.on.worldcat.org/search?queryString=' + WorldCat_Text_Search_final['OCLC_nr'].astype(str)

    # Merge the result with original Dataframe For_later_comparison
    # WorldCat_data_word_search = pd.concat([For_later_comparison, WorldCat_Text_Search_final], ignore_index=True)
    WorldCat_data_word_search = pd.merge(For_later_comparison, WorldCat_Text_Search_final, how="outer", on=['Search_MID'])

    # drop rows where value in column is null
    WorldCat_data_word_search = WorldCat_data_word_search.dropna(subset=['OCLC_nr'])

    # First make a copy of the field I will edit
    WorldCat_data_word_search['Title_copy'] = WorldCat_data_word_search['Title']

    # Turn all string data to lower case (makes it easier to clean)
    WorldCat_data_word_search['Title_copy'] = WorldCat_data_word_search.Title_copy.str.lower()

    # Remove substring values to remove unneeded characters to compare field Title_copy and Filename_copy
    for sub in To_remove_list:
        WorldCat_data_word_search['Title_copy'] = WorldCat_data_word_search['Title_copy'].str.replace(sub, '')
    for subs in To_remove_list2:
        WorldCat_data_word_search['Title_copy'] = WorldCat_data_word_search['Title_copy'].str.replace(subs, ' ')

    # Compare fields Title_copy and Filename_copy and generate a new column ratio with the result
    WorldCat_data_word_search['ratio'] = WorldCat_data_word_search[['Filename_copy', 'Title_copy']].apply(lambda x: SequenceMatcher(lambda y: y == " ", x[0], x[1]).ratio(), axis=1)

    WorldCat_data_word_search.to_csv(f'U:\Werk\OWO\AIP\WC_test\\WorldCat_data_word_search.txt', sep='\t', encoding='utf-8')

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