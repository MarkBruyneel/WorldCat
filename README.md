# WorldCat

Two scripts are included here that allow you to collect data from WorldCat:

The first script searches using the main WorldCat search API on ISBN codes and retrieves publisher data into a csv file. The input is based on an excel file that is generated from a Digital Learning Environment.
The second script uses the same file but where ISBN codes are not available, uses the names of files ro generate a keyword list to search on key words using the same API. Since the search result will vary a lot more where it concerns finding the right publications, it includes a search string comparison to give an approximation on whether the match with a publication is succesful.
