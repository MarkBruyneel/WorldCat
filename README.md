# WorldCat

Two scripts are included here that allow you to collect data from WorldCat:

The first script searches using the main WorldCat search API on ISBN codes and retrieves publisher data into a csv file. The input is based on an excel file that is generated from a Digital Learning Environment.

The second script uses the same excel file but where ISBN codes are not available, uses the names of files to generate a keyword list to search for publication titles using the same API. Since the search result will vary a lot more where it concerns finding the right publications, it includes a search string comparison to give an approximation percentage on whether the match with a publication was succesful.

WorldCat search API: https://developer.api.oclc.org/wcv2

ISBN: https://en.wikipedia.org/wiki/ISBN

N.B.: Access to the WorldCat API requires authentication which needs to be acquired beforehand. This will provide a key and secret code that needs to be put in a config.yml file for both scripts. Such a file looks like:

key: 
secret: 
auth_url: https://oauth.oclc.org/auth
token_url: https://oauth.oclc.org/token
worldcat_api_url: https://americas.discovery.api.oclc.org/worldcat/search/v2
