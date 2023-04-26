### ScoreScraper ###

This is a project meant to work in conjunction with an API built for the GameGuesser website. Upon startup, 
it consistently scrapes https://baseballreference.com on a set interval for the day's games and yesterday's winners.
The interval can be set via the variable interval, in the __init__ method for the Updater class. The information can then be sent to an API in JSON format which can in turn update and/or interact with the database storing the 
info. 

USAGE:
    - ./bin/scrape start
        - runs server on localhost port 6000 
        - log output to var/updater.log
    - ./bin/scrape stop 
        - stops server from running
    - ./bin/scrape status
        - outputs status of ScoreScraper server