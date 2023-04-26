from bs4 import BeautifulSoup
from datetime import date
from datetime import timedelta
from datetime import datetime
import re
import requests
import logging
import click
import time
import threading
import socket
import json

# dictionary to sub team names for team abbreviations
TEAM_DICT = {
    "D'backs": "ARI",
    "Arizona Diamondbacks": "ARI",
    "Braves": "ATL",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Orioles": "BAL",
    "Red Sox": "BOS",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Cubs": "CHC", 
    "Chicago White Sox": "CHW",
    "White Sox": "CHW",
    "Cincinnati Reds": "CIN",
    "Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Rockies": "COL",
    "Detroit Tigers": "DET",
    "Tigers": "DET",
    "Miami Marlins": "MIA",
    "Marlins": "MIA",
    "Houston Astros": "HOU",
    "Astros": "HOU",
    "Kansas City Royals": "KC",
    "Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Dodgers": "LAD",
    "Milwaukee Brewers": "MIL",
    "Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "Twins": "MIN",
    "New York Mets": "NYM",
    "Mets": "NYM",
    "New York Yankees": "NYY",
    "Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Athletics": "OAK",
    "Philadelpha Phillies": "PHI",
    "Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "Pirates": "PIT",
    "San Diego Padres": "SD",
    "Padres": "SD",
    "San Francisco Giants": "SF",
    "Giants": "SF",
    "Seattle Mariners": "SEA",
    "Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Rays": "TB",
    "Texas Rangers": "TX",
    "Rangers": "TX",
    "Toronto Blue Jays": "TOR",
    "Blue Jays": "TOR",
    "Washington Nationals": "WAS",
    "Nationals": "WAS"
}

# logger records output of server 
LOGGER = logging.getLogger(__name__)

class Updater:

    # Scrape for previous day's winners
    def scrape_for_scores(self, date):
        day, month, year = date.rstrip().split('/')
        query = '?year=' + year + '&month=' + month + '&day=' + day

        r = requests.get(f"https://www.baseball-reference.com/boxes/{query}")
        soup = BeautifulSoup(r.content, 'html5lib')

        scoreboard = soup.findAll('tr', attrs= {"class": "winner"})
        winners = []

        for game in scoreboard:
            out_str = game.text
            out_str = out_str.rstrip()
            out_str = re.sub('[0-9]', '', out_str)
            out_str = out_str.replace('Final', '')
            out_str = out_str.replace('()', '')
            out_str = out_str.replace('\n', '')
            out_str = out_str.replace('\t', '')
            out_str = TEAM_DICT[out_str]
            if out_str != '':
                winners.append(out_str)
        
        return winners

    # Scrape for today's matchups 
    def scrape_for_next_games(self):

        r = requests.get("https://www.baseball-reference.com/previews/")
        soup = BeautifulSoup(r.content, 'html5lib')

        matchups = soup.findAll('table', attrs={'class': 'teams'})
        match_out = []

        for matchup in matchups:
            teams = matchup.findAll('a')
            team_list = []
            for team in teams:
                if team.text == 'Preview':
                    continue
                else:
                    out_str = team.text
                    out_str = out_str.rstrip().strip()
                    out_str = re.sub('[0-9]', '', out_str)
                    out_str = out_str.replace('Preview', '')
                    out_str = out_str.replace('(-)', '')
                    out_str = out_str.replace(':PM', '')
                    out_str = TEAM_DICT[out_str]
                    team_list.append(out_str)

            match_out.append(team_list)                   

        return match_out
    
    # Run both scrapers at set interval 
    def run_scrapers(self):
        while not self.signals["shutdown"]:

            # check if it is time for new update 
            now = datetime.now()
            if now < self.nextupdate:
                continue

            else:
                today = date.today()
                d = timedelta(days = 1)
                todaymin1 = today - d
                daymin1 = todaymin1.strftime("%d/%m/%Y")
                today = today.strftime("%d/%m/%Y")
                teams_won = self.scrape_for_scores(daymin1)
                tmr_matchups = self.scrape_for_next_games()

                # Send update request to API to update previous games 
                url = 'http://localhost:8000/v1/api/update/?type=winners'
                obj = {"winners": teams_won,
                    "date": daymin1}
                requests.post(url, json = obj)

                # Send update request to API to add today's games 
                url = 'http://localhost:8000/v1/api/update/?type=matchups'
                obj = {"matchups": tmr_matchups, 
                    "date": today}
                requests.post(url, json = obj)

                # set new update time
                self.currentupdate = now
                delta = timedelta(minutes=1)
                self.nextupdate = self.currentupdate + delta
                LOGGER.info(f"updated winners at {str(self.currentupdate)}")
                LOGGER.info(f"updated matchups at {str(self.currentupdate)}")
                LOGGER.info(f"next update scheduled for {str(self.nextupdate)}")
                time.sleep(10)

    # initialize instance of updater: listen for shutdown and begin scraping thread
    def __init__(self, host, port):
        """Initialize Updater."""
        self.signals = {"shutdown": False}
        LOGGER.info("starting Updater")

        # get current time and time of next update 
        self.currentupdate = datetime.now()
        interval = timedelta(minutes=1)
        self.nextupdate = self.currentupdate + interval

        # initialize thread for running scrapers 
        self.thread = threading.Thread(target=self.run_scrapers)
        self.thread.start()

        # open TCP socket to listen for shutdown messages 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            sock.settimeout(1)

            while True:
                # accept messages on given host and port
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                clientsocket.settimeout(1)

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                message_dict = {}

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    message_dict = {"message_type": "error"}

                if message_dict["message_type"] == "error":
                    continue

                if message_dict["message_type"] == "shutdown":
                    LOGGER.info("shutdown received")
                    self.signals["shutdown"] = True
                    self.thread.join()
                    LOGGER.info("shutting down")
                    return
        

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, logfile, loglevel):
    """Run Updater."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        f"Updater:{port} %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Updater(host, port)


if __name__ == "__main__":
    main()


