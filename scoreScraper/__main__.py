from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
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
    "Dbacks": "ARI",
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
    "Philadelphia Phillies": "PHI",
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
        query = '?date=' + year + '-' + month + '-' + day

        browser = webdriver.Chrome()
        browser.get(f"https://baseballsavant.mlb.com/gamefeed{query}")

        title = (
            WebDriverWait(driver=browser, timeout=10)
            .until(visibility_of_element_located((By.CLASS_NAME, 'team-name')))
            .text
        )

        content = browser.page_source
        soup = BeautifulSoup(content, 'html5lib')
        games = soup.find_all(class_="team-name")
        scores = soup.find_all("div", class_="score")

        winners = []

        for i in range(0, len(games), 2):
            away = games[i].text.strip()
            away = away.replace('\t', '')
            away = away.replace('\n', '')
            away = TEAM_DICT[away]

            home = games[i+1].text.strip()
            home = home.replace('\t', '')
            home = home.replace('\n', '')
            home = TEAM_DICT[home]

            away_score = scores[i].text.strip()
            away_score = away_score.replace('\n', '')
            away_score = away_score.replace('\t', '')

            home_score = scores[i+1].text.strip()
            home_score = home_score.replace('\n', '')
            home_score = home_score.replace('\t', '')

            if int(away_score) > int(home_score):
                winners.append(away)
            else: 
                winners.append(home)
            
        return winners

    # Scrape for next day's matchups 
    def scrape_for_next_games(self, date):

        day, month, year = date.rstrip().split('/')

        browser = webdriver.Chrome()
        browser.get(f"https://baseballsavant.mlb.com/gamefeed?date={year}-{month}-{day}")

        title = (
            WebDriverWait(driver=browser, timeout=10)
            .until(visibility_of_element_located((By.CLASS_NAME, 'team-name')))
            .text
        )

        content = browser.page_source
        soup = BeautifulSoup(content, 'html5lib')
        games = soup.find_all(class_="team-name")
        times = soup.find_all(class_="game-status-scheduled")

        count = 0
        match_out = []
        for i in range(0, len(games), 2):
            away = games[i].text.strip()
            away = away.replace('\t', '')
            away = away.replace('\n', '')
            away = TEAM_DICT[away]

            home = games[i+1].text.strip()
            home = home.replace('\t', '')
            home = home.replace('\n', '')
            home = TEAM_DICT[home]

            gametime = times[count].text.strip()
            gametime = gametime.replace('\t', '')
            gametime = gametime.replace('\n', '')

            game_dict = {
                "away": away, 
                "home": home, 
                "time": gametime, 
            }

            match_out.append(game_dict)

            count += 1                 

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
                todayplus1 = today + d
                daymin1 = todaymin1.strftime("%d/%m/%Y")
                today = today.strftime("%d/%m/%Y")
                todayplus1 = todayplus1.strftime("%d/%m/%Y")
                teams_won = self.scrape_for_scores(daymin1)
                tmr_matchups = self.scrape_for_next_games(todayplus1)

                # Send update request to API to update previous games 
                url = 'http://localhost:8000/v1/api/update/?type=winners'
                obj = {"winners": teams_won,
                    "date": daymin1}
                requests.post(url, json = obj)

                # Send update request to API to add today's games 
                url = 'http://localhost:8000/v1/api/update/?type=matchups'
                obj = {"matchups": tmr_matchups, 
                    "date": todayplus1}
                requests.post(url, json = obj)

                # set new update time
                self.currentupdate = now
                self.nextupdate = self.currentupdate + self.interval
                LOGGER.info(f"updated winners at {str(self.currentupdate)}")
                LOGGER.info(f"updated matchups at {str(self.currentupdate)}")
                LOGGER.info(f"next update scheduled for {str(self.nextupdate)}")
                time.sleep(5)

    # initialize instance of updater: listen for shutdown and begin scraping thread
    def __init__(self, host, port):
        """Initialize Updater."""
        self.signals = {"shutdown": False}
        LOGGER.info("starting Updater")

        # get current time and time of next update 
        self.currentupdate = datetime.now()
        self.interval = timedelta(seconds=30)
        self.nextupdate = self.currentupdate - self.interval

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


