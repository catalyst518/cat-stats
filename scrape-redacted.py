import time
import requests
# from lxml import html
from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from datetime import date
import datetime
from scipy.stats import norm
import matplotlib.pyplot as plt
import numpy as np
import ast
import math
# from operator import itemgetter
import logging
import os.path
# import urllib.parse
import sqlite3
from tqdm import tqdm
import tagpro_eu
import json
# import matplotlib.ticker as ticker
from dateutil import parser
# from dateutil import tz
import pandas as pd
# import numpy as np
import os

s1time=1613222155
s1date=datetime.datetime.strptime("2021-02-13","%Y-%m-%d")
s1id="?Season=9781c66d-cdfc-434c-a319-cef7cedb15ef"
s2id="?Season=8e1c6965-c08e-493c-9d6c-c627706eb2f6"

def browserLogin():
    options = webdriver.ChromeOptions()
    options.add_argument('user-data-dir=C:/Users/REDACTED/AppData/Local/Google/Chrome/User Data/Profile 3')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    options.headless = True
    browser = webdriver.Chrome('D:/Documents/catstats/chromedriver', options=options)  # Optional argument, if not specified will search path.
    try:
        link = (
                "https://accounts.google.com/o/oauth2/v2/auth/oauthchooseaccount"
                + "?redirect_uri=https%3A%2F%2Fdevelopers.google.com%2F"
                + "oauthplayground&prompt=consent&response_type=code"
                + "&client_id=407408718192.apps.googleusercontent.com"
                + "&scope=email&access_type=offline&flowName=GeneralOAuthFlow"
        )
        browser.get(link)
        emailElem = browser.find_element_by_id('Email')
        emailElem.send_keys('REDACTED')
        nextButton = browser.find_element_by_id('next')  # .find_element_by_css_selector('button')
        nextButton.click()
        time.sleep(1)
        capElem = browser.find_elements_by_id('identifier-captcha-input')
        if len(capElem):
            browser.save_screenshot("cap.png")
            captcha = input("captcha:")
            if len(captcha) > 1:
                capElem = browser.find_element_by_id('identifier-captcha-input')
                capElem.send_keys(captcha)
                nextButton = browser.find_element_by_id('next')
                nextButton.click()
            time.sleep(1)
        passwordElem = browser.find_element_by_id('password')
        passwordElem.send_keys('REDACTED')
        form = browser.find_element_by_id('challenge')
        browser.execute_script("HTMLFormElement.prototype.submit.call(arguments[0])", form)
        print("waiting for 2fa")
        browser.save_screenshot("ss3.png")
        time.sleep(20)
        browser.save_screenshot("ss4.png")
        # browser.quit()
        print("finished waiting")
    except Exception as e:
        print("google error:", e)

    login = "https://www.tpm.gg/Account/Login"
    browser.get(login)
    return browser

def updateStreaks():
    print("Updating streaks...")
    players = cursor.execute("SELECT DISTINCT Name from players").fetchall()
    for user in tqdm(players):
        data = cursor.execute("SELECT EloDelta, matches.void from players INNER JOIN matches on matches.tpmid=players.tpmid where Name=:name and matches.MatchDate>:s1 order by matches.MatchDate", {"name":user[0],"s1":s1time}).fetchall()
        loss = 0
        win = 0
        void = 0
        greenstreaks = []
        redstreaks = []
        currentstreak = 0
        winstreak = 0
        lossstreak = 0
        last = -1
        for row in data:
            # print(user)
            # print(row)
            if row[0] < 0 and void == 0:
                loss += 1
                if last == 1:
                    lossstreak = 1
                    greenstreaks.append(winstreak)
                    winstreak = 0
                else:
                    lossstreak += 1
                last = 0
            elif row[0] > 0:
                win += 1
                if last == 0:
                    winstreak = 1
                    redstreaks.append(lossstreak)
                    lossstreak = 0
                else:
                    winstreak += 1
                last = 1
            elif row[0] < 0 and void == 1:
                void += 1
                if last == 1:
                    greenstreaks.append(winstreak)
                    winstreak = 0
                    lossstreak = 0
                elif last == 0:
                    redstreaks.append(lossstreak)
                    winstreak = 0
                    lossstreak = 0
                last = -1
        if last == 1:
            greenstreaks.append(winstreak)
            currentstreak = winstreak
        elif last == 0:
            redstreaks.append(lossstreak)
            currentstreak = -lossstreak
        # reset table
        cursor.execute("UPDATE streaks SET Total=0, Time=CURRENT_TIMESTAMP WHERE Name=:name", {"name": user[0]})
        # fill table
        for win in greenstreaks:
            row = cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=:streak", {"name": user[0], "streak": win}).fetchone()
            if row is not None:
                cursor.execute("UPDATE streaks SET Total=Total+1, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"rid": row[0]})
            else:
                cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,?,1)", (user[0], win))
        for loss in redstreaks:
            row = cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=:streak", {"name": user[0], "streak": -loss}).fetchone()
            if row is not None:
                cursor.execute("UPDATE streaks SET Total=Total+1, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"rid": row[0]})
            else:
                cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,?,1)", (user[0], -loss))
        # set current streak
        row = cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=0", {"name": user[0]}).fetchone()
        if row is not None:
            cursor.execute("UPDATE streaks SET Total=:current, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"rid": row[0], "current": currentstreak})
        else:
            cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,0,?)", (user[0], currentstreak))
    db.commit()
    print("Update complete.")


# if message.content.startswith('$updateleaderboard'):
def updateLeaderboard():
    print("Updating leaderboard...")
    url = "https://www.tpm.gg/Leaderboard/CTFNA"
    r = requests.get(url)
    next = True
    currentpage = 1
    lastpage = 1
    leaderboard = []
    print("Scraping leaderboard...")
    # scrape name, Elo, and tpm URL
    while next and currentpage < 30:  # default 30, currently 19
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.findAll('a', attrs={'class': 'leaderboardText'}):
            data = [x.strip() for x in [a.text.strip().rsplit('(', 1)[0], a.text.strip().rsplit('(', 1)[1].strip(')')]]
            data.append(a['href'])
            leaderboard.append(data)
        for pageindex in soup.findAll('a', attrs={'class': 'userProfilePagers'}):
            if int(pageindex.text) > lastpage:
                lastpage = int(pageindex.text)
                r = requests.get(url + '?page=' + pageindex.text)
                next = True
                break
        if currentpage >= lastpage:
            next = False
        currentpage += 1
    print("Updating elos database...")
    for user in tqdm(leaderboard):
        # update #elos
        row = cursor.execute("SELECT rowid FROM elos WHERE Name=:name AND Day=date('now')", {"name": user[0]}).fetchone()
        if row is not None:
            cursor.execute("UPDATE elos SET Elo=:elo, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"elo": user[1], "rid": row[0]})
        else:
            cursor.execute("INSERT INTO elos(Name, Elo, Day) VALUES (?,?,date('now'))", (user[0], user[1]))
        # add new user to #profiles
        cursor.execute("INSERT INTO profiles(Name, URL) VALUES (?,?) ON CONFLICT DO NOTHING", (user[0], user[2]))
    db.commit()
    print("Update complete.")
    return [[player[0],player[2]] for player in leaderboard]


# await discord.utils.get(client.get_guild(597561804662767627).text_channels, name='bot-spam').send('Elo update complete at '+str(datetime.datetime.now()))
# await message.channel.send('Elo update complete.')

def updateProfiles(players):
    print("Updating profiles...")
    browser = browserLogin()
    # players=cursor.execute("SELECT Name, URL FROM profiles").fetchall()
    print("Scraping %d profiles..." % (len(players)))
    for name, url in tqdm(players):
        browser.get("https://www.tpm.gg" + url + s2id)
        # browser.save_screenshot(name+".png")
        next = True
        matches = []
        currentpage = 1
        lastpage = 1
        if len(browser.find_elements_by_class_name("matchHistoryTitle")) > 0:
            while next and currentpage < 100:
                soup = BeautifulSoup(browser.page_source, 'html.parser')
                found = 0
                for match in soup.findAll('div', attrs={'class': 'matchResultBox'}):
                    if match.has_attr('onclick'):
                        data = match['onclick'][11:].split('\'')[0]
                        if cursor.execute("SELECT EXISTS(SELECT 1 FROM matches WHERE tpmid=:id AND MatchDate IS NOT NULL)", {"id": data}).fetchone()[0]:
                            found += 1
                            if found > 10:
                                next = False
                                break
                            if cursor.execute("SELECT MatchDate FROM matches WHERE tpmid=:id", {"id": data}).fetchone()[0] < 1603497601:
                                next = False
                                break
                        else:
                            matches.append(data)
                if next:
                    for pageindex in browser.find_elements_by_class_name('userProfilePagers'):
                        if int(pageindex.text) > lastpage:
                            lastpage = int(pageindex.text)
                            pageindex.click()
                            break
                    if currentpage >= lastpage:
                        next = False
                    currentpage += 1
                else:
                    break
            try:
                games = browser.execute_script("return myChart.titleBlock.options.text")
                if ' | ' in games:
                    games = games.split('|')[1].split()[0]
                else:
                    games = games.split()[1]
                if int(games) > 0:
                    cursor.execute("UPDATE profiles SET GamesPlayed=:games, Time=CURRENT_TIMESTAMP WHERE Name=:name", {"name": name, "games": games})
            except:
                pass
                # print("No matches found " + name)
            # only useful on first scrape of profile
            # cursor.execute("UPDATE profiles SET GamesPlayed=:games, Time=CURRENT_TIMESTAMP WHERE Name=:name",{"name":name,"games":len(matches)})
            # add new match to #matches
            for match in matches:
                cursor.execute("INSERT INTO matches(tpmid) VALUES (:id) ON CONFLICT DO NOTHING", {"id": match})
            if len(matches):
                print("Found %d new matches" % (len(matches)))
            db.commit()
    browser.quit()
    print("Update complete.")


def updateMatchesOverride(matches):
    print("Scraping override match data...")
    browser = browserLogin()
    print("Scraping %d matches..." % (len(matches)))
    for match in tqdm(matches):
        tpmid = match[0]
        browser.get("https://www.tpm.gg/Match/" + tpmid)
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        start = int(parser.parse(soup.find('input', {'id': 'StartTime'}).get('value')).replace(tzinfo=datetime.timezone.utc).timestamp())
        mapid = soup.find('img', {'class': 'mapImage'}).get('src')
        if "previews/" in mapid:
            mapid = mapid.split("previews/")[1].split('.')[0]
            soup = BeautifulSoup(requests.get("http://unfortunate-maps.jukejuice.com/show/" + mapid).text, 'html.parser')
            mapname = soup.find('h2', {'class': 'searchable'}).text
        else:
            mapname = None
        cursor.execute("UPDATE matches SET MatchDate=:start, Map=:mapname, Time=CURRENT_TIMESTAMP WHERE tpmid=:tpmid", {"tpmid": tpmid, "start": start, "mapname": mapname})
    db.commit()
    browser.quit()
    print("Update complete.")


def updateMatches(matches):
    print("Updating matches...")
    print("Scraping %d matches..." % (len(matches)))
    newplayers = []
    browser=browserLogin()
    for match in tqdm(matches):
        players = []
        eu = None
        redwin = None
        bluewin = None
        void = None
        redscore = None
        bluescore = None
        tpmid = match[0]
        browser.get("https://www.tpm.gg/Match/" + tpmid)
        # r = requests.get("https://www.tpm.gg/Match/" + tpmid)
        # scrape name, Elo, and tpm URL
        if len(browser.page_source):
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            red = soup.find('div', attrs={'class': 'teamContainerRed'})
            redelo = red.h1.text.strip().split('(')[1].strip(')')

            for player in red.findAll('a'):
                # player url, player name, elo delta
                spans = player.h4.findAll('span')
                players.append([player['href'], spans[0].text.strip(), spans[1].text[1:-1]])
            blue = soup.find('div', attrs={'class': 'teamContainerBlue'})
            blueelo = blue.h1.text.strip().split('(')[1].strip(')')
            for player in blue.findAll('a'):
                # player url, player name, elo delta
                spans = player.h4.findAll('span')
                players.append([player['href'], spans[0].text.strip(), spans[1].text[1:-1]])
            if soup.find('h3', attrs={'class': 'resultRedWin'}):
                redwin = 1
                bluewin = 0
                void = 0
                if soup.find('div', attrs={'class': 'basicContainer'}).h3.text != "Match Found":
                    eu = soup.find('div', attrs={'class': 'basicContainer'}).a['href']
                scores = soup.findAll('h1', attrs={'class': 'resultScore'})
                redscore = scores[0].text
                bluescore = scores[1].text
            elif soup.find('h3', attrs={'class': 'resultBlueWin'}):
                redwin = 0
                bluewin = 1
                void = 0
                if soup.find('div', attrs={'class': 'basicContainer'}).h3.text != "Match Found":
                    eu = soup.find('div', attrs={'class': 'basicContainer'}).a['href']
                scores = soup.findAll('h1', attrs={'class': 'resultScore'})
                redscore = scores[0].text
                bluescore = scores[1].text
            elif soup.find('h3', attrs={'class': 'resultVoid'}):
                redwin = 0
                bluewin = 0
                void = 1
            else:
                print("Unknown Match Result!")

            cursor.execute(
                "UPDATE matches SET euid=:euid, RedWin=:redwin, BlueWin=:bluewin, Void=:void, RedElo=:redelo, BlueElo=:blueelo, RedScore=:redscore, BlueScore=:bluescore, Time=CURRENT_TIMESTAMP WHERE tpmid=:tpmid",
                {"tpmid": tpmid, "euid": eu, "redwin": redwin, "bluewin": bluewin, "void": void, "redelo": redelo, "blueelo": blueelo, "redscore": redscore, "bluescore": bluescore})
            # update #players
            for player in players:
                playerexists = cursor.execute("SELECT rowid FROM profiles WHERE Name=:name", {"name": player[1]}).fetchone()
                if playerexists is None:
                    cursor.execute("INSERT INTO profiles(Name, URL) VALUES (?,?)", (player[1], player[0]))
                    newplayers.append([player[1], player[0]])
                row = cursor.execute("SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name", {"name": player[1], "tpmid": tpmid}).fetchone()
                if row is not None:
                    cursor.execute("UPDATE players SET EloDelta=:delta, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"delta": player[2], "rid": row[0]})
                else:
                    cursor.execute("INSERT INTO players(tpmid, Name, EloDelta) VALUES (?,?,?)", (tpmid, player[1], player[2]))

            db.commit()
    if len(newplayers):
        print("Found new profiles: ")
        print(newplayers)
        updateProfiles(newplayers)
    print("Update complete.")


def updateFromBulkEU(dbmatches):
    print("Loading maps")
    maps = json.load(open("bulkmaps.json", encoding="utf8"))
    print("Loading matches1")
    matches1 = json.load(open("bulkmatches2682319-2690593.json", encoding="utf8"))
    print("Loading matches2")
    matches2 = json.load(open("bulkmatches2432319-2682318.json", encoding="utf8"))
    print("Updating database...")
    for match in tqdm(dbmatches):
        if len(match[0]) > 25:
            eu = match[0].split('=')[1]
            if eu in matches1:
                # red=matches1[eu]['teams'][0]['score']
                # blue=matches1[eu]['teams'][1]['score']
                mapname = maps[str(matches1[eu]['mapId'])]['name']
                duration = matches1[eu]['duration']
                epoch = matches1[eu]['date']
            elif eu in matches2:
                # red=matches2[eu]['teams'][0]['score']
                # blue=matches2[eu]['teams'][1]['score']
                mapname = maps[str(matches2[eu]['mapId'])]['name']
                duration = matches2[eu]['duration']
                epoch = matches2[eu]['date']
            else:
                print("eu not found! " + eu)
                continue
            cursor.execute("UPDATE matches SET Map=:mapname, Duration=:duration, MatchDate=:epoch, Time=CURRENT_TIMESTAMP WHERE euid=:euid",
                           {"euid": match[0], "mapname": mapname, "duration": duration, "epoch": epoch})
    db.commit()
    print("Update complete.")


def updateFromEU(dbmatches):
    print("Updating match database from tagpro.eu")
    # print(len(dbmatches))
    for tpmid, euid in tqdm(dbmatches):
        if len(euid) > 25:
            eu = euid.split('=')[1]
            try:
                match = tagpro_eu.download_match(eu)
                mapname = match.map.name
                duration = int(match.duration)
                epoch = int(match.date.timestamp())
                players = cursor.execute("select Name from players where tpmid=:tpmid and RedTeam is not null",
                                         {"tpmid": tpmid}).fetchall()
                for name in players:
                    p = next((x for x in match.players if x.name.lower() == name[0].lower()), None)
                    if p is not None:
                        s = p.stats
                        if p.team is None:
                            if p.cap_diff == match.team_red.score - match.team_blue.score:
                                redteam = 1
                            elif p.cap_diff == match.team_blue.score - match.team_red.score:
                                redteam = 0
                            else:
                                redteam = None
                        elif p.team.name == 'Red':
                            redteam = 1
                        elif p.team.name == 'Blue':
                            redteam = 0
                        else:
                            redteam = None
                        row = cursor.execute(
                            "SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name collate nocase",
                            {"name": name[0], "tpmid": tpmid}).fetchone()
                        if row is not None:
                            cursor.execute(
                                "UPDATE players SET RedTeam=?, TeamWin=?, block=?, button=?, CapDiff=?, CapsAgainst=?, CapsFor=?, captures=?, drops=?, grabs=?, hold=?, pops=?, prevent=?, TotalPups=?, jukejuicepup=?, tagpropup=?, rollingpup=?, returns=?, tags=?, TimePlayed=?, Time=CURRENT_TIMESTAMP WHERE rowid=?",
                                (redteam, p.cap_diff > 0, s.block.seconds, s.button.seconds, s.cap_diff, s.caps_against,
                                 s.caps_for, s.captures, s.drops, s.grabs, s.hold.seconds, s.pops, s.prevent.seconds,
                                 s.pups_total, s.pups[1], s.pups[4], s.pups[2], s.returns, s.tags, s.time.seconds,
                                 row[0]))
                    else:
                        row = cursor.execute(
                            "SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name collate nocase",
                            {"name": name[0], "tpmid": tpmid}).fetchone()
                        cursor.execute("UPDATE players SET eumismatch=?, Time=CURRENT_TIMESTAMP WHERE rowid=?",
                                       (True, row[0]))
                cursor.execute("UPDATE matches SET Map=:mapname, Duration=:duration, MatchDate=:epoch, Time=CURRENT_TIMESTAMP WHERE euid=:euid",
                               {"euid": euid, "mapname": mapname, "duration": duration, "epoch": epoch})
            except Exception as e:
                print("Error: ",e)
                print("eu not found! " + eu)
    db.commit()
    print("Update complete.")


def updatePlayers(dbmatches):
    i = 0
    for tpmid, euid in tqdm(dbmatches):
        i += 1
        if len(euid) > 25:
            eu = euid.split('=')[1]
            try:
                match = tagpro_eu.download_match(eu)
                mapname = match.map.name
                duration = int(match.duration)
                epoch = int(match.date.timestamp())
            except Exception as e:
                print("Error:", e)
                print("eu not found! " + eu)
                continue
            players = cursor.execute("select Name from players where tpmid=:tpmid and RedTeam is null",
                                     {"tpmid": tpmid}).fetchall()
            for name in players:
                p = next((x for x in match.players if x.name.lower() == name[0].lower()), None)
                if p is not None:
                    s = p.stats
                    if p.team is None:
                        if p.cap_diff == match.team_red.score - match.team_blue.score:
                            redteam = 1
                        elif p.cap_diff == match.team_blue.score - match.team_red.score:
                            redteam = 0
                        else:
                            redteam = None
                    elif p.team.name == 'Red':
                        redteam = 1
                    elif p.team.name == 'Blue':
                        redteam = 0
                    else:
                        redteam = None
                    row = cursor.execute("SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name collate nocase",
                                         {"name": name[0], "tpmid": tpmid}).fetchone()
                    if row is not None:
                        cursor.execute(
                            "UPDATE players SET RedTeam=?, TeamWin=?, block=?, button=?, CapDiff=?, CapsAgainst=?, CapsFor=?, captures=?, drops=?, grabs=?, hold=?, pops=?, prevent=?, TotalPups=?, jukejuicepup=?, tagpropup=?, rollingpup=?, returns=?, tags=?, TimePlayed=?, Time=CURRENT_TIMESTAMP WHERE rowid=?",
                            (redteam, p.cap_diff > 0, s.block.seconds, s.button.seconds, s.cap_diff, s.caps_against,
                             s.caps_for, s.captures, s.drops, s.grabs, s.hold.seconds, s.pops, s.prevent.seconds,
                             s.pups_total, s.pups[1], s.pups[4], s.pups[2], s.returns, s.tags, s.time.seconds, row[0]))
                else:
                    row = cursor.execute("SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name collate nocase",
                                         {"name": name[0], "tpmid": tpmid}).fetchone()
                    cursor.execute("UPDATE players SET eumismatch=?, Time=CURRENT_TIMESTAMP WHERE rowid=?", (True, row[0]))

        if not i % 100:
            print("saving")
            db.commit()
    db.commit()
    print("Update player stats complete.")


def importAlias():
    if os.path.exists('alias_tpm.txt'):
        with open('alias_tpm.txt', 'r') as filehandle:
            aliaslist = [ast.literal_eval(line) for line in filehandle]
        for row in aliaslist:
            cursor.execute("INSERT INTO alias(Discord, Name, Time) VALUES(:discord,:name,CURRENT_TIMESTAMP) ON CONFLICT(Discord) DO UPDATE SET Name=excluded.Name,Time=CURRENT_TIMESTAMP",
                           {"discord": row[0], "name": row[1]})
        db.commit()


def importElos():
    # batch days
    # totaldays=210
    # for ndays in tqdm(range(totaldays)):
    # 	nyesterday=(date.today()-datetime.timedelta(days=ndays)).strftime("%b-%d-%Y")
    # 	nyesterdaydb=(date.today()-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
    # 	if os.path.exists(nyesterday+'tpm.txt'):
    # 		with open(nyesterday+'tpm.txt', 'r') as filehandle:
    # 			oldelo=[ast.literal_eval(line) for line in filehandle]
    # 		for user in oldelo:
    # 			#update #elos
    # 			row=cursor.execute("SELECT rowid FROM elos WHERE Name=:name AND Day=:day",{"name":user[0],"day":nyesterdaydb}).fetchone()
    # 			if row is not None:
    # 				cursor.execute("UPDATE elos SET Elo=:elo, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"elo":int(user[1]),"rid":row[0]})
    # 			else:
    # 				cursor.execute("INSERT INTO elos(Name, Elo, Day) VALUES (?,?,?)",(user[0],user[1],nyesterdaydb))
    # 			#add new user to #profiles, will need to find url
    # 			cursor.execute("INSERT INTO profiles(Name) VALUES (:name) ON CONFLICT DO NOTHING",{"name":user[0]})
    # 		db.commit()
    # single day
    singleday = "2020-11-02"
    nyesterday = datetime.datetime.strptime(singleday, "%Y-%m-%d").strftime("%b-%d-%Y")
    nyesterdaydb = datetime.datetime.strptime(singleday, "%Y-%m-%d").strftime("%Y-%m-%d")
    if os.path.exists(nyesterday + 'tpm_temp.txt'):
        with open(nyesterday + 'tpm_temp.txt', 'r') as filehandle:
            oldelo = [ast.literal_eval(line) for line in filehandle]
        for user in oldelo:
            # update #elos
            row = cursor.execute("SELECT rowid FROM elos WHERE Name=:name AND Day=:day", {"name": user[0], "day": nyesterdaydb}).fetchone()
            if row is not None:
                cursor.execute("UPDATE elos SET Elo=:elo, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"elo": int(user[1]), "rid": row[0]})
            else:
                cursor.execute("INSERT INTO elos(Name, Elo, Day) VALUES (?,?,?)", (user[0], user[1], nyesterdaydb))
            # add new user to #profiles, will need to find url
            cursor.execute("INSERT INTO profiles(Name) VALUES (:name) ON CONFLICT DO NOTHING", {"name": user[0]})
        db.commit()
    print("Import complete")


def extrapolateElo():
    startdate = "2020-11-13"
    enddate = "2020-11-16"
    players = cursor.execute("SELECT DISTINCT Name FROM Elos WHERE Day=:start", {"start": startdate}).fetchall()
    # days=(datetime.datetime.now()-datetime.datetime.strptime(startdate,"%Y-%m-%d")).days
    days = (datetime.datetime.strptime(enddate, "%Y-%m-%d") - datetime.datetime.strptime(startdate, "%Y-%m-%d")).days
    for player in tqdm(players):
        elo = cursor.execute("SELECT Elo from elos WHERE name=:name AND Day=:start", {"name": player[0], "start": startdate}).fetchone()[0]
        for day in range(days):
            olddate = datetime.datetime.strptime(startdate, "%Y-%m-%d") + datetime.timedelta(days=day + 1)
            newdate = datetime.datetime.strptime(startdate, "%Y-%m-%d") + datetime.timedelta(days=day + 2)
            delta = cursor.execute("SELECT sum(EloDelta) from players INNER JOIN matches ON matches.tpmid=players.tpmid WHERE players.Name=:name AND matches.MatchDate>:old AND matches.MatchDate<:new",
                                   {"name": player[0], "old": olddate.replace(tzinfo=datetime.timezone.utc).timestamp(), "new": newdate.replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[
                0]
            if delta is None:
                delta = 0
            elo += delta
            row = cursor.execute("SELECT rowid,Elo FROM elos WHERE Name=:name AND Day=:day", {"name": player[0], "day": olddate.strftime("%Y-%m-%d")}).fetchone()
            if row is None:
                cursor.execute("INSERT INTO elos(Name, Elo, Day) VALUES (?,?,?)", (player[0], elo, olddate.strftime("%Y-%m-%d")))
            else:
                if row[1] != elo:
                    print("Player: " + player[0] + " Recorded Elo: " + str(row[1]) + " Extrapolated Elo: " + str(elo) + " " + olddate.strftime("%Y-%m-%d"))
                    cursor.execute("UPDATE elos SET Elo=:elo, Time=CURRENT_TIMESTAMP WHERE rowid=:rid", {"elo": elo, "rid": row[0]})
        db.commit()
    # #beginning of day 5-15-20 is greater than:
    # datetime.datetime.strptime("2020-05-15","%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp()
    # #end of day 5-15-20 is less than
    # datetime.datetime.strptime("2020-05-16","%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp()

def updateStats():
    # players = pd.read_sql_query("select * from players", db)
    players = pd.read_sql_query("select * from players inner join matches m on players.tpmid = m.tpmid where m.MatchDate>:s1", db,params={"s1":s1time})
    stats = players.groupby(['Name']).sum()
    stats['games'] = players.groupby(['Name'])['tags'].count()
    stats.index = stats.index.str.lower()
    stats['kd'] = stats['tags'] / stats['pops']
    stats['score'] = stats['captures'] / stats['grabs'] * 100
    stats['hpg'] = stats['hold'] / stats['grabs']
    stats['kf'] = stats['grabs'] - stats['captures'] - stats['drops']
    stats['losses'] = stats['games'] - stats['TeamWin']
    for (columnName, columnData) in stats.iteritems():
        stats[columnName + 'pm'] = stats[columnName] / stats['TimePlayed'] * 60
        stats[columnName + 'pg'] = stats[columnName] / stats['games']
    ranks = stats[stats.games >= 10]
    for (columnName, columnData) in ranks.iteritems():
        stats[columnName + 'rank'] = ranks[columnName].rank(pct=True)
    stats.to_sql("stats2", db, if_exists="replace", index_label="Name")
    print("Stats updated.")

def endofday():
    players=updateLeaderboard()
    updateProfiles(players)
    # updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL").fetchall())
    updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall())
    updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall())
    updateFromEU(cursor.execute("SELECT tpmid, euid FROM matches WHERE Void=0 and euid IS NOT NULL and Map IS NULL").fetchall())
    updateFromEU(cursor.execute("SELECT tpmid, euid FROM matches WHERE Void=0 and euid IS NOT NULL and Duration is null").fetchall())
    updateMatchesOverride(cursor.execute("SELECT tpmid FROM matches WHERE MatchDate IS NULL").fetchall())
    updateStreaks()
    updatePlayers(cursor.execute(
        "select tpmid, euid from matches where tpmid in (select tpmid from players where redteam is null and eumismatch is null) and Void=0 and euid is not null").fetchall())
    updateStats()


def partscrape():
    # updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL").fetchall())
    print("matches")
    updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall()[:200])
    print("eu")
    updateFromEU(cursor.execute(
        "SELECT tpmid, euid FROM matches WHERE Void=0 and euid IS NOT NULL and Map IS NULL").fetchall()[:200])
    print("players")
    updatePlayers(
        cursor.execute("select tpmid, euid from matches where tpmid in (select tpmid from players where redteam is null and eumismatch is null) and Void=0 and euid is not null").fetchall())

def scrape():
    os.system('cmd /c "gcloud compute scp REDACTED@f1-micro-free:tpm.db ."')
    global db
    db = sqlite3.connect('tpm.db')
    global cursor
    cursor = db.cursor()
    # Create table
    cursor.execute("CREATE TABLE IF NOT EXISTS profiles(Name varchar(255) PRIMARY KEY, URL varchar(255), GamesPlayed INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute("CREATE TABLE IF NOT EXISTS elos(Name varchar(255), Elo INTEGER, Day varchar(10), Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS matches(tpmid varchar(255) PRIMARY KEY, euid INTEGER, RedWin INTEGER, BlueWin INTEGER, Void INTEGER, RedElo INTEGER, BlueElo INTEGER, RedScore INTEGER, BlueScore INTEGER, Map varchar(255), Duration INTEGER, MatchDate INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS players(tpmid varchar(255), Name varchar(255), EloDelta INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(tpmid) REFERENCES matches(tpmid), FOREIGN KEY(Name) REFERENCES profiles(Name))")
    cursor.execute("CREATE TABLE IF NOT EXISTS alias(Discord INTEGER PRIMARY KEY, Name varchar(255), Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
    cursor.execute("CREATE TABLE IF NOT EXISTS streaks(Name varchar(255), Streak INTEGER, Total INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
    db.commit()
    endofday()
    cursor.close()
    db.close()
    os.system('cmd /c "gcloud compute scp tpm.db REDACTED@f1-micro-free:/home/REDACTED"')

def login():
    print("logging into tpm...")
    options = webdriver.ChromeOptions()
    # paths chrome in windows
    options.add_argument('user-data-dir=C:/Users/REDACTED/AppData/Local/Google/Chrome/User Data/Profile 3')
    # options.add_argument('--profile-directory=Default')
    # options.add_argument('--disable-gpu')
    # options.add_argument('--no-sandbox')
    # options.headless=True

    browser = webdriver.Chrome('chromedriver', options=options)  # Optional argument, if not specified will search path.
    link = (
            "https://accounts.google.com/o/oauth2/v2/auth/oauthchooseaccount"
            + "?redirect_uri=https%3A%2F%2Fdevelopers.google.com%2F"
            + "oauthplayground&prompt=consent&response_type=code"
            + "&client_id=407408718192.apps.googleusercontent.com"
            + "&scope=email&access_type=offline&flowName=GeneralOAuthFlow"
    )
    browser.get(link)
    time.sleep(45)
    browser.quit()
    print("login complete.")

scrape()
# endofday()
# updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL").fetchall())
# extrapolateElo()
# importElos()
# importAlias()
# updateLeaderboard()
# updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL").fetchall())
# # updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL AND GamesPlayed IS NULL").fetchall())
# updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall())
# # # updateFromBulkEU(cursor.execute("SELECT euid FROM matches WHERE Void=0 and euid IS NOT NULL").fetchall())
# # updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedScore IS NULL AND Void!=1").fetchall())
# updateFromEU(cursor.execute("SELECT euid FROM matches WHERE Void=0 and euid IS NOT NULL and Map IS NULL").fetchall())
# updateMatchesOverride(cursor.execute("SELECT tpmid FROM matches WHERE MatchDate IS NULL").fetchall())

