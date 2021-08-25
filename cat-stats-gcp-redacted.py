import discord
import time
import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from datetime import date
import datetime
from scipy.stats import norm
import matplotlib.pyplot as plt
import numpy as np  
import ast
import math
from operator import itemgetter
import logging
import os.path
from discord.ext import tasks, commands
import urllib.parse
import sqlite3
from tqdm import tqdm
import tagpro_eu
import matplotlib.ticker as ticker
from dateutil import parser
from dateutil import tz
import pandas as pd

s1time=1613222155
s1scrape=1613322155
s1date=datetime.datetime.strptime("2021-02-13","%Y-%m-%d")
s1id="?Season=9781c66d-cdfc-434c-a319-cef7cedb15ef"
s2id="?Season=8e1c6965-c08e-493c-9d6c-c627706eb2f6"

def opendb():
	wait=0
	while not os.path.exists('/home/REDACTED/tpm.db'):
		time.sleep(1)
		wait+=1
		print("DATABASE NOT FOUND. WAITING 1 SECOND")
		if wait>120:
			print("DATABASE NOT FOUND. TIMEOUT. EXITING.")
			exit()
	global db
	db=sqlite3.connect('/home/REDACTED/tpm.db')
	global cursor
	cursor=db.cursor()
	# Create table
	cursor.execute("CREATE TABLE IF NOT EXISTS profiles(Name varchar(255) PRIMARY KEY, URL varchar(255), GamesPlayed INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	cursor.execute("CREATE TABLE IF NOT EXISTS elos(Name varchar(255), Elo INTEGER, Day varchar(10), Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
	cursor.execute("CREATE TABLE IF NOT EXISTS matches(tpmid varchar(255) PRIMARY KEY, euid INTEGER, RedWin INTEGER, BlueWin INTEGER, Void INTEGER, RedElo INTEGER, BlueElo INTEGER, RedScore INTEGER, BlueScore INTEGER, Map varchar(255), Duration INTEGER, MatchDate INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	cursor.execute("CREATE TABLE IF NOT EXISTS players(tpmid varchar(255), Name varchar(255), EloDelta INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(tpmid) REFERENCES matches(tpmid), FOREIGN KEY(Name) REFERENCES profiles(Name))")
	cursor.execute("CREATE TABLE IF NOT EXISTS alias(Discord INTEGER PRIMARY KEY, Name varchar(255), Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
	cursor.execute("CREATE TABLE IF NOT EXISTS streaks(Name varchar(255), Streak INTEGER, Total INTEGER, Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(Name) REFERENCES profiles(Name))")
	db.commit()

opendb()

async def messageCatalyst():
	await client.get_user(REDACTED).send("Waiting for 2FA from $fix command")

def browserLogin():
	options = webdriver.ChromeOptions()
	options.add_argument('user-data-dir=/home/REDACTED/.config/google-chrome/Profile 2')
	options.add_argument('--disable-gpu')
	options.add_argument('--no-sandbox')
	options.add_argument("--window-size=1920,1080")
	options.headless = True
	browser = webdriver.Chrome('/home/REDACTED/chromedriver',
							   options=options)  # Optional argument, if not specified will search path.
	try:
		link = (
				"https://accounts.google.com/o/oauth2/v2/auth/oauthchooseaccount"
				+ "?redirect_uri=https%3A%2F%2Fdevelopers.google.com%2F"
				+ "oauthplayground&prompt=consent&response_type=code"
				+ "&client_id=407408718192.apps.googleusercontent.com"
				+ "&scope=email&access_type=offline&flowName=GeneralOAuthFlow"
		)
		browser.get(link)
		try:
			emailElem = browser.find_element_by_id('Email')
		except Exception as e:
			emailElem = None
			print("google error:", e)
		try:
			account = browser.find_element_by_id("choose-account-0")
			account.click()
			time.sleep(1)
		except Exception as e:
			print("google error:", e)
		if emailElem:
			emailElem.send_keys('REDACTED')
		allowButton = browser.find_element_by_id('submit_approve_access')
		if allowButton:
			allowButton.click()
			time.sleep(1)
		else:
			nextButton = browser.find_element_by_id('next')  # .find_element_by_css_selector('button')
			if nextButton:
				nextButton.click()
				time.sleep(1)
				capElem = browser.find_elements_by_id('identifier-captcha-input')
				if len(capElem):
					browser.save_screenshot("cap.png")
					messageCatalyst()
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
				messageCatalyst()
				time.sleep(20)
				browser.save_screenshot("ss4.png")
				print("finished waiting")
	except Exception as e:
		print("google error:", e)

	login = "https://www.tpm.gg/Account/Login"
	browser.get(login)
	return browser

def updateStreaks():
	print("Updating streaks...")
	players=cursor.execute("SELECT DISTINCT Name from players").fetchall()
	for user in tqdm(players):
		data=cursor.execute("SELECT EloDelta, matches.void from players INNER JOIN matches on matches.tpmid=players.tpmid where Name=:name and matches.MatchDate>:s1 order by matches.MatchDate",{"name":user[0],"s1":s1time}).fetchall()
		loss=0
		win=0
		void=0
		greenstreaks=[]
		redstreaks=[]
		currentstreak=0
		winstreak=0
		lossstreak=0
		last=-1
		for row in data:
			if row[0]<0 and void==0:
				loss+=1
				if last==1:
					lossstreak=1
					greenstreaks.append(winstreak)
					winstreak=0
				else:
					lossstreak+=1
				last=0     
			elif row[0]>0:
				win+=1
				if last==0:
					winstreak=1
					redstreaks.append(lossstreak)
					lossstreak=0
				else:
					winstreak+=1
				last=1    
			elif row[0]<0 and void==1:
				void+=1
				if last==1:
					greenstreaks.append(winstreak)
					winstreak=0
					lossstreak=0
				elif last==0:
					redstreaks.append(lossstreak)
					winstreak=0
					lossstreak=0
				last=-1
		if last==1:
			greenstreaks.append(winstreak)
			currentstreak=winstreak
		elif last==0:
			redstreaks.append(lossstreak)
			currentstreak=-lossstreak
		#reset table
		cursor.execute("UPDATE streaks SET Total=0, Time=CURRENT_TIMESTAMP WHERE Name=:name",{"name":user[0]})
		#fill table
		for win in greenstreaks:
			row=cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=:streak",{"name":user[0],"streak":win}).fetchone()
			if row is not None:
				cursor.execute("UPDATE streaks SET Total=Total+1, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"rid":row[0]})
			else:
				cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,?,1)",(user[0],win))
		for loss in redstreaks:
			row=cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=:streak",{"name":user[0],"streak":-loss}).fetchone()
			if row is not None:
				cursor.execute("UPDATE streaks SET Total=Total+1, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"rid":row[0]})
			else:
				cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,?,1)",(user[0],-loss))
		#set current streak
		row=cursor.execute("SELECT rowid FROM streaks WHERE Name=:name AND Streak=0",{"name":user[0]}).fetchone()
		if row is not None:
			cursor.execute("UPDATE streaks SET Total=:current, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"rid":row[0],"current":currentstreak})
		else:
			cursor.execute("INSERT INTO streaks(Name, Streak, Total) VALUES (?,0,?)",(user[0],currentstreak))
	db.commit()
	print("Update complete.")

def updateLeaderboard():
	print("Updating leaderboard...")
	url="https://www.tpm.gg/Leaderboard/CTFNA"
	r=requests.get(url)
	next=True
	currentpage=1
	lastpage=1
	leaderboard=[]
	print("Scraping leaderboard...")
	#scrape name, Elo, and tpm URL
	while next and currentpage<30:#default 30, currently 19
		soup = BeautifulSoup(r.text, 'html.parser')
		for a in soup.findAll('a', attrs={'class': 'leaderboardText'}):
			data=[x.strip() for x in [a.text.strip().rsplit('(',1)[0],a.text.strip().rsplit('(',1)[1].strip(')')]]
			data.append(a['href'])
			leaderboard.append(data)
		for pageindex in soup.findAll('a', attrs={'class':'userProfilePagers'}):
			if int(pageindex.text)>lastpage:
				lastpage=int(pageindex.text)
				r=requests.get(url+'?page='+pageindex.text)
				next=True
				break
		if currentpage>=lastpage:
			next=False
		currentpage+=1
	print("Updating elos database...")
	for user in tqdm(leaderboard):
		#update #elos
		row=cursor.execute("SELECT rowid FROM elos WHERE Name=:name AND Day=date('now')",{"name":user[0]}).fetchone()
		if row is not None:
			cursor.execute("UPDATE elos SET Elo=:elo, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"elo":user[1],"rid":row[0]})
		else:
			cursor.execute("INSERT INTO elos(Name, Elo, Day) VALUES (?,?,date('now'))",(user[0],user[1]))
		#add new user to #profiles
		cursor.execute("INSERT INTO profiles(Name, URL) VALUES (?,?) ON CONFLICT DO NOTHING",(user[0],user[2]))
	db.commit()
	print("Update complete.")
	# discord.utils.get(client.get_guild(597561804662767627).text_channels, name='bot-spam').send('Elo update complete at '+str(datetime.datetime.now()))

def updateProfiles(players):
	print("Updating profiles...")
	browser=browserLogin()
	print("Scraping %d profiles..." % (len(players)))
	for name, url in tqdm(players):
		browser.get("https://www.tpm.gg"+url+s2id)
		next=True
		matches=[]
		currentpage=1
		lastpage=1
		if len(browser.find_elements_by_class_name("matchHistoryTitle"))>0:
			while next and currentpage<100:
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
				games=browser.execute_script("return myChart.titleBlock.options.text")
				if ' | ' in games:
					games=games.split('|')[1].split()[0]
				else:
					games=games.split()[1]
				if int(games)>0:
					cursor.execute("UPDATE profiles SET GamesPlayed=:games, Time=CURRENT_TIMESTAMP WHERE Name=:name",{"name":name,"games":games})
			except:
				print("No matches found "+name)
			#add new match to #matches
			for match in matches:
				cursor.execute("INSERT INTO matches(tpmid) VALUES (:id) ON CONFLICT DO NOTHING",{"id":match})
			if len(matches):
				print("Found %d new matches" % (len(matches)))
			db.commit()
	browser.quit()
	print("Update complete.")

def updateMatchesOverride(matches):
	print("Scraping override match data...")
	browser=browserLogin()
	print("Scraping %d matches..." % (len(matches)))
	for match in tqdm(matches):
		tpmid=match[0]
		browser.get("https://www.tpm.gg/Match/"+tpmid)
		soup = BeautifulSoup(browser.page_source, 'html.parser')
		start=int(parser.parse(soup.find('input', {'id': 'StartTime'}).get('value')).replace(tzinfo=datetime.timezone.utc).timestamp())
		mapid=soup.find('img', {'class': 'mapImage'}).get('src')
		if "previews/" in mapid:
			mapid=mapid.split("previews/")[1].split('.')[0]
			soup = BeautifulSoup(requests.get("http://unfortunate-maps.jukejuice.com/show/"+mapid).text, 'html.parser')
			mapname=soup.find('h2', {'class': 'searchable'}).text
		else:
			mapname=None
		cursor.execute("UPDATE matches SET MatchDate=:start, Map=:mapname, Time=CURRENT_TIMESTAMP WHERE tpmid=:tpmid",{"tpmid":tpmid,"start":start,"mapname":mapname})
	db.commit()
	browser.quit()
	print("Update complete.")

def updateMatches(matches):
	print("Updating matches...")
	print("Scraping %d matches..." % (len(matches)))
	newplayers=[]
	browser = browserLogin()
	for match in tqdm(matches):
		players=[]
		eu=None
		redwin=None
		bluewin=None
		void=None
		redscore=None
		bluescore=None
		tpmid=match[0]
		browser.get("https://www.tpm.gg/Match/" + tpmid)
		#scrape name, Elo, and tpm URL
		if len(browser.page_source):
			soup = BeautifulSoup(browser.page_source, 'html.parser')
			red=soup.find('div',attrs={'class': 'teamContainerRed'})
			redelo=red.h1.text.strip().split('(')[1].strip(')')
			for player in red.findAll('a'):
				#player url, player name, elo delta
				spans=player.h4.findAll('span')
				players.append([player['href'],spans[0].text.strip(),spans[1].text[1:-1]])
			blue=soup.find('div',attrs={'class': 'teamContainerBlue'})
			blueelo=blue.h1.text.strip().split('(')[1].strip(')')
			for player in blue.findAll('a'):
				#player url, player name, elo delta
				spans=player.h4.findAll('span')
				players.append([player['href'],spans[0].text.strip(),spans[1].text[1:-1]])
			if soup.find('h3',attrs={'class': 'resultRedWin'}):
				redwin=1
				bluewin=0
				void=0
				if soup.find('div',attrs={'class': 'basicContainer'}).h3.text!="Match Found":
					eu=soup.find('div',attrs={'class': 'basicContainer'}).a['href']
				scores=soup.findAll('h1',attrs={'class': 'resultScore'})
				redscore=scores[0].text
				bluescore=scores[1].text
			elif soup.find('h3',attrs={'class': 'resultBlueWin'}):
				redwin=0
				bluewin=1
				void=0
				if soup.find('div',attrs={'class': 'basicContainer'}).h3.text!="Match Found":
					eu=soup.find('div',attrs={'class': 'basicContainer'}).a['href']
				scores=soup.findAll('h1',attrs={'class': 'resultScore'})
				redscore=scores[0].text
				bluescore=scores[1].text
			elif soup.find('h3',attrs={'class': 'resultVoid'}):
				redwin=0
				bluewin=0
				void=1
			else:
				print("Unknown Match Result!")

			cursor.execute("UPDATE matches SET euid=:euid, RedWin=:redwin, BlueWin=:bluewin, Void=:void, RedElo=:redelo, BlueElo=:blueelo, RedScore=:redscore, BlueScore=:bluescore, Time=CURRENT_TIMESTAMP WHERE tpmid=:tpmid",{"tpmid":tpmid,"euid":eu,"redwin":redwin,"bluewin":bluewin,"void":void,"redelo":redelo,"blueelo":blueelo,"redscore":redscore,"bluescore":bluescore})
			#update #players
			for player in players:
				playerexists=cursor.execute("SELECT rowid FROM profiles WHERE Name=:name",{"name":player[1]}).fetchone()
				if playerexists is None:
					cursor.execute("INSERT INTO profiles(Name, URL) VALUES (?,?)",(player[1],player[0]))
					newplayers.append([player[1],player[0]])
				row=cursor.execute("SELECT rowid FROM players WHERE tpmid=:tpmid AND Name=:name",{"name":player[1],"tpmid":tpmid}).fetchone()
				if row is not None:
					cursor.execute("UPDATE players SET EloDelta=:delta, Time=CURRENT_TIMESTAMP WHERE rowid=:rid",{"delta":player[2],"rid":row[0]})
				else:
					cursor.execute("INSERT INTO players(tpmid, Name, EloDelta) VALUES (?,?,?)",(tpmid,player[1],player[2]))
			db.commit()
	if len(newplayers):
		print("Found new profiles: ")
		print(newplayers)
		updateProfiles(newplayers)
	print("Update complete.")

def updateFromEU(dbmatches):
	print("Updating match database from tagpro.eu")
	for match in tqdm(dbmatches):
		if len(match[0])>25:
			eu=match[0].split('=')[1]
			try:
				eumatch=tagpro_eu.download_match(eu)
				mapname=eumatch.map.name
				duration=int(eumatch.duration)
				epoch=int(eumatch.date.timestamp())
				cursor.execute("UPDATE matches SET Map=:mapname, Duration=:duration, MatchDate=:epoch, Time=CURRENT_TIMESTAMP WHERE euid=:euid",{"euid":match[0],"mapname":mapname,"duration":duration,"epoch":epoch})
			except:
				print("eu not found! "+eu)
	db.commit()
	print("Update complete.")

def importAlias():
	if os.path.exists('alias_tpm.txt'):
		with open('alias_tpm.txt', 'r') as filehandle:
			aliaslist=[ast.literal_eval(line) for line in filehandle]
		for row in aliaslist:
			cursor.execute("INSERT INTO alias(Discord, Name, Time) VALUES(:discord,:name,CURRENT_TIMESTAMP) ON CONFLICT(Discord) DO UPDATE SET Name=excluded.Name,Time=CURRENT_TIMESTAMP",{"discord":row[0],"name":row[1]})
		db.commit()

def updateStats():
	players = pd.read_sql_query("select * from players", db)
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
	stats.to_sql("stats", db, if_exists="replace", index_label="Name")
	print("Stats updated.")

def endofday():
	updateLeaderboard()
	updateProfiles(cursor.execute("SELECT Name, URL FROM profiles WHERE URL IS NOT NULL").fetchall())
	updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall())
	updateMatches(cursor.execute("SELECT tpmid FROM matches WHERE RedWin IS NULL").fetchall())
	updateFromEU(cursor.execute("SELECT euid FROM matches WHERE Void=0 and euid IS NOT NULL and Map IS NULL").fetchall())
	updateMatchesOverride(cursor.execute("SELECT tpmid FROM matches WHERE MatchDate IS NULL").fetchall())
	updateStreaks()
	updateStats()

def loadeu(url):
    try:
        return tagpro_eu.download_match(url)
    except Exception as e:
        print("error:", e)
        return None
def checkTPM(url):
    if "tpm.gg/Match/" in str(url):
        tpmid = url.split('/Match/')[1]
        return "https://www.tpm.gg/Match/" + tpmid,tpmid
    else:
        return None

def getTPMPlayers(soup):
    players = []
    red = soup.find('div', attrs={'class': 'teamContainerRed'})
    if red:
        for player in red.findAll('a'):
            spans = player.h4.findAll('span')
            players.append(spans[0].text.strip().casefold())
        blue = soup.find('div', attrs={'class': 'teamContainerBlue'})
        for player in blue.findAll('a'):
            spans = player.h4.findAll('span')
            players.append(spans[0].text.strip().casefold())
    else:
        red = soup.find('div', attrs={'class': 'eggballTeamContainerRed'})
        for player in red.findAll('a'):
            spans = player.h4.findAll('span')
            players.append(spans[0].text.strip().casefold())
        blue = soup.find('div', attrs={'class': 'eggballTeamContainerBlue'})
        for player in blue.findAll('a'):
            spans = player.h4.findAll('span')
            players.append(spans[0].text.strip().casefold())
    return players

def checkStart(soup,eumatch):
    if soup.find('input', {'id': 'StartTime'}) is not None:
        start = int(parser.parse(soup.find('input', {'id': 'StartTime'}).get('value')).replace(tzinfo=datetime.timezone.utc).timestamp())
    else:
        start = 0
    eustart = int(eumatch.date.timestamp())
    return abs(eustart - start) < 2000

def checkMap(soup,eumatch,egg):
    if egg:
        mapname="Egg Ball"
    else:
        mapid = soup.find('img', {'class': 'mapImage'}).get('src')
        if "previews/" in mapid:
            mapid = mapid.split("previews/")[1].split('.')[0]
            soupmaps = BeautifulSoup(requests.get("http://unfortunate-maps.jukejuice.com/show/" + mapid).text, 'html.parser')
            mapname = soupmaps.find('h2', {'class': 'searchable'}).text
        else:
            mapname = None
    eumapname = eumatch.map.name
    return eumapname == mapname or mapname is None

def applyOverride(eumatch,browser,tpmid,euurl):
    try:
        if eumatch.map.name == 'Open Field Masters':
            redscore = eumatch.team_red.stats.hold.seconds
            bluescore = eumatch.team_blue.stats.hold.seconds
        else:
            redscore = eumatch.team_red.score
            bluescore = eumatch.team_blue.score
        override = browser.find_element_by_id('overrideIdInput')
        override.send_keys(tpmid)
        override.send_keys(Keys.ENTER)
        select = Select(browser.find_element_by_id('Result'))
        if redscore > bluescore:
            select.select_by_value('0')
        else:
            select.select_by_value('1')
        browser.find_element_by_id('RedScore').clear()
        browser.find_element_by_id('RedScore').send_keys(redscore)
        browser.find_element_by_id('BlueScore').clear()
        browser.find_element_by_id('BlueScore').send_keys(bluescore)
        browser.find_element_by_id('EUId').send_keys(euurl.split("match=", 1)[1])
        browser.find_element_by_class_name('btn-info').click()
        return True
    except Exception as e:
        print("error:", e)
        return False

logger = logging.getLogger('discord')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

client = discord.Client()

@tasks.loop(minutes=15)
async def updateloop():
	if (datetime.datetime.now()-datetime.timedelta(hours=5)).hour>=21 or (datetime.datetime.now()-datetime.timedelta(hours=5)).hour<=3:
		updateLeaderboard()
		await discord.utils.get(client.get_guild(597561804662767627).text_channels, name='bot-spam').send('Elo update complete at '+str(datetime.datetime.now()))
		print('Elo update complete at '+str(datetime.datetime.now()))

@updateloop.before_loop
async def before():
	await client.wait_until_ready()

@client.event
async def on_ready():
	print('We have logged in as {0.user}'.format(client))
	await discord.utils.get(client.get_guild(597561804662767627).text_channels, name='bot-spam').send("I'm back online! Use $help to see the list of available commands.")
	updateloop.start()

@client.event
async def on_message(message):
	# print('Found a message')
	if message.author==client.user:
		return

	mods=[REDACTED,218568379491942411,98487260940349440,144366661519147008,276284470586507265,752952301538246666,513575602163548170,253882641181310976,252483379415482368,327676810940645378,165934121720217600,457705263529590805,250307395975118858,297842360186961923,248293176144691201]
	if not (isinstance(message.channel,discord.DMChannel)) and message.channel.name=='bugged-games' and message.content.startswith('$fix '):
		if len(message.content.split()) == 3:
			status = await message.channel.send("Processing...")
			tpmurl, euurl = message.content.split(None, 2)[1:]
			eumatch = loadeu(euurl)
			if eumatch is not None:
				url, tpmid = checkTPM(tpmurl)
				if url is not None:
					await status.edit(content="Loading TPM match info...")
					browser = browserLogin()
					browser.get(url)
					teams = BeautifulSoup(browser.page_source, 'html.parser')
					found = teams.find('div', attrs={'class': 'basicContainer'}).h3.text
					if found == "Match Found" or found == "Match Voided":
						players = getTPMPlayers(teams)
						euplayers = [p.name.casefold() for p in eumatch.players]
						if sorted(euplayers) == sorted(players):
							html = browser.find_element_by_tag_name('html')
							html.send_keys(Keys.END)
							if checkMap(teams, eumatch, len(players) == 12):
								if checkStart(teams, eumatch):
									await status.edit(content="All checks complete! Applying override...")
									if applyOverride(eumatch, browser, tpmid, euurl):
										await status.edit(content="Override complete!")
										await message.delete(delay=10)
										await status.delete(delay=10)
									else:
										await status.edit(content="Error: All checks complete, but I'm unable to apply the override '\U0001F63F'")
								else:
									await status.edit(content="Error: start times don't match")
							else:
								await status.edit(content="Error: maps don't match!")
						else:
							await status.edit(content="Error: players don't match")
					else:
						await status.edit(content="Error: eu already assigned")
						await message.delete(delay=10)
						await status.delete(delay=10)
					if browser is not None:
						browser.quit()
				else:
					await status.edit(content="Error: invalid TPM match url: " + tpmurl)
			else:
				await status.edit(content="Error: invalid eu url: " + euurl)
		else:
			await message.channel.send("Error: wrong format: $fix [TPM url] [eu url]")


	if not (isinstance(message.channel,discord.DMChannel)) and message.channel.name=='bugged-games' and message.content.startswith('$fixtime') and int(message.author.id) in mods:
		if len(message.content.split()) == 3:
			status = await message.channel.send("Processing...")
			tpmurl, euurl = message.content.split(None, 2)[1:]
			eumatch = loadeu(euurl)
			if eumatch is not None:
				url, tpmid = checkTPM(tpmurl)
				if url is not None:
					await status.edit(content="Loading TPM match info...")
					browser = browserLogin()
					browser.get(url)
					teams = BeautifulSoup(browser.page_source, 'html.parser')
					found = teams.find('div', attrs={'class': 'basicContainer'}).h3.text
					if found == "Match Found" or found == "Match Voided":
						players = getTPMPlayers(teams)
						euplayers = [p.name.casefold() for p in eumatch.players]
						if sorted(euplayers) == sorted(players):
							html = browser.find_element_by_tag_name('html')
							html.send_keys(Keys.END)
							if checkMap(teams, eumatch, len(players) == 12):
								await status.edit(content="All checks complete! Applying override...")
								if applyOverride(eumatch, browser, tpmid, euurl):
									await status.edit(content="Override complete!")
									await message.delete(delay=10)
									await status.delete(delay=10)
								else:
									await status.edit(content="Error: All checks complete, but I'm unable to apply the override '\U0001F63F'")
							else:
								await status.edit(content="Error: maps don't match!")
						else:
							await status.edit(content="Error: players don't match")
					else:
						await status.edit(content="Error: eu already assigned")
						await message.delete(delay=10)
						await status.delete(delay=10)
					if browser is not None:
						browser.quit()
				else:
					await status.edit(content="Error: invalid TPM match url: " + tpmurl)
			else:
				await status.edit(content="Error: invalid eu url: " + euurl)
		else:
			await message.channel.send("Error: wrong format: $fix [TPM url] [eu url]")


	if not (isinstance(message.channel,discord.DMChannel) or message.channel.name=='bot-spam' or message.channel.name=='cat-stats' or message.channel.name=='verification'):
		return

	#plot player(s) Elo
	if message.content.startswith('$help'):							
		await message.channel.send('\u200b\nList of available commands:\n\tData updated every 15 minutes:\n\t$elo [player] .... returns current Elo of player or author if no player specified\n\t'+
									'$deltaN [player] .... N optional, returns 1 or N day delta of player or author if no player specified, where N=[1,200] or all\n\t'+
									'$rangeN [player] .... N optional, returns 31 or N day Elo range of player or author if no player specified, where N=[1,200] or all\n\t'+
									'$averageN [player] .... N optional, returns 31 or N day Elo average of player or author if no player specified, where N=[1,200] or all\n\t'+
									'$plotN [player, player,...] .... N optional, returns 31 or N day plot of the end of day Elos of listed players where N=[1,200] or all\n\t'+
									'$rankN [player, player,...] .... N optional, returns 31 or N day plot of the end of day ranks of listed players where N=[1,200] or all\n\t'+
									'$leadersN .... N optional, returns list of top 3 players with best and worst 1 or N day deltas, where N=[1,200] or all\n\t'+
									'The above commands also accept \'all\' as input for N, e.g. $plotall Catalyst\n\t'+
									'$topN .... N optional, returns the top 3 or N players on the leaderboard, where N=[3,10]\n\t'+
									'$bottom .... returns the bottom 3 players on the leaderboard\n\t'+
									'Data updated daily:\n\t'+
								   	'$stats [player] .... returns TPM statistics for player or author if no player specified\n\t' +
								   	'$rates [player] .... returns TPM statistical rates for player or author if no player specified\n\t' +
									'$streaks [player] .... returns best, worst, and current streaks for player or author if no player specified\n\t'+
									'$streaksN [player] .... returns number of streaks with N or more wins/losses in a row for player or author if no player specified, where N=[1,50]\n\t'+
									'$winstreaksN .... N optional, returns the top 3 or N players with the longest active win streaks, where N=[3,10]\n\t'+
		 							'$lossstreaksN .... N optional, returns the top 3 or N players with the longest active loss streaks, where N=[3,10]')
		await message.channel.send('\u200b\n\t$activewinstreaksN .... N optional, returns the top 3 or N players with the longest active win streaks, where N=[3,10]\n\t'+
		 							'$activelossstreaksN .... N optional, returns the top 3 or N players with the longest active loss streaks, where N=[3,10]\n\t'+
									'$gamesN .... N optional, returns the top 3 or N players in number of games played, where N=(3,10)\n\t'+
								   	'$maps [mapname] .... mapname optional, returns the top 5 maps played in TPM or stats of the specified map\n\t'+
									'$time .... returns time my Elo database was last updated\n\t'+
									'$alias [name] .... sets your alias for self-referencing commands. Useful for players with different Discord and TPM names.')
		await message.channel.send('Elo database is updated every 15 minutes by default.\nPlayers must have 10 games played on TPM in order to be found.\nCapitalization does not matter for player names.\nIf no player specified in commands, your Discord name must match your TPM name or fix it with $alias.')
	if "tpm.verify" in message.content:
		await message.channel.send("Send your verification code in a direct message to <@280101682044731393>")


	#print player stats
	if message.content.startswith('$stats'):
		if len(message.content.split(None, 1)) > 1:
			player = message.content.split(None, 1)[1]
		else:
			player = message.author.display_name
			alias = cursor.execute("SELECT Name from alias where Discord=:user",
								   {"user": message.author.id}).fetchone()
			if alias is not None:
				player = alias[0]
		player = player.lower()

		def duration(secs):
			if secs > 0:
				secs = int(secs)
				return "%d:%02d:%02d" % (secs // 3600, secs % 3600 // 60, secs % 3600 % 60)
			else:
				return "0:00:00"

		if cursor.execute("SELECT EXISTS(SELECT 1 FROM stats2 WHERE Name=:name COLLATE nocase)",
						  {"name": player}).fetchone()[0]:
			profile = cursor.execute("select name, URL from profiles where name =:name collate nocase",
									 {"name": player}).fetchone()
			stats = pd.read_sql_query("SELECT * from stats2", db, index_col="Name")
			# players = pd.read_sql_query("select * from players", db)
			# stats = players.groupby(['Name']).sum()
			# stats['games'] = players.groupby(['Name'])['tags'].count()
			# stats.index = stats.index.str.lower()
			# if stats.loc[player]['games'] >= 10:
			# 	rankeligible = True
			# else:
			# 	rankeligible = False
			# stats['kd'] = stats['tags'] / stats['pops']
			# stats['score'] = stats['captures'] / stats['grabs'] * 100
			# stats['hpg'] = stats['hold'] / stats['grabs']
			# stats['kf'] = stats['grabs'] - stats['captures'] - stats['drops']
			# stats['losses'] = stats['games'] - stats['TeamWin']
			# if rankeligible:
			# 	stats = stats[stats.games >= 10]
			# 	for (columnName, columnData) in stats.iteritems():
			# 		stats[columnName + 'rank'] = stats[columnName].rank(pct=True)
			# else:
			# 	for (columnName, columnData) in stats.iteritems():
			# 		stats[columnName + 'rank'] = 0
			if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",
							  {"name": player}).fetchone()[0]:
				recent = \
					cursor.execute("SELECT Day from elos where Name=:name COLLATE nocase ORDER BY Day Desc LIMIT 1",
								   {"name": player}).fetchone()[0]
				user = cursor.execute(
					"SELECT * from (select Name, Elo, rank () over (order by Elo Desc), percent_rank() over (order by Elo) from elos where Day=:day) where Name=:name COLLATE nocase",
					{"day": recent, "name": player}).fetchone()
				statembed = discord.Embed(title=profile[0], url="https://www.tpm.gg" + profile[1],
										  description='Elo: ' + str(user[1]) + ', rank: ' + str(
											  user[2]) + ' (' + str(round(user[3] * 100, 1)) + '%)')
			else:
				statembed = discord.Embed(title=profile[0], url="https://www.tpm.gg" + profile[1],
										  description='Elo calibrating')
			statembed.add_field(name='Tags', value=f"{stats.loc[player]['tags']:.0f}\n({stats.loc[player]['tagsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Pops', value=f"{stats.loc[player]['pops']:.0f}\n({stats.loc[player]['popsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='K/D', value=f"{stats.loc[player]['kd']:.2f}\n({stats.loc[player]['kdrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Returns', value=f"{stats.loc[player]['returns']:.0f}\n({stats.loc[player]['returnsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Captures', value=f"{stats.loc[player]['captures']:.0f}\n({stats.loc[player]['capturesrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Prevent', value=f"{duration(stats.loc[player]['prevent'])}\n({stats.loc[player]['preventrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Score %', value=f"{stats.loc[player]['score']:.2f}\n({stats.loc[player]['scorerank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Grabs', value=f"{stats.loc[player]['grabs']:.0f}\n({stats.loc[player]['grabsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Drops', value=f"{stats.loc[player]['drops']:.0f}\n({stats.loc[player]['dropsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Hold', value=f"{duration(stats.loc[player]['hold'])}\n({stats.loc[player]['holdrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Hold/Grab', value=f"{stats.loc[player]['hpg']:.2f}\n({stats.loc[player]['hpgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Total Pups', value=f"{stats.loc[player]['TotalPups']:.0f}\n({stats.loc[player]['TotalPupsrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='TagPros', value=f"{stats.loc[player]['tagpropup']:.0f}\n({stats.loc[player]['tagpropuprank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Juke Juices', value=f"{stats.loc[player]['jukejuicepup']:.0f}\n({stats.loc[player]['jukejuicepuprank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Rolling Bombs', value=f"{stats.loc[player]['rollingpup']:.0f}\n({stats.loc[player]['rollingpuprank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Cap Diff', value=f"{stats.loc[player]['CapDiff']:.0f}\n({stats.loc[player]['CapDiffrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Caps For', value=f"{stats.loc[player]['CapsFor']:.0f}\n({stats.loc[player]['CapsForrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Caps Against', value=f"{stats.loc[player]['CapsAgainst']:.0f}\n({stats.loc[player]['CapsAgainstrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Block', value=f"{duration(stats.loc[player]['block'])}\n({stats.loc[player]['blockrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Button', value=f"{duration(stats.loc[player]['button'])}\n({stats.loc[player]['buttonrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Kept Flags', value=f"{stats.loc[player]['kf']:.0f}\n({stats.loc[player]['kfrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Games', value=f"{stats.loc[player]['games']:.0f}\n({stats.loc[player]['gamesrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Wins', value=f"{stats.loc[player]['TeamWin']:.0f}\n({stats.loc[player]['TeamWinrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Losses', value=f"{stats.loc[player]['losses']:.0f}\n({stats.loc[player]['lossesrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Time Played', value=f"{duration(stats.loc[player]['TimePlayed'])}\n({stats.loc[player]['TimePlayedrank'] * 100:.1f}%)", inline=True)
			await message.channel.send(embed=statembed)
		else:
			await message.channel.send('Player not found in NA CTF S2 or wrong format: $stats player')

	if message.content.startswith('$rates'):
		if len(message.content.split(None, 1)) > 1:
			player = message.content.split(None, 1)[1]
		else:
			player = message.author.display_name
			alias = cursor.execute("SELECT Name from alias where Discord=:user",
								   {"user": message.author.id}).fetchone()
			if alias is not None:
				player = alias[0]
		player = player.lower()

		def duration(secs):
			if secs > 0:
				secs = int(secs)
				return "%d:%02d:%02d" % (secs // 3600, secs % 3600 // 60, secs % 3600 % 60)
			else:
				return "0:00:00"

		if cursor.execute("SELECT EXISTS(SELECT 1 FROM stats2 WHERE Name=:name COLLATE nocase)",
						  {"name": player}).fetchone()[0]:
			profile = cursor.execute("select name, URL from profiles where name =:name collate nocase",
									 {"name": player}).fetchone()
			stats = pd.read_sql_query("SELECT * from stats2", db, index_col="Name")
			# players = pd.read_sql_query("select * from players", db)
			# stats = players.groupby(['Name']).sum()
			# stats['games'] = players.groupby(['Name'])['tags'].count()
			# stats.index = stats.index.str.lower()
			# if stats.loc[player]['games'] >= 10:
			# 	rankeligible = True
			# else:
			# 	rankeligible = False
			# stats['hpg'] = stats['hold'] / stats['grabs']
			# stats['kd'] = stats['tags'] / stats['pops']
			# stats['score'] = stats['captures'] / stats['grabs'] * 100
			# stats['hpg'] = stats['hold'] / stats['grabs']
			# stats['kf'] = stats['grabs'] - stats['captures'] - stats['drops']
			# stats['losses'] = stats['games'] - stats['TeamWin']
			# for (columnName, columnData) in stats.iteritems():
			# 	stats[columnName + 'pm'] = stats[columnName] / stats['TimePlayed'] * 60
			# 	stats[columnName + 'pg'] = stats[columnName] / stats['games']
			# if rankeligible:
			# 	stats = stats[stats.games >= 10]
			# 	for (columnName, columnData) in stats.iteritems():
			# 		stats[columnName + 'rank'] = stats[columnName].rank(pct=True)
			# else:
			# 	for (columnName, columnData) in stats.iteritems():
			# 		stats[columnName + 'rank'] = 0

			if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",
							  {"name": player}).fetchone()[0]:
				recent = \
					cursor.execute("SELECT Day from elos where Name=:name COLLATE nocase ORDER BY Day Desc LIMIT 1",
								   {"name": player}).fetchone()[0]
				user = cursor.execute(
					"SELECT * from (select Name, Elo, rank () over (order by Elo Desc), percent_rank() over (order by Elo) from elos where Day=:day) where Name=:name COLLATE nocase",
					{"day": recent, "name": player}).fetchone()
				statembed = discord.Embed(title=profile[0], url="https://www.tpm.gg" + profile[1],
										  description='Elo: ' + str(user[1]) + ', rank: ' + str(
											  user[2]) + ' (' + str(round(user[3] * 100, 1)) + '%)')
			else:
				statembed = discord.Embed(title=profile[0], url="https://www.tpm.gg" + profile[1],
										  description='Elo calibrating')
			statembed.add_field(name='Tags/min', value=f"{stats.loc[player]['tagspm']:.2f}\n({stats.loc[player]['tagspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Pops/min', value=f"{stats.loc[player]['popspm']:.2f}\n({stats.loc[player]['popspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Returns/min', value=f"{stats.loc[player]['returnspm']:.2f}\n({stats.loc[player]['returnspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Grabs/min', value=f"{stats.loc[player]['grabspm']:.2f}\n({stats.loc[player]['grabspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Drops/min', value=f"{stats.loc[player]['dropspm']:.2f}\n({stats.loc[player]['dropspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Captures/min', value=f"{stats.loc[player]['capturespm']:.2f}\n({stats.loc[player]['capturespmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Prevent/min', value=f"{stats.loc[player]['preventpm']:.2f}\n({stats.loc[player]['preventpmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Hold/min', value=f"{stats.loc[player]['holdpm']:.2f}\n({stats.loc[player]['holdpmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Pups/min', value=f"{stats.loc[player]['TotalPupspm']:.2f}\n({stats.loc[player]['TotalPupspmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Cap Diff/min', value=f"{stats.loc[player]['CapDiffpm']:.3f}\n({stats.loc[player]['CapDiffpmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Block/min', value=f"{stats.loc[player]['blockpm']:.2f}\n({stats.loc[player]['blockpmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Button/min', value=f"{stats.loc[player]['buttonpm']:.2f}\n({stats.loc[player]['buttonpmrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Tags/game', value=f"{stats.loc[player]['tagspg']:.2f}\n({stats.loc[player]['tagspgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Pops/game', value=f"{stats.loc[player]['popspg']:.2f}\n({stats.loc[player]['popspgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Returns/game', value=f"{stats.loc[player]['returnspg']:.2f}\n({stats.loc[player]['returnspgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Grabs/game', value=f"{stats.loc[player]['grabspg']:.2f}\n({stats.loc[player]['grabspgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Captures/game', value=f"{stats.loc[player]['capturespg']:.2f}\n({stats.loc[player]['capturespgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Prevent/game',
								value=f"{int(stats.loc[player]['preventpg']) // 60:2d}:{int(stats.loc[player]['preventpg']) % 3600 % 60:02d}\n({stats.loc[player]['preventpgrank'] * 100:.1f}%)",
								inline=True)
			statembed.add_field(name='Hold/game',
								value=f"{int(stats.loc[player]['holdpg']) // 60:2d}:{int(stats.loc[player]['holdpg']) % 3600 % 60:02d}\n({stats.loc[player]['holdpgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Pups/game', value=f"{stats.loc[player]['TotalPupspg']:.2f}\n({stats.loc[player]['TotalPupspgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Cap Diff/game', value=f"{stats.loc[player]['CapDiffpg']:.3f}\n({stats.loc[player]['CapDiffpgrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Games', value=f"{stats.loc[player]['games']:.0f}\n({stats.loc[player]['gamesrank'] * 100:.1f}%)", inline=True)
			statembed.add_field(name='Time Played', value=f"{duration(stats.loc[player]['TimePlayed'])}\n({stats.loc[player]['TimePlayedrank'] * 100:.1f}%)", inline=True)
			await message.channel.send(embed=statembed)
		else:
			await message.channel.send('Player not found in NA CTF S2 or wrong format: $rates player')

	#check most commonly matched players
	if message.content.startswith('$players'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		players=cursor.execute("SELECT Name, count(*) c from players where tpmid in (select players.tpmid from players inner join matches on matches.tpmid=players.tpmid where void=0 and players.name=:name collate nocase) group by Name order by c desc",{"name":player}).fetchall()
		if len(players)>0:
			text="%s has played with %d players. Most common players:\n" %(players[0][0],len(players)-1)
			for user in players[1:11]:
				text+="%s, %d games\n" %(user[0],user[1])
			await message.channel.send(text)
		else:
			await message.channel.send('Player not found or wrong format: $players player')
	#check head to head records
	if message.content.startswith('$h2h'):
		await message.channel.send('Check out the new tpm.gg shop for this feature.')
	#check top 3 on the games played leaderboard
	if message.content.startswith('$leaders'):
		if len(message.content.split(None,1)[0])>8:
			ndays=message.content.split(None,1)[0][8:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][8:])
				if ndays<1 or ndays>200:
					ndays=1
					await message.channel.send('Number of days must be between 1 and 200. Outputting 1 day delta.')
		else:
			ndays=1
		recent=cursor.execute("SELECT Day from elos ORDER BY Day Desc LIMIT 1").fetchone()[0]
		old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
		gains=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo desc limit 3",{"day":recent,"oldday":old}).fetchall()
		losses=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo limit 3",{"day":recent,"oldday":old}).fetchall()
		await message.channel.send("Top 3 Elo Gains:")
		for user in gains:
			await message.channel.send('%s %+d, rank: %+d (%+.1f%%)' %(user[0],user[1],user[2],user[3]*100))
		await message.channel.send("Top 3 Elo Losses:")
		for user in losses:
			await message.channel.send('%s %+d, rank: %+d (%+.1f%%)' %(user[0],user[1],user[2],user[3]*100))

	#set a player's alias
	if message.content.startswith('$alias'):
		if len(message.content.split(None,1))>1:
			alias=message.content.split(None,1)[1]
		else:
			alias=message.author.display_name
		cursor.execute("INSERT INTO alias(Discord, Name, Time) VALUES(:discord,:name,CURRENT_TIMESTAMP) ON CONFLICT(Discord) DO UPDATE SET Name=excluded.Name,Time=CURRENT_TIMESTAMP",{"discord":message.author.id,"name":alias})
		db.commit()
		await message.channel.send('Alias for %s set to %s' %(message.author.display_name,alias))

	#plot player(s) Elo
	if message.content.startswith('$plot'):
		players=[]
		nplayers=1
		if len(message.content.split(None,1))>1:
			players=message.content.split(None,1)[1]
			players=[i.strip() for i in players.split(',')]
			nplayers=len(players)
		else:
			players.append(message.author.display_name)
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				players[0]=alias[0]

		if len(message.content.split(None,1)[0])>5:
			ndays=message.content.split(None,1)[0][5:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][5:])
				if ndays<1 or ndays>200:
					ndays=31
					await message.channel.send('Number of days must be between 1 and 200. Outputting 31 day plot.')
		else:
			ndays=31

		recent=cursor.execute("SELECT Day from elos ORDER BY Day Desc LIMIT 1").fetchone()[0]
		old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
		dates=[]
		values=[]
		for i in range(nplayers):
			dates.append([])
			values.append([])
			if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase AND Day BETWEEN :low and :high)",{"name":players[i],"low":old,"high":recent}).fetchone()[0]:
				data=cursor.execute("SELECT Day, Elo from elos where Name=:name COLLATE nocase AND Day BETWEEN :low and :high ORDER BY Day",{"name":players[i],"low":old,"high":recent}).fetchall()
				for row in data:
					dates[i].append(datetime.datetime.strptime(row[0],"%Y-%m-%d").strftime("%#m-%d"))
					values[i].append(row[1])
			else:
				await message.channel.send('Player '+players[i]+' not found or wrong format: $Elo player')
		fig, ax =plt.subplots(figsize=(14,5.6))
		for i in range(nplayers):
			ax.plot(dates[i],values[i],'-o',label=players[i])
		plt.xlabel('Date')
		plt.ylabel('Elo')
		plt.title('TPM.GG Elos '+r'$\mathrm{-\ %s\ to\ %s}$' %(datetime.datetime.strptime(old,"%Y-%m-%d").strftime("%#m/%d"),datetime.datetime.strptime(recent,"%Y-%m-%d").strftime("%#m/%d")))
		ax.grid(True, linestyle='dotted', color='black')
		if ndays>14:
			tickspacing=7
		else:
			tickspacing=1
		ax.xaxis.set_major_locator(ticker.MultipleLocator(tickspacing))
		plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
		plt.tight_layout()
		plt.savefig('tpm_elo_plot.png', bbox_inches='tight')
		plt.close()
		await message.channel.send(file=discord.File('tpm_elo_plot.png'))
		
	#plot player(s) Elo rank
	if message.content.startswith('$rank'):
		players=[]
		nplayers=1
		if len(message.content.split(None,1))>1:
			players=message.content.split(None,1)[1]
			players=[i.strip() for i in players.split(',')]
			nplayers=len(players)
		else:
			players.append(message.author.display_name)
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				players[0]=alias[0]
		if len(message.content.split(None,1)[0])>5:
			ndays=message.content.split(None,1)[0][5:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][5:])
				if ndays<1 or ndays>200:
					ndays=31
					await message.channel.send('Number of days must be between 1 and 200. Outputting 31 day plot.')
		else:
			ndays=31
		recent=cursor.execute("SELECT Day from elos ORDER BY Day Desc LIMIT 1").fetchone()[0]
		dates=[]
		values=[]
		for i in range(nplayers):
			dates.append([])
			values.append([])
			for j in range(ndays,-1,-1):
				old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=j)).strftime("%Y-%m-%d")
				if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase AND Day=:day)",{"name":players[i],"day":old}).fetchone()[0]:
					data=cursor.execute("SELECT * from (select Day, rank () over (order by Elo Desc), Elo, Name from elos where Day=:day) where Name=:name COLLATE nocase",{"day":old,"name":players[i]}).fetchone()
					dates[i].append(datetime.datetime.strptime(data[0],"%Y-%m-%d").strftime("%#m-%d"))
					values[i].append(data[1])
		fig, ax =plt.subplots(figsize=(14,5.6))
		for i in range(nplayers):
			ax.plot(dates[i],values[i],'-o',label=players[i])
		plt.xlabel('Date')
		plt.ylabel('Rank')
		old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%#m/%d")
		plt.title('TPM.GG Ranks '+r'$\mathrm{-\ %s\ to\ %s}$' %(old,datetime.datetime.strptime(recent,"%Y-%m-%d").strftime("%#m/%d")))
		ax.grid(True, linestyle='dotted', color='black')
		if ndays>14:
			tickspacing=7
		else:
			tickspacing=1
		ax.xaxis.set_major_locator(ticker.MultipleLocator(tickspacing))
		plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
		plt.tight_layout()
		plt.gca().invert_yaxis()
		plt.savefig('tpm_elo_plot.png', bbox_inches='tight')
		plt.close()
		await message.channel.send(file=discord.File('tpm_elo_plot.png'))

	#check top 3 on the games played leaderboard
	if message.content.startswith('$games'):
		if len(message.content.split(None,1)[0])>6:
			nplayers=int(message.content.split(None,1)[0][6:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, GamesPlayed from profiles where Time>DATETIME(:s1, 'unixepoch') order by GamesPlayed desc LIMIT :limit", {"s1":s1scrape,"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Players in Games Played:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(user[1]))

	#check top 3 on the leaderboard
	if message.content.startswith('$top'):
		if len(message.content.split(None,1)[0])>4:
			nplayers=int(message.content.split(None,1)[0][4:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, Elo, Day from elos order by Day Desc, Elo Desc LIMIT :limit", {"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Players:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(user[1]))

	#check bottom 3 on the leaderboard
	if message.content.startswith('$bottom'):
		leaders=cursor.execute("SELECT name, Elo, Day from elos order by Day Desc, Elo Asc LIMIT 3").fetchall()
		await message.channel.send('Bottom 3 Players:')
		for user in leaders:
			await message.channel.send(user[0]+' '+str(user[1]))

	if message.content.startswith('$range'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		if len(message.content.split(None,1)[0])>6:
			ndays=message.content.split(None,1)[0][6:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][6:])
				if ndays<1 or ndays>200:
					ndays=31
					await message.channel.send('Number of days must be between 1 and 200. Outputting 31 day range.')
		else:
			ndays=31
		recent=cursor.execute("SELECT Day from elos ORDER BY Day Desc LIMIT 1").fetchone()[0]
		old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
		if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",{"name":player}).fetchone()[0]:
			data=cursor.execute("SELECT min(elo),max(Elo) FROM elos WHERE Name=:name COLLATE nocase AND Day BETWEEN :low and :high",{"name":player,"low":old,"high":recent}).fetchone()
			await message.channel.send('Range since '+old+': ['+str(data[0])+', '+str(data[1])+']');
		else:
			await message.channel.send('Player not found or wrong format: $range player')

	if message.content.startswith('$average'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		if len(message.content.split(None,1)[0])>8:
			ndays=message.content.split(None,1)[0][8:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][8:])
				if ndays<1 or ndays>200:
					ndays=31
					await message.channel.send('Number of days must be between 1 and 200. Outputting 31 day average.')
		else:
			ndays=31
		recent=cursor.execute("SELECT Day from elos ORDER BY Day Desc LIMIT 1").fetchone()[0]
		old=datetime.datetime.strptime(recent, "%Y-%m-%d") - datetime.timedelta(days=ndays)
		if old<=s1date:
			old=(s1date+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
		else:
			old = old.strftime("%Y-%m-%d")
		if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",{"name":player}).fetchone()[0]:
			data=cursor.execute("SELECT avg(elo) FROM elos WHERE Name=:name COLLATE nocase AND Day BETWEEN :low and :high",{"name":player,"low":old,"high":recent}).fetchone()
			await message.channel.send('Average Elo since '+old+': '+str(int(data[0])))
		else:
			await message.channel.send('Player not found or wrong format: $average player')

	if message.content.startswith('$winstreaks'):
		if len(message.content.split(None,1)[0])>11:
			nplayers=int(message.content.split(None,1)[0][11:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, Streak from Streaks where Total>0 order by Streak desc, Total desc LIMIT :limit", {"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Win Streaks:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(user[1]))

	if message.content.startswith('$lossstreaks'):
		if len(message.content.split(None,1)[0])>12:
			nplayers=int(message.content.split(None,1)[0][12:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, Streak from Streaks where Total>0 order by Streak, Total desc LIMIT :limit", {"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Loss Streaks:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(-user[1]))

	if message.content.startswith('$activewinstreaks'):
		if len(message.content.split(None,1)[0])>17:
			nplayers=int(message.content.split(None,1)[0][17:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, Total from Streaks where Streak=0 order by Total desc LIMIT :limit", {"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Active Win Streaks:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(user[1]))

	if message.content.startswith('$activelossstreaks'):
		if len(message.content.split(None,1)[0])>18:
			nplayers=int(message.content.split(None,1)[0][18:])
			if nplayers<1 or nplayers>10:
				nplayers=3
				await message.channel.send('Number of players must be between 1 and 10.')
		else:
			nplayers=3
		leaders=cursor.execute("SELECT name, Total from Streaks where Streak=0 order by Total LIMIT :limit", {"limit":nplayers}).fetchall()
		await message.channel.send('Top %d Active Loss Streaks:' %nplayers)
		for user in leaders:
			await message.channel.send(user[0]+' '+str(-user[1]))

	if message.content.startswith('$streaks'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		if cursor.execute("SELECT EXISTS(SELECT 1 FROM streaks WHERE Name=:name COLLATE nocase)",{"name":player}).fetchone()[0]:
			if len(message.content.split(None,1)[0])>8:
				streaklength=int(message.content.split(None,1)[0][8:])
				if streaklength<1 or streaklength>50:
					streaklength=5
					await message.channel.send('Length of streak must be between 1 and 50.')
				await message.channel.send(str(cursor.execute("SELECT sum(Total) from streaks where Name=:name COLLATE nocase and Streak>=:length",{"name":player,"length":streaklength}).fetchone()[0]) +' streaks with '+str(streaklength)+' or more wins. '+str(cursor.execute("SELECT sum(Total) from streaks where Name=:name COLLATE nocase and Streak<=:length",{"name":player,"length":-streaklength}).fetchone()[0])+' streaks with '+str(streaklength)+' or more losses.')
			else:
				currentstreak=cursor.execute("SELECT Total from streaks where Name=:name COLLATE nocase and Streak=0",{"name":player}).fetchone()[0] 
				if currentstreak>0:
					await message.channel.send('Best win streak: '+ str(cursor.execute("SELECT max(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Worst loss streak: '+ str(cursor.execute("SELECT min(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Currently on a '+str(currentstreak)+' win streak.')
				elif currentstreak<0:
					await message.channel.send('Best win streak: '+ str(cursor.execute("SELECT max(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Worst loss streak: '+ str(cursor.execute("SELECT min(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Currently on a '+ str(-currentstreak)+' loss streak.')
				else:
					await message.channel.send('Best win streak: '+ str(cursor.execute("SELECT max(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Worst loss streak: '+ str(cursor.execute("SELECT min(Streak) from streaks where Name=:name COLLATE nocase",{"name":player}).fetchone()[0])+'. Voided their most recent game. Shame.')
		else:
			await message.channel.send('Player not found or wrong format: $streaks player')

	#check player's current Elo
	if message.content.startswith('$Elo') or  message.content.startswith('$elo'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",{"name":player}).fetchone()[0]:
			recent=cursor.execute("SELECT Day from elos where Name=:name COLLATE nocase ORDER BY Day Desc LIMIT 1",{"name":player}).fetchone()[0]
			user=cursor.execute("SELECT * from (select Name, Elo, rank () over (order by Elo Desc), percent_rank() over (order by Elo) from elos where Day=:day) where Name=:name COLLATE nocase",{"day":recent,"name":player}).fetchone()
			games=cursor.execute("SELECT * from (SELECT GamesPlayed, rank () over (order by GamesPlayed desc), percent_rank() over (order by GamesPlayed), Name from profiles order by GamesPlayed desc) where Name=:name COLLATE nocase",{"name":player}).fetchone()
			await message.channel.send(user[0]+' '+str(user[1]) + ', rank: '+str(user[2])+' ('+str(round(user[3]*100,1))+'%), games: '+str(games[0])+ ', rank: '+str(games[1])+' ('+str(round(games[2]*100,1))+'%)')
		else:
			await message.channel.send('Player not found or wrong format: $Elo player')

	#check map data
	if message.content.startswith('$maps'):
		if len(message.content.split(None,1))>1:
			mapname=message.content.split(None,1)[1]
			if cursor.execute("SELECT EXISTS(SELECT 1 FROM matches WHERE Map=:name COLLATE nocase)",{"name":mapname}).fetchone()[0]:
				data=cursor.execute("SELECT map, count(*), sum(redwin), sum(bluewin), sum(void) from matches where Map=:name COLLATE nocase",{"name":mapname}).fetchone()
				await message.channel.send('%s: %d games, Red: %d wins (%.1f%%), Blue: %d wins (%.1f%%), %d Voids' %(data[0],data[1],data[2],data[2]/(data[1]-data[4])*100,data[3],data[3]/(data[1]-data[4])*100,data[4]))
			else:
				await message.channel.send('Map not found or wrong format: $maps map')
		else:
			maps=cursor.execute("SELECT map, count(*), sum(redwin), sum(bluewin), sum(void) from matches group by map order by count(*) desc").fetchall()
			await message.channel.send('Top 5 Maps:')
			for data in maps[:5]:
				await message.channel.send('%s: %d games, Red: %d wins (%.1f%%), Blue: %d wins (%.1f%%), %d Voids' %(data[0],data[1],data[2],data[2]/(data[1]-data[4])*100,data[3],data[3]/(data[1]-data[4])*100,data[4]))

	#check player's current delta
	if message.content.startswith('$delta'):
		if len(message.content.split(None,1))>1:
			player=message.content.split(None,1)[1]
		else:
			player=message.author.display_name
			alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
			if alias is not None:
				player=alias[0]
		if len(message.content.split(None,1)[0])>6:
			ndays=message.content.split(None,1)[0][6:]
			if ndays=="all":
				ndays=cursor.execute("SELECT count(distinct day) from elos").fetchone()[0]-1
			else:
				ndays=int(message.content.split(None,1)[0][6:])
				if ndays<1 or ndays>200:
					ndays=1
					await message.channel.send('Number of days must be between 1 and 200. Outputting 1 day delta.')
		else:
			ndays=1
		if cursor.execute("SELECT EXISTS(SELECT 1 FROM elos WHERE Name=:name COLLATE nocase)",{"name":player}).fetchone()[0]:
			recent=cursor.execute("SELECT Day from elos where Name=:name COLLATE NOCASE ORDER BY Day Desc LIMIT 1",{"name":player}).fetchone()[0]
			old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
			delta=cursor.execute("SELECT * from (SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday) where Name=:name COLLATE NOCASE",{"day":recent,"oldday":old,"name":player}).fetchone()
			await message.channel.send('%s %+d, rank: %+d (%+.1f%%)' %(delta[0],delta[1],delta[2],delta[3]*100))
		else:
			await message.channel.send('Player not found or wrong format: $delta player')

	#check last Elo update time
	if message.content.startswith('$time'):
		updated=parser.parse(cursor.execute("SELECT Time from Elos ORDER BY Time Desc LIMIT 1").fetchone()[0]).replace(tzinfo=datetime.timezone.utc).astimezone(tz.tzlocal())
		await message.channel.send("Elos last updated at: %s" % updated)

	if message.author.id==REDACTED:
		if message.content.startswith('$updatestatsgcp'):
			updateStats()
			await message.channel.send('Stats updated')
		if message.content.startswith('$scrapegcp'):
			updateloop.cancel()
			endofday()
			await message.channel.send('Scraping finished')
			updateloop.start()
		if message.content.startswith('$h2h'):
			if len(message.content.split(None,1))>1:
				players=message.content.split(None,1)[1]
				players=[i.strip() for i in players.split(',')]
				nplayers=len(players)
				if nplayers>2:
					await message.channel.send('Maximum of 2 players. %d players specified.' %(nplayers))
				else:
					if nplayers==1:
						alias=cursor.execute("SELECT Name from alias where Discord=:user",{"user":message.author.id}).fetchone()
						if alias is not None:
							players.append(alias[0])
						else:
							players.append(message.author.display_name)
					if players[0].lower()==players[1].lower():
						await message.channel.send('Please specify two distinct players.')
					else:
						players.sort()
						matches=cursor.execute("SELECT tpmid, Name, EloDelta from players where tpmid in (select tpmid from players where tpmid in (select players.tpmid from players inner join matches on matches.tpmid=players.tpmid where void=0 and players.name=:p1 collate nocase) and name=:p2 collate nocase) and  (name=:p1 collate nocase or name=:p2 collate nocase) order by tpmid, Name",{"p1":players[0],"p2":players[1]}).fetchall()
						games=len(matches)/2
						p1wins=0
						p1losses=0
						p1sharedwins=0
						p1sharedlosses=0
						player1=""
						player2=""
						for index, match in enumerate(matches):
							if index%2==0:
								player1=match[1]
								player2=matches[index+1][1]
								if match[2]==matches[index+1][2]:
									if match[2]>0:
										p1sharedwins+=1
									else:
										p1sharedlosses+=1
								elif match[2]<0 and matches[index+1][2]>0:
									p1losses+=1
								elif match[2]>0 and matches[index+1][2]<0:
									p1wins+=1
								else:
									print("Unknown result!"+str(match))
						if player1=="":
							await message.channel.send('No games found between %s and %s'%(players[0],players[1]))
						else:
							await message.channel.send("%s has won %d and lost %d games against %s. Together, they have won %d and lost %d games." %(player1,p1wins,p1losses,player2,p1sharedwins,p1sharedlosses))
			else:
				await message.channel.send('Specify players to compare: $h2h player1, player2 (player2 optional, will reference discord name or alias if set)')

		if message.content=='$commands':
			await message.channel.send('Getting channel history...')
			messages=await message.channel.history(limit=None).flatten()
			await message.channel.send('Acquired channel history. Computing usage...')
			commands=[x for x in messages if x.content.startswith('$')]
			await message.channel.send('There have been %d commands sent to this channel.' % len([x for x in messages if x.content.startswith('$')]))
			authors=dict()
			for message in commands:
				if message.author.id in authors:
					authors[message.author.id]+=1
				else:
					authors[message.author.id]=1
			await message.channel.send("Top 10 Users of cat-stats:")
			for user in sorted(authors, key=authors.__getitem__,reverse=True)[:10]:
				await message.channel.send(client.get_user(user).display_name+" "+str(authors[user]))

		if message.content=='$goodbyegcp':
			await message.channel.send('Going offline. Goodbye.')
			await client.close()

		if message.content=='$pause':
			await message.channel.send('Paused elo updates.')
			updateloop.cancel()

		if message.content=='$resume':
			await message.channel.send('Resumed elo updates.')
			updateloop.start()

		if message.content=="$close":
			cursor.close()
			db.close()
			updateloop.cancel()
			await message.channel.send('Closed database.')
		if message.content=="$open":
			opendb()
			updateloop.start()
			await message.channel.send('Opened database.')


		if message.content.startswith('$user'):
			#doesn't work currently. Need Intent.Members
			if len(message.content.split(None,1))>1:
				player=message.content.split(None,1)[1]
			else:
				player=message.author.display_name
			await message.channel.send((discord.utils.get(client.get_guild(597561804662767627).members, name=player)).id)

		if message.content.startswith('$updategcp'):
			updateLeaderboard()
			await discord.utils.get(client.get_guild(597561804662767627).text_channels, name='bot-spam').send('Elo update complete at '+str(datetime.datetime.now()))

		if message.content.startswith('$catstats'):
			cschannel=discord.utils.get(message.guild.text_channels, name='cat-stats')
			# endofday()
			ndays=1
			today=date.today().strftime("%b-%d-%Y")
			recent=date.today().strftime("%Y-%m-%d")
			old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
			gains=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo desc limit 3",{"day":recent,"oldday":old}).fetchall()
			losses=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo limit 3",{"day":recent,"oldday":old}).fetchall()
			epoch1=datetime.datetime.strptime(recent, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp()
			epoch2=(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()
			games=cursor.execute("SELECT name,count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 group by players.name order by c desc limit 3",{"epoch1":epoch1,"epoch2":epoch2}).fetchall()
			await cschannel.send(today)
			await cschannel.send("Top 3 Daily Gains:")
			for user in gains:
				game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":epoch1,"epoch2":epoch2}).fetchone()[0]
				await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
			await cschannel.send("Top 3 Daily Losses:")
			for user in losses:
				game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":epoch1,"epoch2":epoch2}).fetchone()[0]
				await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
			await cschannel.send("Top 3 Daily Games Played:")
			for user in games:
				delta=cursor.execute("SELECT * from (SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:recent AND B.Day=:old) WHERE Name=:name COLLATE NOCASE",{"recent":recent,"old":old,"name":user[0]}).fetchone()
				if delta is not None:
					await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],delta[1],delta[2],delta[3]*100,user[1]))
				else:
					await cschannel.send('%s [unknown delta], %d games' %(user[0],user[1]))

			# calculate 7 day deltas
			if date.today().weekday()==6:
				ndays=7
				recent=date.today().strftime("%Y-%m-%d")
				old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
				gains=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo desc limit 3",{"day":recent,"oldday":old}).fetchall()
				losses=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo limit 3",{"day":recent,"oldday":old}).fetchall()
				games=cursor.execute("SELECT name,count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 group by players.name order by c desc limit 3",{"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchall()
				await cschannel.send("Top 3 Weekly Gains:")
				for user in gains:
					game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
					await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
				await cschannel.send("Top 3 Weekly Losses:")
				for user in losses:
					game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
					await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
				await cschannel.send("Top 3 Weekly Games Played:")
				for user in games:
					delta=cursor.execute("SELECT * from (SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:recent AND B.Day=:old) WHERE Name=:name COLLATE NOCASE",{"recent":recent,"old":old,"name":user[0]}).fetchone()
					if delta is not None:
						await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' % (
							user[0], delta[1], delta[2], delta[3] * 100, user[1]))
					else:
						await cschannel.send('%s [unknown delta], %d games' % (user[0], user[1]))

			data=[]
			elo=cursor.execute("SELECT Elo from elos where Day=:day",{"day":date.today().strftime("%Y-%m-%d")}).fetchall()
			for row in elo:
				data.append(row[0])

			elo=np.array(data)
			datos = elo.astype(int)

			# best fit of data
			(mu, sigma) = norm.fit(datos)

			# the histogram of the data
			fig, ax =plt.subplots()	
			if datos.min()<0:
				minbin=int(math.floor(datos.min()/100)*100)
			else:
				minbin=0
			maxbin=datos.max()-datos.max()%-100+100
			if maxbin<3600:
				maxbin=3600
			n, bins, patches = ax.hist(datos, bins=range(minbin,maxbin,100), facecolor='green', linewidth=1.2, edgecolor='black', alpha=0.75)

			# add a 'best fit' line
			# y = norm.pdf( bins, mu, sigma)
			# plt.plot(bins, y, 'r--', linewidth=2)

			#plot
			plt.xlabel('Elo')
			plt.ylabel('Count')
			plt.title('TPM.GG Leaderboard Elos '+r'$\mathrm{-\ %s}\ \mu=%i,\ \sigma=%i,\ n=%i$' %(date.today().strftime("%#m/%d"),mu, sigma, len(data)))
			ax.grid(True, linestyle='dotted', color='black')
			ax.margins(0)
			if minbin<0:
				plt.xlim(math.floor((minbin-100)/500.0)*500.0,math.ceil(maxbin/500.0)*500.0)
			else:
				plt.xlim(0,math.ceil(maxbin/500.0)*500.0)
			ax.set_axisbelow(True)
			plt.savefig(today+'tpm.png', bbox_inches='tight')
			await cschannel.send(file=discord.File(today+'tpm.png'))
			await message.channel.send(cschannel.mention+' posted')

		if message.content.startswith('$retrocatstats'):
			if len(message.content.split(None,1))>1:
				retrodays=int(message.content.split(None,1)[1])
			else:
				retrodays=1
			# endofday()
			# retrodays=5
			for i in tqdm(range(retrodays,0,-1)):
				cschannel=discord.utils.get(message.guild.text_channels, name='cat-stats')
				ndays=1
				# today=date.today().strftime("%b-%d-%Y")
				# recent=date.today().strftime("%Y-%m-%d")
				# old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
				today=(date.today()-datetime.timedelta(days=i)).strftime("%b-%d-%Y")
				recent=(date.today()-datetime.timedelta(days=i)).strftime("%Y-%m-%d")
				old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
				gains=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo desc limit 3",{"day":recent,"oldday":old}).fetchall()
				losses=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo limit 3",{"day":recent,"oldday":old}).fetchall()
				games=cursor.execute("SELECT name,count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 group by players.name order by c desc limit 3",{"epoch1":datetime.datetime.strptime(recent,"%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchall()
				await cschannel.send(today)
				await cschannel.send("Top 3 Daily Gains:")
				for user in gains:
					game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":datetime.datetime.strptime(recent,"%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
					await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
				await cschannel.send("Top 3 Daily Losses:")
				for user in losses:
					game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":datetime.datetime.strptime(recent,"%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
					await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
				await cschannel.send("Top 3 Daily Games Played:")
				for user in games:
					delta=cursor.execute("SELECT * from (SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:recent AND B.Day=:old) WHERE Name=:name COLLATE NOCASE",{"recent":recent,"old":old,"name":user[0]}).fetchone()
					if delta is not None:
						await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],delta[1],delta[2],delta[3]*100,user[1]))
					else:
						await cschannel.send('%s [unknown delta], %d games' %(user[0],user[1]))

				# calculate 7 day deltas
				if (date.today()-datetime.timedelta(days=i)).weekday()==6:
					ndays=7
					recent=(date.today()-datetime.timedelta(days=i)).strftime("%Y-%m-%d")
					old=(datetime.datetime.strptime(recent,"%Y-%m-%d")-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
					gains=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo desc limit 3",{"day":recent,"oldday":old}).fetchall()
					losses=cursor.execute("SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:day AND B.Day=:oldday order by A.elo-B.elo limit 3",{"day":recent,"oldday":old}).fetchall()
					games=cursor.execute("SELECT name,count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 group by players.name order by c desc limit 3",{"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchall()
					await cschannel.send("Top 3 Weekly Gains:")
					for user in gains:
						game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
						await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
					await cschannel.send("Top 3 Weekly Losses:")
					for user in losses:
						game=cursor.execute("SELECT count(matches.tpmid) c from matches inner join players on players.tpmid=matches.tpmid where MatchDate>=:epoch1 and MatchDate<:epoch2 and Name=:name",{"name":user[0],"epoch1":(datetime.datetime.strptime(old,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp(),"epoch2":(datetime.datetime.strptime(recent,"%Y-%m-%d")+datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc).timestamp()}).fetchone()[0]
						await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' %(user[0],user[1],user[2],user[3]*100,game))
					await cschannel.send("Top 3 Weekly Games Played:")
					for user in games:
						delta=cursor.execute("SELECT * from (SELECT A.name, A.elo-B.elo, rank() over(Order by A.Elo)-rank() over(Order by B.Elo), percent_rank() over (order by A.Elo)-percent_rank() over (order by B.Elo) from elos A INNER JOIN elos B on B.name=A.name where A.Day=:recent AND B.Day=:old) WHERE Name=:name COLLATE NOCASE",{"recent":recent,"old":old,"name":user[0]}).fetchone()
						if delta is not None:
							await cschannel.send('%s %+d, rank: %+d (%+.1f%%), %d games' % (
								user[0], delta[1], delta[2], delta[3] * 100, user[1]))
						else:
							await cschannel.send('%s [unknown delta], %d games' % (user[0], user[1]))

				data=[]
				elo=cursor.execute("SELECT Elo from elos where Day=:day",{"day":(date.today()-datetime.timedelta(days=i)).strftime("%Y-%m-%d")}).fetchall()
				for row in elo:
					data.append(row[0])

				elo=np.array(data)
				datos = elo.astype(int)

				# best fit of data
				(mu, sigma) = norm.fit(datos)

				# the histogram of the data
				fig, ax =plt.subplots()
				if datos.min()<0:
					minbin=int(math.floor(datos.min()/100)*100)
				else:
					minbin=0
				maxbin=datos.max()-datos.max()%-100+100
				if maxbin<3600:
					maxbin=3600
				n, bins, patches = ax.hist(datos, bins=range(minbin,maxbin,100), facecolor='green', linewidth=1.2, edgecolor='black', alpha=0.75)

				# add a 'best fit' line
				# y = norm.pdf( bins, mu, sigma)
				# plt.plot(bins, y, 'r--', linewidth=2)

				#plot
				plt.xlabel('Elo')
				plt.ylabel('Count')
				plt.title('TPM.GG Leaderboard Elos '+r'$\mathrm{-\ %s}\ \mu=%i,\ \sigma=%i,\ n=%i$' %((date.today()-datetime.timedelta(days=i)).strftime("%#m/%d"),mu, sigma, len(data)))
				ax.grid(True, linestyle='dotted', color='black')
				ax.margins(0)
				if minbin<0:
					plt.xlim(math.floor((minbin-100)/500.0)*500.0,math.ceil(maxbin/500.0)*500.0)
				else:
					plt.xlim(0,math.ceil(maxbin/500.0)*500.0)
				ax.set_axisbelow(True)
				plt.savefig(today+'tpm.png', bbox_inches='tight')
				await cschannel.send(file=discord.File(today+'tpm.png'))
				await message.channel.send(cschannel.mention+' posted')



client.run('REDACTED')
cursor.close()
db.close()
