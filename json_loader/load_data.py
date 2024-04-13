import os
import psycopg2
import json

def connectDatabase():
    try:
        connect = psycopg2.connect(
            dbname = "postgres",
            user="postgres",
            password= "1234",
            host="localhost",
            port="5432"
        )
        print("Connection successfull")
        return connect
    except:
        print("Connection failed")

os.chdir(os.path.join(os.getcwd(), "open-data", "data", "matches"))

def populateCompetitions():
    path = os.getcwd()[:-7]
    f = open (os.path.join(path, "competitions.json"), "r")
    data = json.load(f)

    res = {}

    for i in range(len(data)):
        if (data[i]["competition_name"] == "La Liga" or data[i]["competition_name"] == "Premier League"):
            try:
                if (data[i]["competition_name"] not in res[data[i]["competition_id"]]):
                    res[data[i]["competition_id"]].append(data[i]["competition_name"])
                    res[data[i]["competition_id"]].append(data[i]["country_name"])
                    res[data[i]["competition_id"]].append(data[i]["competition_gender"])
                    res[data[i]["competition_id"]].append(data[i]["competition_youth"])
                    res[data[i]["competition_id"]].append(data[i]["competition_international"])

            except:
                res[data[i]["competition_id"]] = []
                res[data[i]["competition_id"]].append(data[i]["competition_name"])
                res[data[i]["competition_id"]].append(data[i]["country_name"])
                res[data[i]["competition_id"]].append(data[i]["competition_gender"])
                res[data[i]["competition_id"]].append(data[i]["competition_youth"])
                res[data[i]["competition_id"]].append(data[i]["competition_international"])

    
    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Competitions (competition_id, competition_name, country_name, gender, youth, international) VALUES (%s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4]))
            connect.commit()
        except Exception as e:
            print(e)
    cur.close()
    connect.close()

def populateSeasons():
    connect = connectDatabase()
    cur = connect.cursor()
    try:
        cur.execute("INSERT INTO Seasons (season_id, season_name, competition_id) VALUES (%s, %s, %s)", (90, "2020/2021", 11))
        cur.execute("INSERT INTO Seasons (season_id, season_name, competition_id) VALUES (%s, %s, %s)", (42, "2019/2020", 11))
        cur.execute("INSERT INTO Seasons (season_id, season_name, competition_id) VALUES (%s, %s, %s)", (4, "2018/2019", 11))
        cur.execute("INSERT INTO Seasons (season_id, season_name, competition_id) VALUES (%s, %s, %s)", (44, "2003/2004", 2))
        connect.commit()
    except Exception as e:
        connect.rollback()
        
    cur.close()
    connect.close()


def populateManagers():
    currDir = os.getcwd()
    dirs = os.listdir()
    res = {}
    for d in dirs:
        if(d != ".DS_Store"):
            files = os.listdir(os.path.join(currDir, d))
            for f in files:
                fi = open(os.path.join(currDir, d, f), "r")
                data = json.load(fi);
                for i in range(len(data)):
                    try:
                        for j in (data[i]["home_team"]["managers"]):
                            res[j["id"]].append(j["name"])
                            res[j["id"]].append(j["nickname"])
                            res[j["id"]].append(j["dob"])
                            res[j["id"]].append(j["country"]["name"])
                        res[j["id"]].append(data[i]["home_team"]["home_team_id"])
                        
                    except:
                        res[j["id"]] = []
                        res[j["id"]].append(j["name"])
                        res[j["id"]].append(j["nickname"])
                        res[j["id"]].append(j["dob"])
                        res[j["id"]].append(j["country"]["name"])
                        res[j["id"]].append(data[i]["home_team"]["home_team_id"])

                    try:
                        for j in (data[i]["away_team"]["managers"]):
                            res[j["id"]].append(j["name"])
                            res[j["id"]].append(j["nickname"])
                            res[j["id"]].append(j["dob"])
                            res[j["id"]].append(j["country"]["name"])
                        res[j["id"]].append(data[i]["away_team"]["away_team_id"])
                    except:
                        res[j["id"]] = []
                        res[j["id"]].append(j["name"])
                        res[j["id"]].append(j["nickname"])
                        res[j["id"]].append(j["dob"])
                        res[j["id"]].append(j["country"]["name"])
                        res[j["id"]].append(data[i]["away_team"]["away_team_id"])
    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Managers (manager_id, manager_name, manager_nickname, date_of_birth, country, team_id) VALUES (%s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4]))
            connect.commit()
        except Exception as e:
            connect.rollback()
    cur.close()
    connect.close()



def populateStadiums():
    currDir = os.getcwd()
    dirs = os.listdir()
    res = {}
    for d in dirs:
        if(d != ".DS_Store"):
            files = os.listdir(os.path.join(currDir, d))
            for f in files:
                fi = open(os.path.join(currDir, d, f), "r")
                data = json.load(fi);
                for i in range(len(data)):
                    if("stadium" in data[i].keys()):
                        try:
                            res[data[i]["stadium"]["id"]].append(data[i]["stadium"]["name"])
                            res[data[i]["stadium"]["id"]].append(data[i]["stadium"]["country"]["name"])
                        except:
                            res[data[i]["stadium"]["id"]] = []
                            res[data[i]["stadium"]["id"]].append(data[i]["stadium"]["name"])
                            res[data[i]["stadium"]["id"]].append(data[i]["stadium"]["country"]["name"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Stadiums (stadium_id, stadium_name, country) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except Exception as e:
            connect.rollback()
    cur.close()
    connect.close()

def populateReferees():
    currDir = os.getcwd()
    dirs = os.listdir()
    res = {}
    for d in dirs:
        if(d != ".DS_Store"):
            files = os.listdir(os.path.join(currDir, d))
            for f in files:
                fi = open(os.path.join(currDir, d, f), "r")
                data = json.load(fi);
                for i in range(len(data)):
                    if("referee" in data[i].keys()):
                        try:
                            res[data[i]["referee"]["id"]].append(data[i]["referee"]["name"])
                            res[data[i]["referee"]["id"]].append(data[i]["referee"]["country"]["name"])
                        except:
                            res[data[i]["referee"]["id"]] = []
                            res[data[i]["referee"]["id"]].append(data[i]["referee"]["name"])
                            res[data[i]["referee"]["id"]].append(data[i]["referee"]["country"]["name"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Referees (referee_id, name, country) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except Exception as e:
            connect.rollback()
    cur.close()
    connect.close()


def populateMatches():
    currDir = os.getcwd()
    dirs = os.listdir()
    res = {}
    for d in dirs:
        if(d != ".DS_Store"):
            files = os.listdir(os.path.join(currDir, d))
            for f in files:
                fi = open(os.path.join(currDir, d, f), "r")
                data = json.load(fi);
                for i in range(len(data)):
                    try:
                        res[data[i]["match_id"]].append(data[i]["competition"]["competition_id"])
                        res[data[i]["match_id"]].append(data[i]["season"]["season_id"])
                        res[data[i]["match_id"]].append(data[i]["home_team"]["home_team_id"])
                        res[data[i]["match_id"]].append(data[i]["away_team"]["away_team_id"])
                        res[data[i]["match_id"]].append(data[i]["match_date"])
                        res[data[i]["match_id"]].append(data[i]["kick_off"])
                        res[data[i]["match_id"]].append(data[i]["home_score"])
                        res[data[i]["match_id"]].append(data[i]["away_score"])
                        if("stadium" in data[i].keys()):
                            res[data[i]["match_id"]].append(data[i]["stadium"]["id"])
                        res[data[i]["match_id"]].append(data[i]["match_week"])
                        res[data[i]["match_id"]].append(data[i]["competition_stage"]["name"])
                        if("referee" in data[i].keys()):
                            res[data[i]["match_id"]].append(data[i]["referee"]["id"])
                        else:
                            res[data[i]["match_id"]].append(None)

                    except:
                        res[data[i]["match_id"]] = []
                        res[data[i]["match_id"]].append(data[i]["competition"]["competition_id"])
                        res[data[i]["match_id"]].append(data[i]["season"]["season_id"])
                        res[data[i]["match_id"]].append(data[i]["home_team"]["home_team_id"])
                        res[data[i]["match_id"]].append(data[i]["away_team"]["away_team_id"])
                        res[data[i]["match_id"]].append(data[i]["match_date"])
                        res[data[i]["match_id"]].append(data[i]["kick_off"])
                        res[data[i]["match_id"]].append(data[i]["home_score"])
                        res[data[i]["match_id"]].append(data[i]["away_score"])
                        if("stadium" in data[i].keys()):
                            res[data[i]["match_id"]].append(data[i]["stadium"]["id"])
                        res[data[i]["match_id"]].append(data[i]["match_week"])
                        res[data[i]["match_id"]].append(data[i]["competition_stage"]["name"])
                        if("referee" in data[i].keys()):
                            res[data[i]["match_id"]].append(data[i]["referee"]["id"])
                        else:
                            res[data[i]["match_id"]].append(None)


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Matches (match_id, competition_id, season_id, home_team_id, away_team_id, match_date, kick_off, home_score, away_score, stadium_id, match_week, competition_stage_name, referee_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8], value[9], value[10], value[11]))
            connect.commit()
        except IndexError as er:
            cur.execute("INSERT INTO Matches (match_id, competition_id, season_id, home_team_id, away_team_id, match_date, kick_off, home_score, away_score, stadium_id, match_week, competition_stage_name, referee_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], None, value[8], value[9], value[10]))
        except Exception as e: 
            connect.rollback()
    cur.close()
    connect.close()






def populatePlayers():
    os.chdir(os.path.join(os.getcwd(), "..", "lineups"))
    res = {}
    matches = os.listdir()
    for m in matches:
        file = open(os.getcwd() + "/" + m, "r")
        data = json.load(file)
        teamid = ""
        for i in range(len(data)):
            teamid = data[i]["team_id"]
            for j in range(len(data[i]["lineup"])):
                try:
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["player_name"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["player_nickname"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["jersey_number"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["country"]["name"])
                    res[data[i]["lineup"][j]["player_id"]].append(teamid)
                except:
                    res[data[i]["lineup"][j]["player_id"]] = []
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["player_name"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["player_nickname"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["jersey_number"])
                    res[data[i]["lineup"][j]["player_id"]].append(data[i]["lineup"][j]["country"]["name"])
                    res[data[i]["lineup"][j]["player_id"]].append(teamid)

    
    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Players (player_id, player_name, player_nickname, jersey_number, country, team_id) VALUES (%s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4]))
            connect.commit()
        except psycopg2.Error as e:
            connect.rollback()

    cur.close()
    connect.close()



def populateShots():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Shot"):
                try:
                    res[data[i]["id"]].append(data[i]["shot"]['statsbomb_xg'])
                    if ("first_time" in list(data[i]["shot"].keys())):
                        res[data[i]["id"]].append(data[i]["shot"]["first_time"])
                    else:
                        res[data[i]["id"]].append("false")
                    res[data[i]["id"]].append(data[i]["shot"]["outcome"]["name"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["shot"]["end_location"])
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["shot"]["technique"]["name"])
                    res[data[i]["id"]].append(data[i]["shot"]["body_part"]["name"])
                    if ("key_pass_id" in list(data[i]["shot"].keys())):
                        res[data[i]["id"]].append(data[i]["shot"]["key_pass_id"])
                    else:
                        res[data[i]["id"]].append(None)
                except:
                    res[data[i]["id"]] = []
                    res[data[i]["id"]].append(data[i]["shot"]['statsbomb_xg'])
                    if ("first_time" in list(data[i]["shot"].keys())):
                        res[data[i]["id"]].append(data[i]["shot"]["first_time"])
                    else:
                        res[data[i]["id"]].append("false")
                    res[data[i]["id"]].append(data[i]["shot"]["outcome"]["name"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["shot"]["end_location"])
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["shot"]["technique"]["name"])
                    res[data[i]["id"]].append(data[i]["shot"]["body_part"]["name"])
                    if ("key_pass_id" in list(data[i]["shot"].keys())):
                        res[data[i]["id"]].append(data[i]["shot"]["key_pass_id"])
                    else:
                        res[data[i]["id"]].append(None)
        f.close()
    
    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Shots (event_id, xg_score, first_time, outcome, location, end_location, duration, technique, body_part, key_pass_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], (value[3],), value[4], value[5], value[6], value[7], value[8]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populatePositions():
    path = (os.path.join(os.getcwd(), "..", "lineups"))
    files = os.listdir(path)
    d = {}
    for i in files:
        f = open(path + "/" +i, "r")
        data = json.load(f)
        for i in range(len(data)):
            for j in data[i]["lineup"]:
                for k in j["positions"]:
                    try:
                        d[j["player_id"]].append(k["position_id"])
                        d[j["player_id"]].append(k["position"])
                        d[j["player_id"]].append(k["from"])
                        d[j["player_id"]].append(k["to"])
                        d[j["player_id"]].append(k["from_period"])
                        d[j["player_id"]].append(k["to_period"])
                        d[j["player_id"]].append(k["start_reason"])
                        d[j["player_id"]].append(k["end_reason"])
                    except:
                        d[j["player_id"]] = []
                        d[j["player_id"]].append(k["position_id"])
                        d[j["player_id"]].append(k["position"])
                        d[j["player_id"]].append(k["from"])
                        d[j["player_id"]].append(k["to"])
                        d[j["player_id"]].append(k["from_period"])
                        d[j["player_id"]].append(k["to_period"])
                        d[j["player_id"]].append(k["start_reason"])
                        d[j["player_id"]].append(k["end_reason"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in d.items():
        try:
            cur.execute("INSERT INTO Positions (player_id, position_id, position, from_time, to_time, from_period, to_period, start_reason, end_reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]))
            connect.commit()
        except psycopg2.Error as e:
            connect.rollback()
    cur.close()
    connect.close()


def populateCards():
    path = (os.path.join(os.getcwd(), "..", "lineups"))
    files = os.listdir(path)
    d = {}
    for i in files:
        f = open(path + "/" +i, "r")
        data = json.load(f)
        for i in range(len(data)):
            for j in data[i]["lineup"]:
                for k in j["cards"]:
                    try:
                        d[j["player_id"]].append(k["card_type"])
                        d[j["player_id"]].append(k["reason"])
                        d[j["player_id"]].append(k["time"])
                        d[j["player_id"]].append(k["period"])
                    except:
                        d[j["player_id"]] = []
                        d[j["player_id"]].append(k["card_type"])
                        d[j["player_id"]].append(k["reason"])
                        d[j["player_id"]].append(k["time"])
                        d[j["player_id"]].append(k["period"])


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in d.items():
        try:
            cur.execute("INSERT INTO Cards (player_id, card_type, reason, card_time, period) VALUES (%s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3]))
            connect.commit()
        except psycopg2.Error as e:
            connect.rollback()
    cur.close()
    connect.close()




def populateTeams():
    path = (os.path.join(os.getcwd(), "..", "lineups"))
    files = os.listdir(path)
    d = {}
    for i in files:
        f = open(path + "/" +i, "r")
        data = json.load(f)
        d[data[0]["team_id"]] = data[0]["team_name"]
        d[data[1]["team_id"]] = data[1]["team_name"]


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in d.items():
        try:
            cur.execute("INSERT INTO Teams (team_id, team_name) VALUES (%s, %s)", (key, value))
            connect.commit()
        except psycopg2.Error as e:
            connect.rollback()
    cur.close()
    connect.close()


def populatePasses():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Pass"):
                try:
                    if("through_ball" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["through_ball"])
                    else:
                        res[data[i]["id"]].append("false")

                    if ("recipient" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["recipient"]["id"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["pass"]["end_location"])
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["pass"]["length"])
                    res[data[i]["id"]].append(data[i]["pass"]["angle"])
                    res[data[i]["id"]].append(data[i]["pass"]["height"]["name"])
                    if ("type" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["type"]["name"])
                    else:
                        res[data[i]["id"]].append(None)

                    if ("outcome" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append("Complete")
                    if ("technique" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["technique"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["pass"]["shot_assist"])
                except:
                    res[data[i]["id"]] = []
                    if("through_ball" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["through_ball"])
                    else:
                        res[data[i]["id"]].append("false")

                    if ("recipient" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["recipient"]["id"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["pass"]["end_location"])
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["pass"]["length"])
                    res[data[i]["id"]].append(data[i]["pass"]["angle"])
                    res[data[i]["id"]].append(data[i]["pass"]["height"]["name"])
                    if ("type" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["type"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if ("outcome" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append("Complete")
                    if ("technique" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["technique"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if ("shot_assist" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["shot_assist"])
                    else:
                        res[data[i]["id"]].append(False)
                    if ("body_part" in list(data[i]["pass"].keys())):
                        res[data[i]["id"]].append(data[i]["pass"]["body_part"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
        f.close()

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Passes (event_id, through_ball, recipient_id, location, end_location, duration, length, angle, height, type, outcome, technique, shot_assist, body_part) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8], value[9], value[10], value[11], value[12]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateDribbles():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Dribble"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["dribble"]["outcome"]["name"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["dribble"]["outcome"]["name"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    
        f.close()

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Dribbles (event_id, outcome, under_pressure) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()
        

def populateDribbledPast():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Dribbled Past"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    res[data[i]["id"]].append(data[i]["location"])
                    
        f.close()

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Dribbled_Past (event_id, location) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateEvents():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
                try:
                    res[data[i]["id"]].append(files[j][:-5])
                    res[data[i]["id"]].append(data[i]["team"]["id"])
                    if("player" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["player"]["id"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["index"])
                    res[data[i]["id"]].append(data[i]["period"])
                    res[data[i]["id"]].append(data[i]["timestamp"])
                    res[data[i]["id"]].append(data[i]["possession"])
                    res[data[i]["id"]].append(data[i]["possession_team"]["name"])
                    res[data[i]["id"]].append(data[i]["play_pattern"]["name"])
                except:
                    res[data[i]["id"]] = []
                    res[data[i]["id"]].append(files[j][:-5])
                    res[data[i]["id"]].append(data[i]["team"]["id"])
                    if("player" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["player"]["id"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["index"])
                    res[data[i]["id"]].append(data[i]["period"])
                    res[data[i]["id"]].append(data[i]["timestamp"])
                    res[data[i]["id"]].append(data[i]["possession"])
                    res[data[i]["id"]].append(data[i]["possession_team"]["name"])
                    res[data[i]["id"]].append(data[i]["play_pattern"]["name"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Events (event_id, match_id, team_id, player_id, index, period, timestamp, possession_index, possession_team, play_pattern) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populatePlayerOn():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Player On"):
                try:
                    res[data[i]["id"]] = []
                except:
                    res[data[i]["id"]] = []
                    
                    

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO playerson (event_id) VALUES (%s)", (key,))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populatePlayerOff():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Player Off"):
                try:
                    res[data[i]["id"]] = []
                except:
                    res[data[i]["id"]] = []
                    
                    

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO playersoff (event_id) VALUES (%s)", (key,))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateShields():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Shield"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Shields (event_id, location) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateBallReceipt():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Ball Receipt*"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO ballreceipt (event_id, location) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateInjuryStoppage():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Injury Stoppage"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO injurystoppage (event_id, duration) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateHalfStart():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Half Start"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                except:
                    res[data[i]["id"]] = []
                    
                    res[data[i]["id"]].append(data[i]["duration"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO halfstart (event_id, duration) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateHalfEnd():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Half Start"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                except:
                    res[data[i]["id"]] = []
                    
                    res[data[i]["id"]].append(data[i]["duration"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO halfend (event_id, duration) VALUES (%s, %s)", (key, value[0]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateFiftyFifty():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "50/50"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["50_50"]["outcome"]["name"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["50_50"]["outcome"]["name"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO fiftyfifty (event_id, duration, location, outcome, under_pressure) VALUES (%s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateBallRecovery():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Ball Recovery"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)

                    if ("ball_recovery" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)

                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    if ("ball_recovery" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO ballrecovery (event_id, duration, location, under_pressure, ball_recovery) VALUES (%s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()
    

def populateDispossessed():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Dispossessed"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)

                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO dispossessed (event_id, duration, location, under_pressure) VALUES (%s, %s, %s, %s)", (key, value[0], value[1], value[2]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateClearance():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Clearance"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    res[data[i]["id"]].append(data[i]["clearance"]["body_part"]["name"])

                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    res[data[i]["id"]].append(data[i]["clearance"]["body_part"]["name"])


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO clearance (event_id, duration, location, under_pressure, body_part) VALUES (%s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateDuel():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Duel"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    res[data[i]["id"]].append(data[i]["duel"]["type"]["name"])
                    if ("outcome" in data[i]["duel"]):
                        res[data[i]["id"]].append(data[i]["duel"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("counterpress" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)

                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    res[data[i]["id"]].append(data[i]["duel"]["type"]["name"])
                    if ("outcome" in data[i]["duel"]):
                        res[data[i]["id"]].append(data[i]["duel"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("counterpress" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)



    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO duel (event_id, duration, location, under_pressure, type, outcome, counterpress) VALUES (%s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateInterception():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Interception"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("outcome" in data[i]["interception"].keys()):
                        res[data[i]["id"]].append(data[i]["interception"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("counterpress" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("outcome" in data[i]["interception"].keys()):
                        res[data[i]["id"]].append(data[i]["interception"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("counterpress" in data[i].keys()):
                        res[data[i]["id"]].append(True)
                    else:
                        res[data[i]["id"]].append(False)



    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Interception (event_id, duration, location, outcome, counterpress) VALUES (%s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateBlocks():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Block"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("block" in data[i].keys() and "offensive" in data[i]["block"].keys()):
                        res[data[i]["id"]].append(data[i]["block"]["offensive"])
                    else:
                        res[data[i]["id"]].append(False)
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("block" in data[i].keys() and "offensive" in data[i]["block"].keys()):
                        res[data[i]["id"]].append(data[i]["block"]["offensive"])
                    else:
                        res[data[i]["id"]].append(False)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Blocks (event_id, duration, location, offensive) VALUES (%s, %s, %s, %s)", (key, value[0], value[1], value[2]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populatePressure():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Pressure"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Pressures (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateCarry():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Carry"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["carry"]["end_location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    res[data[i]["id"]].append(data[i]["carry"]["end_location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Carrys (event_id, duration, location, end_location) VALUES (%s, %s, %s, %s)", (key, value[0], value[1], value[2]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateSubstitution():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Substitution"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["substitution"]["outcome"]["name"])
                    res[data[i]["id"]].append(data[i]["substitution"]["replacement"]["id"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["substitution"]["outcome"]["name"])
                    res[data[i]["id"]].append(data[i]["substitution"]["replacement"]["id"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO Substitutions (event_id, duration, outcome, replacement_id) VALUES (%s, %s, %s, %s)", (key, value[0], value[1], value[2]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateOwnGoalFor():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Own Goal For"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO owngoalfor (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateOwnGoalAgainst():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Own Goal Against"):
                try:
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO owngoalagainst (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateFoulWon():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Foul Won"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    if("foul_won" in data[i].keys() and "defensive" in data[i]["foul_won"]):
                        res[data[i]["id"]].append(data[i]["foul_won"]["defensive"])
                    else:
                        res[data[i]["id"]].append(False)
                    if("foul_won" in data[i].keys() and "advantage" in data[i]["foul_won"]):
                        res[data[i]["id"]].append(data[i]["foul_won"]["advantage"])
                    else:
                        res[data[i]["id"]].append(False)
                        
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                    if("under_pressure" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["under_pressure"])
                    else:
                        res[data[i]["id"]].append(False)
                    if("foul_won" in data[i].keys() and "defensive" in data[i]["foul_won"]):
                        res[data[i]["id"]].append(data[i]["foul_won"]["defensive"])
                    else:
                        res[data[i]["id"]].append(False)
                    if("foul_won" in data[i].keys() and "advantage" in data[i]["foul_won"]):
                        res[data[i]["id"]].append(data[i]["foul_won"]["advantage"])
                    else:
                        res[data[i]["id"]].append(False)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO foulswon (event_id, duration, location, under_pressure, defensive, advantage) VALUES (%s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateFoulCommited():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Foul Committed"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                        
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO foulscommitted (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def populateGoalKeeper():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Goal Keeper"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    if("location" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["location"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["goalkeeper"]["type"]["name"])
                    if("position" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["position"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("outcome" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("technique" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["technique"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("body_part" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["body_part"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    if("location" in data[i].keys()):
                        res[data[i]["id"]].append(data[i]["location"])
                    else:
                        res[data[i]["id"]].append(None)
                    res[data[i]["id"]].append(data[i]["goalkeeper"]["type"]["name"])
                    if("position" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["position"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("outcome" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["outcome"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("technique" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["technique"]["name"])
                    else:
                        res[data[i]["id"]].append(None)
                    if("body_part" in data[i]["goalkeeper"].keys()):
                        res[data[i]["id"]].append(data[i]["goalkeeper"]["body_part"]["name"])
                    else:
                        res[data[i]["id"]].append(None)

    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO goalkeepersevent (event_id, duration, location, type, position, outcome, technique, body_part) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateBadBehavior():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Bad Behaviour"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["bad_behaviour"]["card"]["name"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["bad_behaviour"]["card"]["name"])


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO badbehavior (event_id, duration, card) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateErrors():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Error"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO errors (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()


def populateMiscontrols():
    os.chdir(os.path.join(os.getcwd(), "..", "events"))
    files = os.listdir()
    res = {}
    for j in range(len(files)):
        f = open(files[j])
        data = json.load(f)
        for i in range(len(data)):
            if (data[i]["type"]["name"] == "Miscontrol"):
                try:
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])
                except:
                    res[data[i]["id"]] = []
                    
                    
                    res[data[i]["id"]].append(data[i]["duration"])
                    res[data[i]["id"]].append(data[i]["location"])


    connect = connectDatabase()
    cur = connect.cursor()
    for key, value in res.items():
        try:
            cur.execute("INSERT INTO miscontrols (event_id, duration, location) VALUES (%s, %s, %s)", (key, value[0], value[1]))
            connect.commit()
        except psycopg2.Error as e:
            print(e)
    cur.close()
    connect.close()

def loadData():
    populateCompetitions()
    populateSeasons()
    populateReferees()
    populateTeams()
    populateManagers()
    populateStadiums()
    populateMatches()
    populatePlayers()
    populateEvents()
    populatePositions()
    populateCards()
    populateShots()
    populatePasses()
    populateDribbles()
    populateDribbledPast()
    populatePlayerOn()
    populatePlayerOff()
    populateShields()
    populateBallReceipt()
    populateInjuryStoppage()
    populateHalfStart()
    populateHalfEnd()
    populateFiftyFifty()
    populateBallRecovery()
    populateDispossessed()
    populateClearance()
    populateDuel()
    populateInterception()
    populateBlocks()
    populatePressure()
    populateCarry()
    populateSubstitution()
    populateOwnGoalFor()
    populateOwnGoalAgainst()
    populateFoulWon()
    populateFoulCommited()
    populateGoalKeeper()
    populateBadBehavior()
    populateErrors()
    populateMiscontrols()
    return True

# loadData()
populatePasses()