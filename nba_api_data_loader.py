# this file will contain functions that load data into the database using the nba_api module

# Imports from libraries
import json
import multiprocessing.pool
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import boxscoretraditionalv2, commonplayerinfo, leaguegamefinder, playbyplay as historic_play
from nba_api.stats.static import teams, players
import pandas as pd
from sqlalchemy import create_engine, Date, String, REAL, Integer, BigInteger
import tqdm
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local imports
import secret
import query_executor

#create tables function
class NbaApiDataLoader:
    def _init_(self):
        return
    
    # Functions for large interactions with database
    # Creating all tables, dropping all tables, etc
    def create_all_tables(self):
        self.create_teams_table()
        self.create_games_table()
        self.create_players_table()
        self.create_live_games_table()
        self.create_player_performances_table()
        self.create_play_by_play_table()
        
        return
    
    def delete_all_tables(self):
        query_script = """
            drop table if exists
            teams,
            players,
            games,
            live_games,
            play_by_play,
            player_performances
        """
        
        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Could not delete tables from database")

        return

    # Functions for interacting with teams data
    def create_teams_table(self):
        query_script = """
            create table teams(
            team_id bigint primary key,
            name varchar,
            abbreviation varchar,
            nickname varchar,
            city varchar,
            state varchar,
            year_founded integer
            )
            """
        
        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Unable to create team table")

        return

    def load_team_info(self):
        engine = create_engine(secret.connection_string)

        nba_teams = teams.get_teams()
        nba_teams = pd.DataFrame(nba_teams)
        nba_teams = nba_teams.rename(columns={
            "id" : "team_id",
            "full_name" : "name",
        })

        try:
            nba_teams.to_sql('teams', engine, if_exists="append", index=False, 
                dtype={
                    "team_id" : BigInteger,
                    "name" : String,
                    "abbreviation": String,
                    "nickname" : String,
                    "city" : String,
                    "state": String,
                    "year_founded" : Integer
                })
        except:
            print("Teams already loaded")

        return

    # Functions for interacting with games data
    def create_games_table(self):
        query_script = """
            create table games(
            season_id integer,
            team_id bigint,
            team_abbreviation varchar,
            team_name varchar,
            game_id varchar,
            game_date date,
            matchup varchar,
            wl varchar,
            min integer,
            pts integer,
            fgm integer,
            fga integer,
            fg_pct real,
            fg3m integer,
            fg3a integer,
            fg3_pct real,
            ftm integer,
            fta integer,
            ft_pct real,
            oreb integer,
            dreb integer,
            reb integer,
            ast integer,
            stl integer,
            blk integer,
            tov integer,
            pf integer,
            plus_minus integer)
        """

        query_script_2 = """alter table games add primary key (team_id, game_id)"""

        success_status, results = query_executor.execute_query(query_script, return_result=False)
        
        if not success_status:
            print("Unable to create game table")
            return

        success_status, results = query_executor.execute_query(query_script_2, return_result=False)

        if not success_status:
            print("Unable to modify game table")
            return

        return    

    def get_team_ids(self):
        nba_teams = pd.DataFrame.from_dict(teams.get_teams())
        team_ids = nba_teams["id"]
        team_ids = team_ids.tolist()

        return team_ids

    def get_team_games(self, team_id):
        try:
            team_info = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id).get_data_frames()[0]
        except:
            return pd.DataFrame()

        return team_info

    def get_all_team_games(self, team_ids):
        pool = multiprocessing.pool.ThreadPool(processes=30)
        teams_info = pd.concat(tqdm.tqdm(pool.imap_unordered(self.get_team_games, team_ids), total=len(team_ids)), ignore_index=True)
        pool.close()
        pool.join()

        return teams_info
    
    def load_team_games(self):
        team_ids = self.get_team_ids()

        nba_games = self.get_all_team_games(team_ids)

        nba_games.columns = map(str.lower, nba_games.columns)
        nba_games = nba_games.groupby(['game_id', 'team_id']).agg({
            'season_id' : 'first',
            'team_id' : 'first',
            'team_abbreviation' : 'first',
            'team_name' : 'first',
            'game_id' : 'first',
            'game_date' : 'first',
            'matchup' : 'first',
            'wl' : 'first',
            'min' : 'sum',
            'pts' : 'sum',
            'fgm' : 'sum',
            'fga' : 'sum',
            'fg_pct' : 'sum',
            'fg3m' : 'sum',
            'fg3a' : 'sum',
            'fg3_pct' : 'sum',
            'ftm' : 'sum',
            'fta' : 'sum',
            'ft_pct' : 'sum',
            'oreb' : 'sum',
            'dreb' : 'sum',
            'reb' : 'sum',
            'ast' : 'sum',
            'stl' : 'sum',
            'blk' : 'sum',
            'tov' : 'sum',
            'pf' : 'sum',
            'plus_minus' : 'sum'}
        )

        engine = create_engine(secret.connection_string)

        try:
            nba_games.to_sql('games', engine, if_exists="append", index=False, 
                dtype={'season_id' : String,
                'team_id' : BigInteger,
                'team_abbreviation' : String,
                'team_name' : String,
                'game_id' : String,
                'game_date' : Date,
                'matchup' : String,
                'wl' : String,
                'min' : Integer,
                'pts' : Integer,
                'fgm' : Integer,
                'fga' : Integer,
                'fg_pct' : REAL,
                'fg3m' : Integer,
                'fg3a' : Integer,
                'fg3_pct' : REAL,
                'ftm' : Integer,
                'fta' : Integer,
                'ft_pct' : REAL,
                'oreb' : Integer,
                'dreb' : Integer,
                'reb' : Integer,
                'ast' : Integer,
                'stl' : Integer,
                'blk' : Integer,
                'tov' : Integer,
                'pf' : Integer,
                'plus_minus' : Integer}
            )
        except Exception as error:
            print(error)

    # Functions for interacting with players data
    def create_players_table(self):
        query_script = """
            create table players (
            person_id bigint primary key,
            first_name varchar,
            last_name varchar,
            birthdate date,
            school varchar,
            country varchar,
            height varchar,
            weight varchar,
            season_exp integer,
            position varchar,
            draft_year varchar,
            draft_round varchar,
            draft_number varchar)
        """

        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Unable to create player table")

        return
    
    def get_player_ids(self):
        player_ids = pd.DataFrame.from_dict(players.get_players())
        player_ids = player_ids["id"]
        player_ids = player_ids.to_list()

        return player_ids

    def get_player_info(self, id):
        try:
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=id).get_data_frames()[0]
        except:
            return pd.DataFrame()
        
        return player_info
    
    def get_all_player_info(self, player_ids):
        pool = multiprocessing.pool.ThreadPool(processes=200)
        players_info = pd.concat(tqdm.tqdm(pool.imap_unordered(self.get_player_info, player_ids), total=len(player_ids)), ignore_index=True)
        pool.close()
        pool.join()

        return players_info
    
    def load_player_info(self):
        player_ids = self.get_player_ids()
        players_info = self.get_all_player_info(player_ids)
        players_info.columns = map(str.lower, players_info.columns)
        players_info = players_info[["person_id", "first_name", "last_name", "birthdate", "school", "country", "height", "weight", "season_exp", "position", "draft_year", "draft_round", "draft_number"]]

        engine = create_engine(secret.connection_string)

        try:
            players_info.to_sql('players', engine, if_exists="append", index=False, 
                dtype={"person_id" : BigInteger,
                "first_name" : String,
                "last_name" : String,
                "birthdate" : Date,
                "school" : String,
                "country" : String,
                "height" : String,
                "weight" : String,
                "season_exp" : Integer,
                "position" : String,
                "draft_year" : String,
                "draft_round" : String,
                "draft_number" : String
                }
            )
        except Exception as error:
            print(error)

    # Functions for interacting with live_games data

    def create_live_games_table(self):
        query_script = """
            create table live_games(
            game_id varchar primary key,
            home_team varchar,
            away_team varchar,
            home_score integer,
            away_score integer
            )
        """

        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Unable to create live games table")

        return

    def get_live_game_data(self, game_id=None):
        board = scoreboard.ScoreBoard()
        games = board.games.get_dict()
        live_games = pd.DataFrame(columns=["game_id", "home_team", "away_team", "home_score", "away_score"])

        for game in games:
            game_id = game["gameId"]
            home_team = game["homeTeam"]["teamName"]
            home_score = game["homeTeam"]["score"]
            away_team = game["awayTeam"]["teamName"]
            away_score = game["awayTeam"]["score"]
            
            live_game = pd.DataFrame([[game_id, home_team, away_team, home_score, away_score]], columns=["game_id", "home_team", "away_team", "home_score", "away_score"])
            live_games = pd.concat([live_games, live_game], ignore_index=True)

        return live_games
    
    def load_live_game_data(self):
        live_games = self.get_live_game_data()

        if live_games.empty:
            return 

        query_script = """
            insert into live_games (game_id, home_team, away_team, home_score, away_score) values(%s, %s, %s, %s, %s)
        """

        for index, row in live_games.iterrows():
            query_values = (row["game_id"], row["home_team"], row["away_team"], row["home_score"], row["away_score"])
        
            success_status, results = query_executor.execute_query(query_script, query_values, return_result=False)

            if not success_status:
                print("Unable to load live game data")

        return
        
    def update_live_game_data(self):
        live_games = self.get_live_game_data()

        if live_games.empty:
            return 
        
        query_script = """
            update live_games set home_score = %s, away_score = %s where game_id = %s
        """

        for index, row in live_games.iterrows():
            query_values = (row["home_score"], row["away_score"], row["game_id"])

            success_status, results = query_executor.execute_query(query_script, query_values, return_result=False)
            

            if not success_status:
                print("Could not update live game")

        return

    def delete_live_game_data(self):
        query_script = """
            delete from live_games where true
        """

        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Unable to delete live games data")

        return

    # Functions for interacting with player_performance data

    def create_player_performances_table(self):
        query_script = """
            create table player_performances(
            "game_id" varchar,
            "person_id" bigint,
            "start_position" varchar,
            "comment" varchar,
            "fgm" integer,
            "fga" integer,
            "fg_pct" real,
            "fg3m" integer,
            "fg3a" integer,
            "fg3_pct" real,
            "ftm" integer,
            "fta" integer,
            "ft_pct" real,
            "oreb" integer,
            "dreb" integer,
            "reb" integer,
            "ast" integer,
            "stl" integer,
            "blk" integer,
            "tov" integer,
            "pf" integer,
            "pts" integer,
            "plus_minus" integer
            )
        """
        query_script_2 = """
            alter table player_performances add primary key (game_id, person_id)
        """

        success_status, results = query_executor.execute_query(query_script, return_result=False)
        if not success_status:
            print("Unable to create player performances table")

        success_status, results = query_executor.execute_query(query_script_2, return_result=False)

        if not success_status:
            print("Unable to modify player performance table")

        return

    def get_player_performance_data(self, game_id):
        try:
            game_performances = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[0]
        except:
            return pd.DataFrame()
        
        return game_performances
    
    def get_all_player_performances(self, game_ids):
        pool = multiprocessing.pool.ThreadPool(processes=100)
        player_performances = pd.concat(tqdm.tqdm(pool.imap_unordered(self.get_player_performance_data, game_ids), total=len(game_ids)), ignore_index=True)
        pool.close()
        pool.join()

        return player_performances
    
    def load_all_player_performances(self, game_ids):
        player_performances = self.get_all_player_performances(game_ids)
        player_performances.columns = map(str.lower, player_performances.columns)
        player_performances = player_performances.rename(columns={"player_id" : "person_id"})
        player_performances = player_performances[[
            "game_id",
            "person_id",
            "start_position",
            "comment",
            "fgm",
            "fga",
            "fg_pct",
            "fg3m",
            "fg3a",
            "fg3_pct",
            "ftm",
            "fta",
            "ft_pct",
            "oreb",
            "dreb",
            "reb",
            "ast",
            "stl",
            "blk",
            "tov",
            "pf",
            "pts",
            "plus_minus"
        ]]

        engine = create_engine(secret.connection_string)

        try:
            player_performances.to_sql('player_performances', engine, if_exists="append", index=False, 
                dtype={"game_id" : String,
                    "person_id" : BigInteger,
                    "start_position" : String,
                    "comment" : String,
                    "min" : Integer,
                    "fgm" : Integer,
                    "fga" : Integer,
                    "fg_pct" : REAL,
                    "fg3m" : Integer,
                    "fg3a" : Integer,
                    "fg3_pct" : REAL,
                    "ftm" : Integer,
                    "fta" : Integer,
                    "ft_pct" : REAL,
                    "oreb" : Integer,
                    "dreb" : Integer,
                    "reb" : Integer,
                    "ast" : Integer,
                    "stl" : Integer,
                    "blk" : Integer,
                    "to" : Integer,
                    "pf" : Integer,
                    "pts" : Integer,
                    "plus_minus" : Integer
                }
            )
        except Exception as error:
            print(error)

    # Functions for interacting with play_by_play data
    def create_play_by_play_table(self):
        query_script = """
            create table play_by_play(
            game_id varchar,
            eventnum integer,
            eventmsgtype integer,
            eventmsgactiontype integer,
            period integer,
            pctimestring varchar,
            homedescription varchar,
            neutraldescription varchar,
            visitordescription varchar,
            score varchar,
            scoremargin varchar
            )
        """

        query_script_2 = """
            alter table play_by_play add primary key (game_id, eventnum)
        """

        success_status, results = query_executor.execute_query(query_script, return_result=False)

        if not success_status:
            print("Unable to create play by play table")

        success_status, results = query_executor.execute_query(query_script_2, return_result=False)
        
        if not success_status:
            print("Unable to alter play by play table")

        return

    def get_play_by_play(self, game_id):
        try:
            play_by_play = historic_play.PlayByPlay(game_id=game_id).get_data_frames()[0]
        except:
            return pd.DataFrame()

        return play_by_play
    
    def get_all_play_by_play_data(self, game_ids):
        pool = multiprocessing.pool.ThreadPool(processes=100)
        play_by_plays = pd.concat(tqdm.tqdm(pool.imap_unordered(self.get_play_by_play, game_ids), total=len(game_ids)), ignore_index=True)
        pool.close()
        pool.join()

        return play_by_plays

    def load_all_play_by_play_data(self, game_ids):
        play_by_plays = self.get_all_play_by_play_data(game_ids)
        play_by_plays.columns = map(str.lower, play_by_plays.columns)
        play_by_plays = play_by_plays[[
            "game_id",
            "eventnum",
            "eventmsgtype",
            "eventmsgactiontype",
            "period",
            "pctimestring",
            "homedescription",
            "neutraldescription",
            "visitordescription",
            "score",
            "scoremargin"
        ]]

        engine = create_engine(secret.connection_string)

        try:
            play_by_plays.to_sql('play_by_play', engine, if_exists="append", index=False, 
                dtype={"game_id" : String,
                "eventnum" : Integer,
                "eventmsgtype" : Integer,
                "eventmsgactiontype" : Integer,
                "period" : Integer,
                "pctimestring" : String,
                "homedescription" : String,
                "neutraldescription" : String,
                "visitordescription" : String,
                "score" : Integer,
                "scoremargin" : Integer
                }
            )
        except Exception as error:
            print(error)

        return