# this file will contain functions that query the PostgreSQL database for information of players, teams, games, etc
import query_executor

class DatabaseDataLoader:
    def _init_(self):
        return

    def get_all_player_ids(self):
        query_script = """select person_id from players"""

        success_status, player_ids = query_executor.execute_query(query_script=query_script, return_result=True)

        if not success_status:
            print("Failed to get player ids")

        return player_ids
    
    # functions for players table
    def get_player_ids(self, first_name, last_name):
        query_script = """
            select person_id from players where first_name like '%{}%' and last_name like '%{}%'
        """.format(first_name, last_name)

        success_status, player_ids = query_executor.execute_query(query_script, return_result=True)

        if not success_status:
            print("Failed to get player id")

        return player_ids

    def get_player_stats_for_career(self, player_id):
        query_script = """
            select
                count(*)games_recorded,
                sum(pts)points,
                sum(fgm)field_goals_made,
                sum(fga)field_goals_attempted,
                sum(fg3m)threes_made,
                sum(fg3a)threes_attempted,
                sum(ftm)freethrow_made,
                sum(fta)freethrows_attempted,
                sum(oreb)offensive_rebounds,
                sum(dreb)defensive_rebounds,
                sum(reb)rebounds,
                sum(ast)assists,
                sum(stl)steals,
                sum(blk)blocks,
                sum(tov)turnovers,
                sum(pf)fouls,
                sum(plus_minus)plus_minus
                    from (select * from player_performances where person_id = {})
        """.format(player_id)

        success_status, player_id = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get player career stats")

        return player_id

    def get_player_stats_for_season(self, player_id, season_id):
        query_script = """
            select 
                count(*)games_recorded,
                sum(player_performances.pts)points,
                sum(player_performances.fgm)field_goals_made,
                sum(player_performances.fga)field_goals_attempted,
                sum(player_performances.fg3m)threes_made,
                sum(player_performances.fg3a)threes_attempted,
                sum(player_performances.ftm)freethrow_made,
                sum(player_performances.fta)freethrows_attempted,
                sum(player_performances.oreb)offensive_rebounds,
                sum(player_performances.dreb)defensive_rebounds,
                sum(player_performances.reb)rebounds,
                sum(player_performances.ast)assists,
                sum(player_performances.stl)steals,
                sum(player_performances.blk)blocks,
                sum(player_performances.tov)turnovers,
                sum(player_performances.pf)fouls,
                sum(player_performances.plus_minus)plus_minus 
                    from player_performances where person_id = {} and game_id in (
                        select distinct player_performances.game_id
                            from player_performances join games on player_performances.game_id = games.game_id 
                                where season_id = {})
        """.format(player_id, season_id)
        
        success_status, player_stats = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get player season stats")

        return player_stats

    def get_player_stats_for_game(self, player_id, game_date):
        query_script = """
            select 	
                distinct game_date,
                player_performances.pts,
                player_performances.fgm,
                player_performances.fga,
                player_performances.fg3m,
                player_performances.fg3a,
                player_performances.ftm,
                player_performances.fta,
                player_performances.oreb,
                player_performances.dreb,
                player_performances.reb,
                player_performances.ast,
                player_performances.stl,
                player_performances.blk,
                player_performances.tov,
                player_performances.pf,
                player_performances.plus_minus 
                    from player_performances join games on player_performances.game_id = games.game_id 
                        where game_date = '{}' and person_id = {}
        """.format(game_date, player_id)


        success_status, player_stats = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get player game stats")

        return player_stats

    def get_player_info(self, player_ids):
        if len(player_ids) == 1:
            player_ids = [player_id for player_id in player_ids][0]
            query_script = """
                select first_name, last_name, birthdate, school, country, height, weight, season_exp, position, draft_year, draft_round, draft_number from players where person_id = {}
            """.format(player_ids)
        else:
            query_script = """
                select first_name, last_name, birthdate, school, country, height, weight, season_exp, position, draft_year, draft_round, draft_number from players where person_id in {}
            """.format(tuple(player_ids))

        success_status, player_info = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get player info")

        return player_info



    def get_player_games(self, player_id):
        query_script = """
            select * from player_performances where person_id = %s
        """

        query_values = (player_id,)

        success_status, player_game_ids = query_executor.execute_query(query_script, query_values)

        if not success_status:
            print("Failed to get player game ids")
        
        return player_game_ids

    # functions for games table
    def get_game_id(self, team_id, game_date):
        query_script = """
            select game_id from games where team_id = %s and game_date = %s
        """

        query_values = (team_id, game_date,)

        success_status, game_id = query_executor.execute_query(query_script, query_values)

        if not success_status:
            print("Failed to get game id")

        return game_id

    def get_game_ids(self, game_date):
        query_script = """
            select * from games where game_date = %s
        """

        query_values = (game_date,)

        success_status, game_ids = query_executor.execute_query(query_script, query_values)

        if not success_status:
            print("Failed to get game ids")

        return game_ids
    
    def get_all_game_ids(self):
        query_script = """
            select distinct game_id from games
        """

        success_statsus, game_ids = query_executor.execute_query(query_script)

        if not success_statsus:
            print("Failed to get game_ids")

        return game_ids

    # functions for teams table
    def get_team_id(self, team_name):
        query_script = """
            select team_id from teams where team_name like '%{}%'
        """.format(team_name)

        success_status, team_id = query_executor.execute_query(query_script, return_result=True)

        return team_id

    def get_all_team_info(self):
        query_script = """
            select * from teams
        """

        success_status, teams_info = query_executor.execute_query(query_script, return_result=True)

        return teams_info

    # functions for games table

    def get_team_stats_for_franchise(self, team_id):
        query_script = """
            select 
                count(*)games_recorded,
                sum(case when wl = 'W' then 1 else 0 end)wins,
                sum(case when wl = 'L' then 1 else 0 end)losses,
                sum(min)minutes,
                sum(pts)points,
                sum(fgm)field_goals_made,
                sum(fga)field_goals_attempted,
                sum(fg3m)threes_made,
                sum(fg3a)threes_attempted,
                sum(ftm)freethrow_made,
                sum(fta)freethrows_attempted,
                sum(oreb)offensive_rebounds,
                sum(dreb)defensive_rebounds,
                sum(ast)assists,
                sum(stl)steals,
                sum(blk)blocks,
                sum(tov)turnovers,
                sum(pf)fouls
		            from (select * from games where team_id = {})
        """.format(team_id)

        success_status, team_stats = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get franchise stats")
        
        return team_stats

    def get_team_stats_for_season(self, team_id, season_id):
        query_script = """
            select 
                count(*)games_recorded, sum(case when wl = 'W' then 1 else 0 end)wins,
                sum(case when wl = 'L' then 1 else 0 end)losses,
                sum(min)minutes,
                sum(pts)points,
                sum(fgm)field_goals_made,
                sum(fga)filed_goals_attempted,
                sum(fg3m)threes_made,
                sum(fg3a)threes_attempted,
                sum(ftm)freethrow_made,
                sum(fta)freethrows_attempted,
                sum(oreb)offensive_rebounds,
                sum(dreb)defensive_rebounds,
                sum(ast)assits,
                sum(stl)steals,
                sum(blk)blocks,
                sum(tov)turnovers,
                sum(pf)fouls
		            from (select * from games where team_id = {} and season_id = {})
        """.format(team_id, season_id)

        success_status, team_stats = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get team stats for season")

        return team_stats

    def get_team_stats_for_game(self, team_id, game_date):
        query_script = """
            select game_date, matchup, wl, min, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, plus_minus from games where team_id = {} and game_date = '{}'
        """.format(team_id, game_date)

        success_status, team_stats = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get team stats for game")

        return team_stats

    # functions for live_games table
    def get_live_game_data_headlines(self):
        query_script = """
            select game_id, home_team, away_team, home_score, away_score from live_games
        """

        success_status, game_data = query_executor.execute_query(query_script,)

        if not success_status:
            print("Failed to get live game data")

        return game_data

    def get_live_game_data(self, game_id):
        query_script = """
            select * from live_games where game_id = %s
        """

        query_values = (game_id,)

        success_status, game_data = query_executor.execute_query(query_script, query_values)

        if not success_status:
            print("Failed to get live game data")

        return game_data
    
    def get_live_game_ids(self):
        query_script = """
            select game_id from live_games
        """
        success_status, game_data = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get live game data")

        return game_data
    
    def get_game_play_by_play(self, team_id, game_date):
        game_id = self.get_game_id(team_id, game_date)
        if game_id.empty:
            return game_id
        else:
            game_id = game_id["game_id"].iloc[0]
        
        query_script = """
            select eventmsgtype, eventmsgactiontype, period, homedescription, visitordescription from play_by_play where game_id = '{}'
        """.format(game_id)

        success_status, play_by_play = query_executor.execute_query(query_script)

        if not success_status:
            print("Failed to get live game data")

        return play_by_play