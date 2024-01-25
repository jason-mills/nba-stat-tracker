from nba_api_data_loader import NbaApiDataLoader
from database_data_loader import DatabaseDataLoader
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from time import sleep
from enum import Enum

EVENTMSGACTION = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
class EventMsgType(Enum):
    FIELD_GOAL_MADE = 1
    FIELD_GOAL_MISSED = 2
    FREE_THROW_ATTEMPT = 3
    REBOUND = 4
    TURNOVER = 5
    FOUL = 6
    VIOLATION = 7
    SUBSTITUTION = 8
    TIMEOUT = 9
    JUMP_BALL = 10
    EJECTION = 11
    PERIOD_BEGIN = 12
    PERIOD_END = 13

EVENTMSGACTIONTYPE = [1, 3, 5, 6, 7, 41, 44, 47, 50, 52, 58, 66, 71, 72, 73, 75, 76, 78, 79, 80, 86, 97, 98, 99, 108]
class EventMsgActionType(Enum):
    THREE_PT_JUMP_SHOT = 1
    THREE_PT_PULLUP_JUMP_SHOT = 79
    THREE_PT_STEP_BACK_JUMP_SHOT = 80
    ALLEY_OOP_DUNK = 52
    CUTTING_DUNK_SHOT = 108
    CUTTING_FINGER_ROLL_LAYUP_SHOT = 99
    CUTTING_LAYUP_SHOT = 98
    DRIVING_FINGER_ROLL_LAYUP = 75
    DRIVING_LAYUP = 6
    DRIVING_REVERSE_LAYUP = 73
    DUNK = 7
    FINGER_ROLL_LAYUP = 71
    FLOATING_JUMP_SHOT = 78
    HOOK_SHOT = 3
    JUMP_BANK_SHOT = 66
    JUMP_SHOT = 1
    LAYUP = 5
    PULLUP_JUMP_SHOT = 79
    PUTBACK_LAYUP = 72
    REVERSE_LAYUP = 44
    RUNNING_DUNK = 50
    RUNNING_FINGER_ROLL_LAYUP = 76
    RUNNING_LAYUP = 41
    STEP_BACK_JUMP_SHOT = 80
    TIP_LAYUP_SHOT = 97
    TURNAROUND_FADEAWAY = 86
    TURNAROUND_HOOK_SHOT = 58
    TURNAROUND_JUMP_SHOT = 47

def main():
    database_loader = DatabaseDataLoader()
    nba_api_data_loader = NbaApiDataLoader()

    while True:
        print("\nChoose a selection: ")
        print("0. Exit program")
        print("1. Load Tables and Data")
        print("2. View Team Data")
        print("3. View Player Data")
        print("4. View Live Game Data")

        selection = input("\nEnter a selection: ")
        
        if selection == "0":
            break
        elif selection == "1":
            while True:
                print("Choose a selection: ")
                print("0. Back to main menu")
                print("1. Create tables")
                print("2. Reload all data")

                sub_selection = input("\nEnter a selection: ")

                if sub_selection == "0":
                    break
                elif sub_selection == "1":
                    nba_api_data_loader.create_all_tables()
                    break
                elif sub_selection == "2":
                    choice = input("Are you sure you want to reload all data?\nReloading all data will take a LONG time. Y to confirm: ")
                    if choice == "Y":
                        print("Deleting all tables")
                        nba_api_data_loader.delete_all_tables()

                        print("Recreating all tables")
                        nba_api_data_loader.create_all_tables()

                        print("Loading in teams")
                        nba_api_data_loader.load_team_info()

                        print("Loading in games")
                        nba_api_data_loader.load_team_games()
                        game_ids = database_loader.get_all_game_ids() # Need this for loading in other tables
                        game_ids = game_ids["game_id"]
                        game_ids = game_ids.to_list()

                        print("Loading in players")
                        nba_api_data_loader.load_player_info()

                        print("Loading in player performances")
                        nba_api_data_loader.load_all_player_performances(game_ids)

                        print("Loading in play by play")
                        nba_api_data_loader.load_all_play_by_play_data(game_ids)

                        print("All Table Recreated")
                    else:
                        continue
                else:
                    print("Please enter a valid selection")          
        elif selection == "2":
            team_info_columns = ["TEAM ID", "NAME", "ABBREVIATION", "NICKNAME", "CITY", "STATE", "YEAR FOUNDED"]
            display_team_info_columns = ["NAME", "ABBREVIATION", "NICKNAME", "CITY", "STATE", "YEAR FOUNDED"]
            team_stats_display_columns = [
                "GAMES RECORDED",
                "WINS",
                "LOSSES",
                "MINUTES PLAYED",
                "POINTS",
                "FIELD GOALS MADE",
                "FIELD GOALS ATTEMPTED",
                "THREES MADE",
                "THREES ATTEMTPED",
                "FREETHROWS MADE",
                "FREETHROWS ATTEMPTED",
                "OFFENSIVE REBOUNDS",
                "DEFENSIVE REBOUNDS",
                "ASSISTS",
                "STEALS",
                "BLOCKS",
                "TURNOVERS",
                "FOULS"
            ]
            game_stats_display_columns = [
                "GAME DATE",
                "MATCHUP",
                "WIN/LOSS",
                "MIN",
                "POINTS",
                "FIELD GOALS MADE",
                "FIELD GOALS ATTEMPTED",
                "FIELD GOAL PERCENTAGE",
                "THREES MADE",
                "THREES ATTEMPTED",
                "THREE POINT PERCENTAGE",
                "FREETHROWS MADE",
                "FREETHROWS ATTEMPTED",
                "FREETHROW PERCENTAGE",
                "OFFENSIVE REBOUNDS",
                "DEFENSIVE REBOUNDS",
                "REBOUNDS",
                "ASSISTS",
                "STEALS",
                "BLOCKS",
                "TURNOVERS",
                "FOULS",
                "PLUS MINUS"
            ]
            play_by_play_display_columns = [
                "PERIOD",
                "EVENT",
                "ACTION DESCRIPTION",
                "HOMEDESCRIPTION",
                "VISITORDESCRIPTION"
            ]

            teams_info = database_loader.get_all_team_info()
            teams_info.columns = team_info_columns
            teams_display = teams_info[display_team_info_columns]
            print(teams_display)
            team_abbreviations = teams_info["ABBREVIATION"]
            team_abbreviations = team_abbreviations.to_list()
            while True: 
                print("\nChoose a selection")
                print("0. Back to main menu")
                print("Enter team abbreviation for more options")
                sub_selection = input("\nEnter a selection: ")

                if sub_selection == "0":
                    break
                elif sub_selection in team_abbreviations:
                    team_id = str(teams_info.loc[teams_info["ABBREVIATION"] == sub_selection]["TEAM ID"].iloc[0])

                    while True:
                        print("\nChoose a selection")
                        print("0. Return to previous menu")
                        print("1. Get team stats for franchise")
                        print("2. Get team stats for season")
                        print("3. Get team stats for game")
                        print("4. See play by play for game")

                        sub_sub_selection = input("")

                        if sub_sub_selection == "0":
                            break
                        elif sub_sub_selection == "1":
                            franchise_stats = database_loader.get_team_stats_for_franchise(team_id)
                            franchise_stats.columns = team_stats_display_columns
                            print(franchise_stats)
                        elif sub_sub_selection == "2":
                            season_id = "2" + input("Enter starting year of season: ")
                            season_stats = database_loader.get_team_stats_for_season(team_id, season_id)
                            if season_stats.empty:
                                print("Could not find stats for that season")
                            else:
                                season_stats.columns = team_stats_display_columns
                                print(season_stats)
                        elif sub_sub_selection == "3":
                            game_date = input("Enter date of game (YYYY-MM-DD): ")
                            game_stats = database_loader.get_team_stats_for_game(team_id, game_date)
                            if game_stats.empty:
                                print("Could not find stats for that date")
                            else:
                                game_stats.columns = game_stats_display_columns
                                print(game_stats)
                        elif sub_sub_selection == "4":
                            game_date = input("Enter date of tame (YYYY-MM-DD): ")
                            game_play_by_play = database_loader.get_game_play_by_play(team_id, game_date)
                            if game_play_by_play.empty:
                                print("Could not load play by play for that date")
                            else:
                                print(*play_by_play_display_columns, sep = " | ")
                                for index, row in game_play_by_play.iterrows():
                                    period = row["period"]
                                    event = row["eventmsgtype"]
                                    action_description = row["eventmsgactiontype"]
                                    homedescription = row["homedescription"]
                                    visitordescription = row["visitordescription"]
                                    if event in EVENTMSGACTION:
                                        event = EventMsgType(event).name
                                    if action_description in EVENTMSGACTIONTYPE:
                                        action_description = EventMsgActionType(action_description).name

                                    print(period, event, action_description, homedescription, visitordescription, sep=" | ")
                        else:
                            print("Please enter a valid selection")
                else:
                    print("Please enter a valid selection")
        elif selection == "3":
            while True:
                display_player_info_columns = ["FIRST NAME", "LAST NAME", "BIRTHDATE", "SCHOOL", "COUNTRY", "HEIGHT", "WEIGHT", "SEASON EXPERIENCE", "POSITION", "DRAFT YEAR", "DRAFT ROUND", "DRAFT NUMBER"]
                display_player_stats_range_columns = [
                    "GAMES RECORDED",
                    "POINTS",
                    "FIELD GOALS MADE",
                    "FIELD GOALS ATTEMPTED",
                    "THREES MADE",
                    "THREES ATTEMPTED",
                    "FREETHROWS MADE",
                    "FREETHROWS ATTEMPTED",
                    "OFFENSIVE REBOUNDS",
                    "DEFENSIVE REBOUNDS",
                    "REBOUNDS",
                    "ASSISTS",
                    "STEALS",
                    "BLOCKS",
                    "TURNOVERS",
                    "FOULS",
                    "PLUS MINUS"
                ]
                display_player_stats_game_columns = [
                    "GAME DATE",
                    "POINTS",
                    "FIELD GOALS MADE",
                    "FIELD GOALS ATTEMPTED",
                    "THREES MADE",
                    "THREES ATTEMPTED",
                    "FREETHROWS MADE",
                    "FREETHROWS ATTEMPTED",
                    "OFFENSIVE REBOUNDS",
                    "DEFENSIVE REBOUNDS",
                    "REBOUNDS",
                    "ASSISTS",
                    "STEALS",
                    "BLOCKS",
                    "TURNOVERS",
                    "FOULS",
                    "PLUS MINUS"
                ]

                first_name = input("\nPlayer First Name: ")
                last_name = input("Player Last Name: ")

                player_ids = database_loader.get_player_ids(first_name, last_name)

                if player_ids.empty:
                    print("\nNo players with that name found")
                    break
                    
                else:
                    print("\nFound ", player_ids.shape[0], " player(s): ")

                    players_info = database_loader.get_player_info(tuple(player_ids["person_id"]))
                        
                    players_info.columns = display_player_info_columns
                    print(players_info)

                    print("Enter player index to continue or anything else to go back")
                    player_index = int(input("Player index: "))

                    
                    if player_index in range(player_ids.shape[0]):
                        player_id = player_ids.iloc[player_index].item()

                        while True:
                            print("Choose a selection")
                            print("0. Back to previous menu")
                            print("1. See player stats for career")
                            print("2. See player stats for season")
                            print("3. See player stats for game")

                            sub_selection = input("\nEnter a selection: ")

                            if sub_selection == "0":
                                break
                            elif sub_selection == "1":
                                career_stats = database_loader.get_player_stats_for_career(player_id)
                                career_stats.columns = display_player_stats_range_columns
                                print(career_stats)
                            elif sub_selection == "2":
                                season_id = "2" + input("Enter starting year of season: ")
                                season_stats = database_loader.get_player_stats_for_season(player_id, season_id)
                                if season_stats.empty:
                                    print("No player games for that date found")
                                else:
                                    season_stats.columns = display_player_stats_range_columns
                                    print(season_stats)
                            elif sub_selection == "3":
                                game_date = input("Enter date of tame (YYYY-MM-DD): ")
                                game_stats = database_loader.get_player_stats_for_game(player_id, game_date)
                                if game_stats.empty:
                                    print("No player games for that date found")
                                else:
                                    game_stats.columns = display_player_stats_game_columns
                                    print(game_stats)
                            else:
                                print("Please enter a valid selection")
                    else:
                        pass
        elif selection == "4":
            display_live_games_overview_columns = ["GAME ID", "HOME TEAM", "AWAY TEAM", "HOME SCORE", "AWAY SCORE"]
            print("CTRL + C to return to main menu")
            print(*display_live_games_overview_columns)

            nba_api_data_loader.load_live_game_data()
            try:
                while True:
                    nba_api_data_loader.update_live_game_data()
                    live_games = database_loader.get_live_game_data_headlines()
                    if live_games.empty:
                        print("\rNo live games found", end="")
                    else:
                        live_games.columns = display_live_games_overview_columns
                        live_games = live_games[display_live_games_overview_columns]

                        print(live_games.to_string(header=False), "\n")

                    sleep(10)
            except KeyboardInterrupt:
                nba_api_data_loader.delete_live_game_data()
                pass

    return 0


if __name__ == "__main__":
    main()