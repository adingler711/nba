import pandas as pd
from nba_api.stats.endpoints import *
from datetime import date, timedelta
from time import sleep
from random import randint
import os


class FantasyDataBase(object):
    """
    Base class for all data aggregations
    """
    # _scoreboard_path = '../nba_data/season_scores/historical_game_scores.csv.gz'

    def __init__(self):
        self._scoreboard_path = '../nba_data/season_scores/historical_game_scores.csv.gz'

    @staticmethod
    def create_date_list(start_date, end_date):
        start_date_split = start_date.split('-')
        end_date_split = end_date.split('-')

        # start date
        d1 = date(int(start_date_split[0]),
                  int(start_date_split[1]),
                  int(start_date_split[2]))
        # end date
        d2 = date(int(end_date_split[0]),
                  int(end_date_split[1]),
                  int(end_date_split[2]))

        # timedelta
        delta = d2 - d1
        date_list = []
        for i in range(delta.days + 1):
            date_list += [str(d1 + timedelta(i))]

        return date_list

    @staticmethod
    def check_if_file_exist(file_path):
        # check if file exist
        if os.path.isfile(file_path):
            season_historical = pd.read_csv(file_path)
        else:
            season_historical = pd.DataFrame()

        return season_historical

    @staticmethod
    def create_response_df(json_response, data_label):
        response_ = json_response.get_data_sets()
        response_data = response_[data_label]['data']
        response_cols = response_[data_label]['headers']
        response_df = pd.DataFrame(response_data, columns=response_cols)

        return response_df

    def merge_historical(self, file_path, current_df):
        historical_df = self.check_if_file_exist(file_path)
        historical_df = pd.concat((historical_df, current_df)).drop_duplicates()
        historical_df.to_csv(self._scoreboard_path,
                                 index=False, compression='gzip')


class FantasyData(FantasyDataBase):

    def get_historical_scoreboards(self, start_date, end_date):
        date_list = self.create_date_list(start_date, end_date)

        date_output_df = pd.DataFrame()
        for date_ in date_list:
            scoreboard_response = scoreboardv2.ScoreboardV2(game_date=date_).nba_response
            if scoreboard_response.valid_json():
                scoreboard_df = self.create_response_df(scoreboard_response, 'GameHeader')
                scoreboard_df = pd.concat((date_output_df, scoreboard_df))
                sleep(randint(2, 6))

        # check if file exist
        self.merge_historical(self._scoreboard_path, scoreboard_df)

    def update_historical_scoreboards(self):
        scoreboard_response = scoreboardv2.ScoreboardV2().nba_response
        scoreboard_df = self.create_response_df(scoreboard_response, 'GameHeader')

        # check if file exist
        self.merge_historical(self._scoreboard_path, scoreboard_df)
