import pandas as pd
import requests, json


class Disney_Info:
    """
    This object is to take data using information pulled from the Disney Api.
    The data is cleaned and tucked into a dictionary that is then put into a dataframe ready to be used.
    It will have a path, an sql_url, a page, a charac(ter), and a t(rans)_pose(d) dataframe.
    It can then be saved as a csv file or sent into a personal sql server.
    
    The attributes for this class:
        -Path: A string, the file path url. It is already set to the Disney Api.
        -Sql Url: A string, the url path set to a personal sql server.
        -Page: A json, uses a current url link to pull from the api.
        -Character: A dictionary, the dictionary is saved from an indexed json file and then sent
            through a method to create a cleaner dictionary made for a DataFrame object.
        -Transposed DataFrame: A DataFrame object, once the dictionary is set to a DataFrame, it needs to be flipped
            and index ripped out. This will be the final product of the wrangle method.
        
    The methods for this class are:
        -Character Dictionary: Creates a dicitionary using the Page attribute to pull the information.
        -Wrangle: Wrangles the url, loops through each page of the api and pulls all the Disney characters out into dictionaries
            using the Character Dictionary method. Then organizes those dictionaries into a DataFrame. That DataFrame is 
            further cleaned. Once finished, it returns the Transposed DataFrame attribute.
        -Save Csv: Saves the Transposed Dataframe to a csv file inside the current folder.
        -Create Sql: Exports out the Transposed DataFrame to the Sql Url, with the Table name "disney_characters".
    """
    def __init__(self,file_path = 'https://api.disneyapi.dev/character'):
        self.path = file_path
        self.sql_url = 'postgresql://tzeawoio:lJ0WLuLWyP5LtH8xAMNyGE0bp1Ft3fds@batyr.db.elephantsql.com/tzeawoio'

    def charac_dict(self) -> dict:
        character_takein = {
            'name':self.charac['name'],
            'id':self.charac['_id'],
            'films':self.charac['films'],
            'tv_shows':self.charac['tvShows'],
            'video_games':self.charac['videoGames'],
            'source_url':self.charac['sourceUrl'],
            'api_url':self.charac['url']
        }
        return character_takein

    def wrangle(self):
        self.page = (requests.get(self.path)).json()
        
        charac_info = {}
        while self.page['info']['nextPage'] != None:
            for i in range(len(self.page['data'])):
                self.charac = self.page['data'][i]
                character = self.charac_dict()
                charac_info[(character['name'])] = character
            self.page =(requests.get(self.page['info']['nextPage'])).json()
        
        df = pd.DataFrame.from_dict(charac_info)
        self.tpose_df = df.transpose().reset_index()
        self.tpose_df.drop('index',axis=1,inplace=True)
        
        for name in self.tpose_df.columns.tolist():
            for e in range(len(self.tpose_df[name])):
                if isinstance(self.tpose_df[name][e], list) and len(self.tpose_df[name][e]) > 0:
                    self.tpose_df[name][e] = ", ".join(self.tpose_df[name][e])
    
        return self.tpose_df
    
    def save_csv(self):
        self.tpose_df.to_csv('disney_character_info',index=False)
    
    def create_sql(self,df:pd.DataFrame):
        df.to_sql('disney_characters', con = self.sql_url)
        


if __name__ == '__main__':
    pipe = Disney_Info()
    df = pipe.wrangle()
    pipe.save_csv()
    pipe.create_sql(df)