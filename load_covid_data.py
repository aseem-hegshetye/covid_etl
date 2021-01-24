import json
from datetime import datetime

import pandas as pd
import requests
import sqlalchemy


class LoadCovidData:
    """
    get covid data from API url, and
    load it in db. Each county has a seperate table.
    """

    url = r'https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD'
    engine = sqlalchemy.create_engine(
        'sqlite:////home/aseem/Code/interview_assignments/covid_etl/covid'
    )

    # columns from api data that are loaded in db
    columns = [
        'test_date', 'new_positives', 'cumulative_number_of_positives',
        'total_number_of_tests', 'cumulative_number_of_tests'
    ]

    # column whose value becomes the table name
    county_col_name = 'county'

    def load_covid_data(self):
        df = self.get_data()
        self.load_data_per_county(df)

    def get_data(self):
        """
        :return: all data as a datafram
        """
        resp = requests.get(self.url)
        json_data = json.loads(resp.text)
        columns = json_data['meta']['view']['columns']
        column_names = [c['fieldName'] for c in columns]
        df = pd.DataFrame(json_data['data'], columns=column_names)
        return df

    def load_data_per_county(self, df):
        """
        :param df: dataframe to load into db.
            df has multiple counties (shown in county col)
            which get loaded in respective county tables.
        Load certain columns (self.columns) from df
        into db in respective county table. Also add load_date
        col with todays date.
        """
        for county in df[self.county_col_name].unique():
            county_df = df[df[self.county_col_name] == county]
            county_df_imp_cols = county_df[self.columns]
            county_df_imp_cols = county_df_imp_cols.assign(
                load_date=lambda x: datetime.now().date()
            )

            with self.engine.connect() as connection:
                county_df_imp_cols.to_sql(
                    county,
                    connection,
                    if_exists='append',
                    index=False,
                    method='multi'
                )


LoadCovidData().load_covid_data()
