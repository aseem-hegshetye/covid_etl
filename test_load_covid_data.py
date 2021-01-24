import unittest

import pandas as pd
import sqlalchemy

from load_covid_data import LoadCovidData


class TestLoadCovidData(
    LoadCovidData,
    unittest.TestCase
):
    @classmethod
    def setUpClass(cls):
        cls.engine = sqlalchemy.create_engine(
            'sqlite:////home/aseem/Code/interview_assignments/covid_etl/covid_test'
        )


    def test_get_data(self):
        """
        test that all columns that are going to be stored in db
        exist in the API data. Also test that county_col_name
        (column whose value becomes the table name) exist
        """
        df = self.get_data()
        for c in self.columns:
            self.assertTrue(
                c in df.columns
            )
        self.assertTrue(
            self.county_col_name in df.columns
        )

    def test_load_data_per_county(self):
        """
        test that data for one county 
        is correctly being loaded in db.
        Make sure that county table doesnt exist in db prior
        to loading its data using load_data_per_county func.
        """
        df = self.get_data()
        county = df[self.county_col_name].unique()[0]
        county_df = df[df[self.county_col_name] == county]

        self.assertFalse(
            self.check_table_exists(tablename=county)
        )
        self.load_data_per_county(county_df)
        self.assertTrue(
            self.check_table_exists(tablename=county)
        )

    def check_table_exists(self, tablename):
        """
        :param tablename: table name to check
        :return: bool. True if table exists,
            else false
        """
        sql = """
                SELECT count(*) as col1 FROM sqlite_master 
                WHERE type='table' AND name='{}';
            """.format(tablename)

        with self.engine.connect() as conn:
            resp = pd.read_sql(sql, conn)
            return resp['col1'].values[0] == 1

    def tearDown(self):
        super().tearDown()
        self.drop_all_tables()

    def drop_all_tables(self):
        sql = """
        select name from sqlite_master where type='table';
        """
        with self.engine.connect() as conn:
            resp = pd.read_sql(sql, conn)
            print(f'resp={resp}')
            for table_name in resp['name'].values:
                delete_sql = """
                drop table if exists {};
                """.format(table_name)
                conn.execute(delete_sql)


if __name__ == '__main__':
    unittest.main()
