import os
import unittest
import polars as pl
from extraction import (
    percentage_state_df,
    state_year_chapter,
    per_pop_state_year_chapter,
    bimaru
)

class TestExtractionFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.makedirs('DataCSV', exist_ok=True)
        # Create mock data for state_year.csv
        pl.DataFrame({
            'state': ['STATE1', 'STATE2'],
            'year': [2020, 2020],
            'state_total_amount': [100, 200]
        }).write_csv('DataCSV/state_year.csv')
        # Create mock data for total_year.csv
        pl.DataFrame({
            'year': [2020],
            'total_amount': [300]
        }).write_csv('DataCSV/total_year.csv')
        # Create mock data for final_df.csv
        pl.DataFrame({
            'state': ['STATE1', 'STATE2'],
            'year': [2020, 2020],
            'chapter': ['CH1', 'CH2'],
            'currency': ['USD', 'USD'],
            'yearly_total': [100, 200]
        }).write_csv('DataCSV/final_df.csv')
        # Create mock data for percentage_state_year_chapter.csv
        pl.DataFrame({
            'state': ['STATE1', 'STATE2'],
            'year': [2020, 2020],
            'chapter': ['CH1', 'CH2'],
            'percentage': [33.33, 66.67]
        }).write_csv('DataCSV/percentage_state_year_chapter.csv')
        # Create mock population.csv
        pl.DataFrame({
            'state': ['STATE1', 'STATE2'],
            'population': [1000, 2000]
        }).write_csv('DataCSV/population.csv')

    def test_percentage_state_df(self):
        percentage_state_df()
        df = pl.read_csv('DataCSV/percentage_year_state.csv')
        self.assertIn('percentage', df.columns)
        self.assertAlmostEqual(df.filter(pl.col('state') == 'STATE1').select('percentage').item(), 100/300*100)
        self.assertAlmostEqual(df.filter(pl.col('state') == 'STATE2').select('percentage').item(), 200/300*100)

    def test_state_year_chapter(self):
        state_year_chapter()
        df = pl.read_csv('DataCSV/state_year_chapter.csv')
        self.assertIn('chapter_amount', df.columns)
        self.assertEqual(df.shape[0], 2)
        self.assertEqual(df['chapter_amount'].to_list(), [100, 200])

    def test_per_pop_state_year_chapter(self):
        per_pop_state_year_chapter()
        df = pl.read_csv('DataCSV/per_population_state_year_chapter.csv')
        self.assertIn('population', df.columns)
        self.assertEqual(df.shape[0], 2)
        self.assertListEqual(df['population'].to_list(), [1000, 2000])

    def test_bimaru(self):
        # Add a BIMARU state row to per_population_state_year_chapter.csv
        bimaru_df = pl.read_csv('DataCSV/per_population_state_year_chapter.csv').vstack(
            pl.DataFrame({'state': ['BIHAR'], 'year': [2020], 'chapter': ['CH3'], 'percentage': [50.0], 'population': [3000]})
        )
        bimaru_df.write_csv('DataCSV/per_population_state_year_chapter.csv')
        bimaru()
        df = pl.read_csv('DataCSV/bimaru.csv')
        self.assertTrue('BIHAR' in df['state'].to_list())
        self.assertEqual(df.shape[0], 1)

    @classmethod
    def tearDownClass(cls):
        # Clean up test files
        files = [
            'state_year.csv', 'total_year.csv', 'final_df.csv',
            'percentage_state_year_chapter.csv', 'population.csv',
            'percentage_year_state.csv', 'state_year_chapter.csv',
            'per_population_state_year_chapter.csv', 'bimaru.csv'
        ]
        for f in files:
            try:
                os.remove(f'DataCSV/{f}')
            except FileNotFoundError:
                pass

if __name__ == '__main__':
    unittest.main()
