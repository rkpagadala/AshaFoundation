import polars as pl
import tempfile
import os
from Download import extraction

def test_add_all_chapters_and_bimaru(tmp_path):
    # Minimal fixture: 2 states, 2 chapters, 2 years, with BIMARU states
    df = pl.DataFrame(
        [
            ["BIHAR",      "A", 2021, 100, 200, 50, 2],
            ["RAJASTHAN",  "A", 2021, 100, 200, 50, 2],
            ["KERALA",     "A", 2021, 50,  200, 25, 1],
            ["BIHAR",      "B", 2022, 200, 400, 50, 2],
            ["RAJASTHAN",  "B", 2022, 200, 400, 50, 2],
            ["KERALA",     "B", 2022, 100, 400, 25, 1],
        ],
        columns=["state", "chapter", "year", "state_total_amount", "total_amount", "state_percentage", "pop_adj_units"],
        orient="row",
    )
    result = extraction._add_all_chapters_and_bimaru(df)
    # Check BIMARU rows
    bimaru = result.filter(pl.col('state') == 'BIMARU')
    assert bimaru.height > 0
    # Check All Chapters rows
    all_chapters = result.filter(pl.col('chapter') == 'All Chapters')
    assert all_chapters.height > 0
    # Check output columns
    assert set(result.columns) == {'state','chapter','year','state_total_amount','total_amount','state_percentage','pop_adj_units'}

def test_create_per_population_state_chapter_year(tmp_path):
    # Write a fixture CSV
    input_path = tmp_path / 'in.csv'
    output_path = tmp_path / 'out.csv'
    data = [
        {"state": "BIHAR", "chapter": "A", "year": 2021, "state_total_amount": 100},
        {"state": "KERALA", "chapter": "A", "year": 2021, "state_total_amount": 50},
    ]
    pl.DataFrame(data).write_csv(input_path)
    extraction.create_per_population_state_chapter_year(str(input_path), str(output_path))
    out = pl.read_csv(output_path)
    # Output should have required columns
    for col in ['state','chapter','year','state_total_amount','total_amount','state_percentage','pop_adj_units']:
        assert col in out.columns
    # Output should not be empty
    assert out.height > 0
