try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
import re
import polars as pl

def read_and_analyze(i):
    try:
        with open(f"Download/HTML_DATA/ashasup_{i}.html", "r") as f:
            if BeautifulSoup is None:
                return None
            soup = BeautifulSoup(f.read(), 'html.parser')
            if not soup.find_all('div', {"class": "x-accordion-inner"}):
                return None
    except FileNotFoundError:
        return None
    return soup


def extract_funding(text, pid: int):
    parts = re.findall(r'(.*?(?:USD|INR|EUR|GBP|CHF|CAD)\s+\d+)', text)
    print(text)
    result = []
    for record in parts:
        pattern = r'(\w+)\s+(\d+)([A-Za-z\./\-\s]+)(USD|INR|EUR|GBP|CHF|CAD)\s+(\d+)'
        matches = re.match(pattern, record)

        if matches:
            month = matches.group(1)
            year = matches.group(2)
            chapter = matches.group(3)
            currency = matches.group(4)
            amount = matches.group(5)
            
            #print(month, year, chapter, currency, amount,'######')
            result.append((pid,month, year, chapter, currency, amount))
    return result


def extract_status(text,pid):
    pattern = r'Status:(.*?)Project Steward:(.*?)Project Partner...:(.*?)Other Contacts:(.*?)Project Address:(.*?)Tel:(.*?)Stewarding Chapter:(.*?)'
    match   = re.search(pattern, text)
    if match:
        Status             = match.group(1)
        Project_Steward    = match.group(2)
        Project_Partner    = match.group(3)
        Other_Contacts     = match.group(4)
        Project_Address    = match.group(5)
        Tel                = match.group(6)
        Stewarding_Chapter = match.group(7)
        extracted_state    = extract_state(Project_Address)
      #  print(Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter)
    
    result =  [pid,Status, Project_Steward, Project_Partner, Other_Contacts, Project_Address, Tel, Stewarding_Chapter,extracted_state]
    return result 

def extract_data(i):
    soupO           = read_and_analyze(i)
    pid             = i
    projects        = soupO.find_all('div', {"class": "x-accordion-inner"})
    project_glance  = projects[0].text 
    project_status  = projects[1].text
    project_funding = projects[2].text
    project_desc    = projects[3].text
    
    project_status_value  = extract_status( project_status,  pid)
    project_funding_value = extract_funding(project_funding, pid)
  #  print(project_funding_value)
   # print(project_status_value)
    column_names = ['pid','month', 'year', 'chapter', 'currency', 'amount']
    
    df = pl.DataFrame(project_funding_value, schema=column_names,orient='row')
    print(df)
    
    column_names2 = ['pid','Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter','Extracted State']
    df2 = pl.DataFrame([project_status_value], schema=column_names2,orient='row')
    print(df2)
    return project_status_value, project_funding_value

def convert_to_DF():
# Initialize empty lists to store all data
    all_funding_data = []
    all_status_data = []

    # Main loop #glob of filename in that directory extract data of that file name or pid 
    for pid in range(1, 1354):
        project_status_value, project_funding_value = extract_data(pid)
        all_status_data.append(project_status_value)
        all_funding_data.extend(project_funding_value)

    funding_column_names = ['pid', 'month', 'year', 'chapter', 'currency', 'amount']
    status_column_names = ['pid', 'Status', 'Project Steward', 'Project Partner', 'Other Contacts', 'Project Address', 'Tel', 'Stewarding Chapter','Extracted State']

    funding_df = pl.DataFrame(all_funding_data, schema=funding_column_names,orient='row')
    status_df  = pl.DataFrame(all_status_data,  schema=status_column_names,orient='row').drop_nulls()
    status_df = status_df.drop(['Project Steward', 'Project Partner', 'Other Contacts'])

    # Save to CSV
    funding_df.write_csv('DataCSV/consolidated_funding.csv')
    status_df.write_csv( 'DataCSV/consolidated_status.csv')

    
def cumulative_funding_yearCurr():
    funding_df = pl.read_csv('DataCSV/consolidated_funding.csv')
    funding_df_processed = funding_df.with_columns([
        pl.col(    'year').cast(pl.Int32),
        pl.col(  'amount').cast(pl.Float64),
        pl.col('currency').cast(pl.Utf8)
    ])
    
    yearCurr = funding_df_processed.group_by(['year', 'currency']).agg([
        pl.col('amount').sum().alias('total_amount')
    ]).sort(['year', 'currency']) 
    
    yearCurr.write_csv('DataCSV/yearCurr.csv')

def calculate_funding_pidYear():
    # Ensure 'year' and 'amount' are of correct types
    funding_df = pl.read_csv('DataCSV/consolidated_funding.csv')
    funding_df_processed = funding_df.with_columns([
        pl.col(  'year').cast(pl.Int32),
        pl.col('amount').cast(pl.Float64),
        
    ])
    # Group by year and calculate yearly total
        # Group by 'pid' and 'year', then sum the amounts for each combination
    pid_year_totals = funding_df_processed.group_by(['pid', 'year','chapter','currency']).agg([
        pl.col('amount').sum().alias('yearly_total')
    ]).sort(['pid', 'year'])

    # Calculate the cumulative sum of amounts within each 'pid'
    # funding_cumulative_df = pid_year_totals.with_columns([
    #     pl.col('yearly_total').cum_sum().over('pid').alias('cumulative_amount')
    # ])
    
    print("Cumulative Funding DataFrame:")
    print(pid_year_totals)
    print("****")

    pid_year_totals.write_csv('DataCSV/cumulative_funding.csv')

def extract_state(text):
    namechangedict = {state.upper(): state.upper() for state in [
        "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", 
        "Chhattisgarh", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", 
        "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", 
        "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland", 
        "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", 
        "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
        "Andaman and Nicobar Islands", "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu", 
        "Lakshadweep", "Delhi", "Puducherry", 
        "Ladakh", "Jammu and Kashmir", "Uttaranchal" , "Pondicherry", "Orissa" ,
    ]}
    namechangedict["PONDICHERRY"] = "PUDUCHERRY"
    namechangedict["UTTARANCHAL"] = "UTTARAKHAND"
    namechangedict["ORISSA"] = "ODISHA"

    #text = "hello bihar, this is a test"

    pattern              = "(?i)" + "|".join(namechangedict.keys())
    match                = re.search(pattern, text)
    
    if match:
        extracted_string = match.group()
        extracted_string = namechangedict[extracted_string.upper()]
    else:
        extracted_string = None
    print(f"Extracted string: {extracted_string}")

    return extracted_string

def final_df():
    status_df  = pl.read_csv('DataCSV/consolidated_status.csv')
    funding_df = pl.read_csv('DataCSV/cumulative_funding.csv')
    
    final_df = status_df.join(funding_df, on='pid', how='left').select([
    'pid', 'year', 'chapter',  'currency', 'yearly_total', 'Extracted State'
    ]).rename({'Extracted State': 'state'})
    print("Final DataFrame:")
    print(final_df)
    print("****")
    
    final_df.write_csv('DataCSV/final_df.csv')

def state_year():
    final_df = pl.read_csv('DataCSV/final_df.csv')
    # Convert 'year' to integer type if it's not already
    final_df = final_df.with_columns(pl.col('year').cast(pl.Int64))

    final_df = final_df.filter(pl.col('currency') == 'USD')
    # Perform the grouping and aggregation
    new_df = final_df.group_by(['state', 'year']).agg([
        pl.col('yearly_total').sum().alias('state_total_amount')
    ])
    new_df.write_csv('DataCSV/state_year.csv')

def state_year_chapter():
    final_df = pl.read_csv('DataCSV/final_df.csv')
    # Convert 'year' to integer type if it's not already
    final_df = final_df.with_columns(pl.col('year').cast(pl.Int64))

    final_df = final_df.filter(pl.col('currency') == 'USD')
    # Perform the grouping and aggregation
    new_df = final_df.group_by(['state', 'year', 'chapter']).agg([
        pl.col('yearly_total').sum().alias('chapter_amount')
    ]).sort(['state', 'year', 'chapter'])
    new_df.write_csv('DataCSV/state_year_chapter.csv')

def total_year_df():
    new_df        = pl.read_csv('DataCSV/state_year.csv')

    total_year_df = new_df.group_by('year').agg([
        pl.col('state_total_amount').sum().alias('total_amount')
    ]).sort('year')
    total_year_df.write_csv('DataCSV/total_year.csv')

def percentage_state_df():
    state_year_df = pl.read_csv('DataCSV/state_year.csv')
    total_year_df = pl.read_csv('DataCSV/total_year.csv')
    
    # Join state_year_df with total_year_df
    joined_df     = state_year_df.join(total_year_df, on='year', how='left')
    
    # Calculate the percentage of total amount for each state and year
    percentage_df = joined_df.with_columns((pl.col('state_total_amount')/pl.col('total_amount')* 100).alias('percentage'))
    percentage_df = percentage_df.sort(['year','state'])
    percentage_df.write_csv('DataCSV/percentage_year_state.csv')
    
    percentage_df = percentage_df.sort(['state','year'])
    percentage_df.write_csv('DataCSV/percentage_state_year.csv')
    
def state_chapter_df():
    #drop pid, currency only usd, and drop currency, 
    final_df = pl.read_csv('DataCSV/final_df.csv')
    final_df = final_df.filter(pl.col('currency') == 'USD')
    final_df = final_df.select(['year','state', 'chapter', 'yearly_total'])
    final_df = final_df.sort(['state','year'])
    final_df.write_csv('DataCSV/state_chapter.csv')
    
def percentage_state_year_chapter():
    #out x% y% came from silicon valley 
    state_year_df         = pl.read_csv('DataCSV/state_year.csv')
    state_year_chapter_df = pl.read_csv('DataCSV/state_year_chapter.csv')
    
    # Join state_year_chapter_df with total_year_df
    joined_df     = state_year_chapter_df.join(state_year_df, on=['state','year'], how='left')
    
    # Calculate the percentage of total amount for each state and year
    percentage_df = joined_df.with_columns((pl.col('chapter_amount')/pl.col('state_total_amount')* 100).alias('percentage'))
    percentage_df = percentage_df.sort(['state','year','chapter'])
    percentage_df.write_csv('DataCSV/percentage_state_year_chapter.csv')
    
def per_pop_state_year_chapter(): 
    percentage_state_year_chapter_df = pl.read_csv('DataCSV/percentage_state_year_chapter.csv')
    population_df                    = pl.read_csv('DataCSV/population.csv')

    percentage_state_year_chapter_df = percentage_state_year_chapter_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    population_df = population_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    
    population_df = population_df.select(['state', 'Population', '% of Total'])

    joined_df     = percentage_state_year_chapter_df.join(population_df, on='state', how='left')
  #  joined_df    = joined_df.with_columns((pl.col('chapter_amount')/pl.col('Population')* 100).alias('percentage_population'))
    joined_df     = joined_df.sort(['state','year','chapter'])
    joined_df.write_csv('DataCSV/per_population_state_year_chapter.csv')
    
def per_pop_state_year():
    percentage_state_year_df = pl.read_csv('DataCSV/percentage_state_year.csv')
    population_df            = pl.read_csv('DataCSV/population.csv')
    
    percentage_state_year_df = percentage_state_year_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    population_df = population_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    population_df = population_df.select(['state', 'Population', '% of Total'])
    joined_df     = percentage_state_year_df.join(population_df, on='state', how='left')
    joined_df     = joined_df.sort(['state','year'])
    joined_df.write_csv('DataCSV/per_pop_state_year.csv')
    
def per_pop_year_state():
    percentage_year_state_df = pl.read_csv('DataCSV/percentage_year_state.csv')
    population_df            = pl.read_csv('DataCSV/population.csv')
    
    percentage_year_state_df = percentage_year_state_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    
    population_df = population_df.with_columns([
        pl.col('state').cast(pl.String),
    ])
    population_df = population_df.select(['state', 'Population', '% of Total'])
    joined_df     = percentage_year_state_df.join(population_df, on='state', how='left')
    joined_df     = joined_df.sort(['state','year'])
    joined_df.write_csv('DataCSV/per_pop_year_state.csv')

def bimaru():
    # Build BIMARU aggregates from per_population_state_year_chapter.csv
    base_df = pl.read_csv('DataCSV/per_population_state_year_chapter.csv')
    bimaru_states = ['BIHAR', 'JHARKHAND', 'MADHYA PRADESH', 'CHHATTISGARH', 'RAJASTHAN', 'UTTAR PRADESH', 'UTTARAKHAND']

    # Total amount per (year, chapter) across all states
    if 'chapter_amount' in base_df.columns:
        totals = base_df.group_by(['year', 'chapter']).agg(
            pl.col('chapter_amount').sum().alias('total_amount')
        )
    else:
        # Fallback if chapter_amount is not available
        totals = base_df.group_by(['year', 'chapter']).agg(
            pl.col('state_total_amount').sum().alias('total_amount')
        )

    # BIMARU population share once (constant over time)
    population_df = pl.read_csv('DataCSV/population.csv').with_columns([
        pl.col('state').cast(pl.String)
    ])
    bimaru_pop_pct = population_df.filter(pl.col('state').is_in(bimaru_states))['% of Total'].sum()

    # Aggregate BIMARU chapter amounts per (year, chapter)
    if 'chapter_amount' in base_df.columns:
        bimaru_chapter = base_df.filter(pl.col('state').is_in(bimaru_states)).group_by(['year', 'chapter']).agg([
            pl.col('chapter_amount').sum().alias('state_total_amount')
        ])
    else:
        bimaru_chapter = base_df.filter(pl.col('state').is_in(bimaru_states)).group_by(['year', 'chapter']).agg([
            pl.col('state_total_amount').sum().alias('state_total_amount')
        ])
    bimaru_chapter = bimaru_chapter.join(totals, on=['year', 'chapter'], how='left')
    # compute state_percentage first
    bimaru_chapter = bimaru_chapter.with_columns([
        (pl.when(pl.col('total_amount') != 0)
         .then(pl.col('state_total_amount') / pl.col('total_amount') * 100)
         .otherwise(None)).alias('state_percentage'),
        pl.lit('BIMARU').alias('state')
    ])
    # then compute pop_adj_units to avoid referencing a new column in the same call
    bimaru_chapter = bimaru_chapter.with_columns([
        (pl.when(pl.lit(bimaru_pop_pct) != 0)
         .then(pl.col('state_percentage') / pl.lit(bimaru_pop_pct))
         .otherwise(None)).alias('pop_adj_units')
    ])

    bimaru_out = bimaru_chapter.select(['state', 'chapter', 'year', 'state_total_amount', 'total_amount', 'state_percentage', 'pop_adj_units']).sort(['state', 'year', 'chapter'])
    bimaru_out.write_csv('DataCSV/bimaru.csv')
    
    
def create_per_population_state_chapter_year(input_path='DataCSV/per_population_state_year_chapter.csv', output_path='DataCSV/per_population_state_chapter_year.csv'):
    """
    ppsyc as input and ppscy as output
    Generate per_population_state_chapter_year.csv in DataCSV directory using per_population_state_year_chapter.csv as input.
    Ensures columns: state, chapter, year, state_total_amount, total_amount, state_percentage, pop_adj_units.
    Calculates missing columns as needed.
    Adds 'All Chapters' summary rows for each (state, year), and BIMARU state rows.
    Sorts by ['state', 'year', 'chapter'] and writes to output_path.
    """
    df = pl.read_csv(input_path)
    df = _add_all_chapters_and_bimaru(df)
    df.write_csv(output_path)

def _add_all_chapters_and_bimaru(df):
    output_cols = [
        'state',
        'chapter',
        'year',
        'state_total_amount',
        'total_amount',
        'state_percentage',
        'pop_adj_units'
    ]

    # Determine the per-(state,year,chapter) amount column to use
    # Prefer 'chapter_amount' if present; otherwise, assume 'state_total_amount' represents per-chapter rows
    if 'chapter_amount' in df.columns:
        working = df.with_columns([
            pl.col('chapter_amount').alias('_state_amount')
        ])
    else:
        working = df.with_columns([
            pl.col('state_total_amount').alias('_state_amount')
        ])

    # Recompute total_amount per (year, chapter) to avoid double counting
    working = working.with_columns([
        pl.col('_state_amount').sum().over(['year', 'chapter']).alias('total_amount')
    ])

    # Recompute state_percentage based on the corrected totals
    working = working.with_columns([
        (pl.when(pl.col('total_amount') != 0).then(pl.col('_state_amount') / pl.col('total_amount') * 100).otherwise(None)).alias('state_percentage')
    ])

    # Compute pop-adjusted units if population share exists
    if '% of Total' in working.columns:
        working = working.with_columns([
            (pl.col('state_percentage') / pl.when(pl.col('% of Total') != 0).then(pl.col('% of Total')).otherwise(None)).alias('pop_adj_units')
        ])
    else:
        working = working.with_columns([
            pl.lit(None).alias('pop_adj_units')
        ])

    # Build All Chapters rows (sum across chapters for each state-year)
    agg_dict = {
        'state_total_amount': pl.col('_state_amount').sum(),
    }
    if '% of Total' in working.columns:
        agg_dict['% of Total'] = pl.col('% of Total').max()
    all_chapters = working.group_by(['state', 'year']).agg(**agg_dict).with_columns([
        pl.lit('All Chapters').alias('chapter')
    ])
    # Year-level totals across states (for All Chapters rows)
    year_totals = all_chapters.group_by('year').agg([
        pl.col('state_total_amount').sum().alias('total_amount')
    ])
    all_chapters = all_chapters.join(year_totals, on='year', how='left').with_columns([
        (pl.when(pl.col('total_amount') != 0).then(pl.col('state_total_amount') / pl.col('total_amount') * 100).otherwise(None)).alias('state_percentage')
    ])
    if '% of Total' in all_chapters.columns:
        all_chapters = all_chapters.with_columns([
            (pl.col('state_percentage') / pl.when(pl.col('% of Total') != 0).then(pl.col('% of Total')).otherwise(None)).alias('pop_adj_units')
        ])
    else:
        all_chapters = all_chapters.with_columns([
            pl.lit(None).alias('pop_adj_units')
        ])

    # Finalize base (per-chapter) rows: expose _state_amount as state_total_amount
    base_out = working.with_columns([
        pl.col('_state_amount').alias('state_total_amount')
    ]).select(output_cols)
    all_chapters_out = all_chapters.select(output_cols)

    df_out = pl.concat([base_out, all_chapters_out])

    # Add BIMARU aggregate rows
    bimaru_states = ['BIHAR', 'JHARKHAND', 'MADHYA PRADESH', 'CHHATTISGARH', 'RAJASTHAN', 'UTTAR PRADESH', 'UTTARAKHAND']
    sub = df_out.filter(pl.col('state').is_in(bimaru_states))
    if sub.height > 0:
        # Compute combined BIMARU population share once
        pop_df = pl.read_csv('DataCSV/population.csv').with_columns([
            pl.col('state').cast(pl.String)
        ])
        bimaru_pop_pct = pop_df.filter(pl.col('state').is_in(bimaru_states))['% of Total'].sum()
        bimaru_rows = (
            sub.group_by(['year', 'chapter']).agg([
                pl.col('state_total_amount').sum().alias('state_total_amount'),
                pl.col('total_amount').max().alias('total_amount')
            ])
            .with_columns([
                (pl.when(pl.col('total_amount') != 0)
                 .then(pl.col('state_total_amount') / pl.col('total_amount') * 100)
                 .otherwise(None)).alias('state_percentage'),
                pl.lit('BIMARU').alias('state')
            ])
            .with_columns([
                (pl.when((pl.lit(bimaru_pop_pct) != 0) & (pl.col('total_amount') != 0))
                 .then((pl.col('state_total_amount') / pl.col('total_amount') * 100) / pl.lit(bimaru_pop_pct))
                 .otherwise(None)).alias('pop_adj_units')
            ])
            .select(output_cols)
        )
        df_out = pl.concat([df_out, bimaru_rows])

    df_out = df_out.sort(['state', 'year', 'chapter'])
    return df_out

if __name__ == "__main__":
    create_per_population_state_chapter_year()

def regenerate_all_files():
        """
        Regenerates all CSV files in the correct order, based on the dataflow
        analysis.  Assumes that the HTML files in Download/HTML_DATA/
        and population.csv are present.
        """
        convert_to_DF()
        cumulative_funding_yearCurr()
        calculate_funding_pidYear()
        final_df()
        state_year()
        state_year_chapter()
        total_year_df()
        percentage_state_df()
        state_chapter_df()
        percentage_state_year_chapter()
        per_pop_state_year_chapter()
        per_pop_state_year()
        per_pop_year_state()
        bimaru()
        create_per_population_state_chapter_year()