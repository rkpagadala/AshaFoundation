from bs4 import BeautifulSoup
import re
import polars as pl

def read_and_analyze(i):
    try:
        with open(f"Download/HTML_DATA/ashasup_{i}.html", "r") as f:
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
    #population_df = pl.read_csv('DataCSV/population.csv')
    per_pop_state_year_chapter_df = pl.read_csv('DataCSV/per_population_state_year_chapter.csv')
    
    bimaru_states = ['BIHAR', 'JHARKHAND', 'MADHYA PRADESH', 'CHHATTISGARH', 'RAJASTHAN', 'UTTAR PRADESH', 'UTTARAKHAND']
    bimaru_df     = per_pop_state_year_chapter_df.filter(pl.col('state').is_in(bimaru_states))
    bimaru_df     = bimaru_df.with_columns([
                    pl.col('Population').cast(pl.Int64),
                ])
    bimaru_population = bimaru_df['Population'].unique().sum()
    bimaru_percentage = bimaru_df['% of Total'].unique().sum()
    bimaru_chapter_amount = bimaru_df['chapter_amount'].sum()
    # bimaru_state_total_amount = bimaru_df['state_total_amount'].sum()
    
    per_pop_state_year_chapter_df = per_pop_state_year_chapter_df.with_columns([
        (pl.when(pl.col("state").is_in(bimaru_states)).then(pl.lit("Bimaru")).otherwise(pl.col("state")).alias("state")),
        (pl.when(pl.col("state").is_in(bimaru_states)).then(pl.lit(bimaru_population)).otherwise(pl.col("Population")).alias("Population")),
        (pl.when(pl.col("state").is_in(bimaru_states)).then(pl.lit(bimaru_percentage)).otherwise(pl.col("% of Total")).alias("% of Total")),
        (pl.when(pl.col("state").is_in(bimaru_states)).then(pl.lit(bimaru_chapter_amount)).otherwise(pl.col("chapter_amount")).alias("chapter_amount")),
       # (pl.when(pl.col("state").is_in(bimaru_states)).then(pl.lit(bimaru_state_total_amount)).otherwise(pl.col("state_total_amount")).alias("state_total_amount")),
    ])
    per_pop_state_year_chapter_df = per_pop_state_year_chapter_df.with_columns([
        (pl.col('state_total_amount')/pl.col('% of Total')).alias('state_total_amount_per_capita'),
    ])
    
    per_pop_state_year_chapter_df.write_csv('DataCSV/bimaru.csv')
    
    
def create_per_population_state_chapter_year(input_path='DataCSV/per_population_state_year_chapter.csv', output_path='DataCSV/per_population_state_chapter_year.csv'):
    """
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
    # Ensure state_total_amount exists (should be present)
    if 'state_total_amount' not in df.columns:
        raise ValueError('state_total_amount column is required in input')
    # total_amount: sum for (year, chapter)
    if 'total_amount' not in df.columns:
        df = df.with_columns([
            pl.col('state_total_amount').sum().over(['year', 'chapter']).alias('total_amount')
        ])
    # state_percentage: (state_total_amount / total_amount) * 100
    if 'state_percentage' not in df.columns:
        df = df.with_columns([
            (pl.col('state_total_amount') / pl.col('total_amount') * 100).alias('state_percentage')
        ])
    # pop_adj_units: (state_percentage / % of Total) if % of Total exists
    if 'pop_adj_units' not in df.columns:
        if '% of Total' in df.columns:
            df = df.with_columns([
                (pl.col('state_percentage') / pl.when(pl.col('% of Total') != 0).then(pl.col('% of Total')).otherwise(None)).alias('pop_adj_units')
            ])
        else:
            df = df.with_columns([
                pl.lit(None).alias('pop_adj_units')
            ])
    # --- Add All Chapters rows ---
    group_cols = ['state', 'year']
    agg_dict = {
        'state_total_amount': pl.col('state_total_amount').sum(),
        'total_amount': pl.col('total_amount').sum(),
    }
    if '% of Total' in df.columns:
        agg_dict['% of Total'] = pl.col('% of Total').max()
    all_chapters = df.group_by(group_cols).agg(**agg_dict)
    all_chapters = all_chapters.with_columns([
        pl.lit('All Chapters').alias('chapter'),
        (pl.col('state_total_amount') / pl.col('total_amount') * 100).alias('state_percentage')
    ])
    if '% of Total' in df.columns:
        all_chapters = all_chapters.with_columns([
            (pl.col('state_percentage') / pl.when(pl.col('% of Total') != 0).then(pl.col('% of Total')).otherwise(None)).alias('pop_adj_units')
        ])
    else:
        all_chapters = all_chapters.with_columns([
            pl.lit(None).alias('pop_adj_units')
        ])
    all_chapters = all_chapters.select(output_cols)
    df = df.select(output_cols)
    df = pl.concat([df, all_chapters])
    # --- Add BIMARU rows ---
    bimaru_states = [
        'BIHAR', 'JHARKHAND', 'MADHYA PRADESH', 'CHHATTISGARH', 'RAJASTHAN', 'UTTAR PRADESH', 'UTTARAKHAND'
    ]
    bimaru_rows = []
    for chapter in df['chapter'].unique().to_list():
        for year in df['year'].unique().to_list():
            mask = (df['state'].is_in(bimaru_states)) & (df['chapter'] == chapter) & (df['year'] == year)
            sub = df.filter(mask)
            if sub.height == 0:
                continue
            state_total_amount = sub['state_total_amount'].sum()
            total_amount = sub['total_amount'].sum()
            state_percentage = (state_total_amount / total_amount * 100) if total_amount != 0 else None
            pop_adj_units = None
            if 'pop_adj_units' in sub.columns:
                pop_adj_units = sub['pop_adj_units'].sum()
            bimaru_rows.append({
                'state': 'BIMARU',
                'chapter': chapter,
                'year': year,
                'state_total_amount': state_total_amount,
                'total_amount': total_amount,
                'state_percentage': state_percentage,
                'pop_adj_units': pop_adj_units
            })
    for year in df['year'].unique().to_list():
        mask = (df['state'].is_in(bimaru_states)) & (df['chapter'] == 'All Chapters') & (df['year'] == year)
        sub = df.filter(mask)
        if sub.height == 0:
            continue
        state_total_amount = sub['state_total_amount'].sum()
        total_amount = sub['total_amount'].sum()
        state_percentage = (state_total_amount / total_amount * 100) if total_amount != 0 else None
        pop_adj_units = None
        if 'pop_adj_units' in sub.columns:
            pop_adj_units = sub['pop_adj_units'].sum()
        bimaru_rows.append({
            'state': 'BIMARU',
            'chapter': 'All Chapters',
            'year': year,
            'state_total_amount': state_total_amount,
            'total_amount': total_amount,
            'state_percentage': state_percentage,
            'pop_adj_units': pop_adj_units
        })
    if bimaru_rows:
        bimaru_df = pl.DataFrame(bimaru_rows)
        df = pl.concat([df, bimaru_df])
    df = df.sort(['state', 'year', 'chapter'])
    return df

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