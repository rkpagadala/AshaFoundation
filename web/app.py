import typing
import streamlit as st
import yaml
import pandas as pd

from streamlit_echarts import st_echarts

with open('web/config/config.yaml') as file:
    config = yaml.safe_load(file)
    
st.set_page_config(page_title=config['app']['title'], page_icon=":bar_chart:", layout="wide")
    
def main():
    st.title(config['app']['title'])
    st.markdown(config['app']['subtitle'])
    st.markdown("---")
    cols         = st.columns(2)
    state_df     = pd.read_csv("DataCSV/percentage_year_state.csv")
    
    state_df     = add_missing_years(state_df)
    state_df     = add_all_years(state_df)
    
    chapters_df  = pd.read_csv("DataCSV/per_population_state_chapter_year.csv")
    
    chapters     = list(chapters_df['chapter'].unique())

    chapter :str = st.selectbox("Select Chapter", chapters, index=0)
    
    metrics      = get_metrics(chapters_df, chapter)
    
    cols         = st.columns(3)
    for i,metric in enumerate(metrics):
        cols[i%3].metric(
            label=metric['label'],
            value=metric['value'],
            delta_color="inverse"
        )
    
    all_states = get_funded_states(chapters_df, chapter)
    
    with st.expander("States that received grant", expanded=False):
        st.write(", " .join(list(map(lambda x: x.title(), all_states))))
        pass
    with st.expander("Funding year wise", expanded=False):
        
        plot_year_wise_bar(chapters_df, chapter)
        pass
    chapter_title = chapter if chapter != 'All Chapters' else ''
    colored_chapter_title = f"<span style='color:green;'>{chapter_title}</span>"

    st.markdown(f"### Year wise {colored_chapter_title} grant distribution by State", unsafe_allow_html=True)
    # st.subheader(f"Year wise {chapter.title() if chapter != 'All Chapters' else ''} grant distribution by State")
    all_states = get_funded_states(chapters_df, chapter)
    
    states = st.multiselect("Select States", all_states, default=["BIMARU"] if "BIMARU" in all_states else [])

    plot_state_year_wise_funds_breakdown(states, chapter)

    # show table
    st.markdown(f"### State wise {colored_chapter_title} grant distribution by Year", unsafe_allow_html=True)

    end_year             = pd.Timestamp.now().year - 1
    start_year, end_year = st.slider("Select Year Range", min_value=1991, max_value=end_year, value=(1991, end_year), step=1)
    end_year             = end_year+1


    plot_state_wise_funds_breakdown(start_year, end_year, chapter)
    
    with st.expander("", expanded=True):
        st.markdown("The funds adjusted for the population of the state are calculated as follows:")
                    
        st.latex(r"""
        \text{Value} = \frac{\text{\% of Funds Received by the State in the Year}}{\text{\% Population of the State}}
        """)

        st.markdown("---")

        st.markdown("""
        - If the value exceeds 1, the state receives more funding than its population size would suggest.
- If the value is below 1, the state receives less funding than its population proportion indicates.
        """)
        
    with st.expander("", expanded=True):
        st.markdown("**BIMARU** refers to a combination of less developed states in India, specifically Bihar,Jharkhand, Madhya Pradesh, Chhattisgarh, Rajasthan, Uttar Pradesh and Uttarakhand.")

def add_all_years(df):
    
    all_year_rows = []
    for state in df['state'].unique():
        total_amount_state = df[df['state'] == state]['state_total_amount'].sum()
        total_amount       = df['state_total_amount'].sum()
        all_year_rows.append({
            'state'             :  state,
            'year'              :  'All Years',
            'state_total_amount':  total_amount_state,
            'total_amount'      :  total_amount,
            'percentage'        : (total_amount_state/total_amount)*100
        })
    new_df = pd.DataFrame(all_year_rows)
    return pd.concat([df, new_df])
   
def add_missing_years(df):
    
    all_state_names = get_all_states()
    all_state_names = [s.upper() for s in all_state_names]
    for y in df['year'].unique():
        missing_states = []
        present_states = df[df['year'] == y]['state'].unique()
        total_amount   = df[df['year'] == y]['state_total_amount'].sum()
        for state in all_state_names:
            if state not in present_states:
                missing_states.append({
                    'state': state, 
                    'year': y, 
                    'state_total_amount': 0, 
                    'total_amount': total_amount,
                    'percentage': 0
                })
        # print(missing_states)
        # If there are missing states, add them to the DataFrame
        if missing_states:
            df = pd.concat([df, pd.DataFrame(missing_states)], ignore_index=True) 
            
    return df   

def plot_state_year_wise_funds_breakdown(states, chapter):
    
    if len(states) == 0:
        st.markdown("Please select at least one state")
        st.markdown("---")
        return

    state_wise_data = []
    min_year = 2024
    max_year = 1990
    for state in states:
        df = pd.read_csv("DataCSV/per_population_state_chapter_year.csv")
        df = df[df['chapter'] == chapter]
        df = df[df['state'] == state]
        data = []

        for i,row in df.iterrows():
            data.append((row['year'],row['pop_adj_units']))
            min_year = min(min_year, row['year'])
            max_year = max(max_year, row['year'])
        state_wise_data.append((state,data))
    
    for state,data in state_wise_data:
        years_present = set([i[0] for i in data])
        for y in range(min_year, max_year+1):
            if y not in years_present:
                data.append((y, 0))

    
    formatted_state_wise_data = []
    for state,data in state_wise_data:
        data = sorted(data, key=lambda x: x[0])
        formatted_state_wise_data.append({
            'name': state,
            'data': [
                    {"value": i[1]}
                for i in data
            ],
            'type': 'bar'
        })

    option = {
        "tooltip": { "trigger": "axis", "axisPointer": { "type": "shadow" } },
        "legend": {
            "data": states
        },
        "xAxis": {
            "type": "category",
            "data": [y for y in range(min_year, max_year+1)],
            "name": "Year",
            "nameTextStyle": {
                "fontSize": 16,      
                "color": '#000000',  
                "fontWeight": 'bold', 
                "padding": [5, 0, 0, 0], 
            },
        },
        "yAxis": { 
            "type": "value",
            "name": "\t\t\t\t\t\t\t\t\tFunds adjusted for population of state",
            "min":0,
            "max": 5 if state != "MANIPUR" else 25,
            "nameTextStyle": {
                "fontSize": 16,      
                "color": '#000000',  
                "fontWeight": 'bold', 
                "padding": [5, 0, 0, 0], 
            },
        },
        "series": formatted_state_wise_data
    }
    

    st_echarts(
        options=option, height="600px", width="100%"
    )
    
    st.markdown("---")
    
def plot_state_wise_funds_breakdown(start_year, end_year, chapter):
    df = pd.read_csv("DataCSV/per_population_state_chapter_year.csv")
    
    df = df[df['chapter'] == chapter]
    df = df[df['year'].between(start_year, end_year)]
    data = []
    
    # group by state
    df = df.groupby('state').sum().reset_index()
    df['pop_adj_units'] = df['pop_adj_units']/(end_year-start_year)
    
    for i,row in df.iterrows():
        data.append((row['state'],row['pop_adj_units']))
    
    # concat population percent
    updated_data = add_population(data.copy())
    updated_data.sort(key=lambda x: x[1], reverse=True)
    option = {
        "tooltip": { "trigger": "axis", "axisPointer": { "type": "shadow" } },
        "grid": {
            "bottom": 150,  # Increase the bottom margin to give more space for x-axis labels
            # "left":50
        },
        "xAxis": {
            "type": "category",
            "data": [str(i[0]) for i in updated_data],
            "name": "State - Pop %",
            "nameTextStyle": {
                "fontSize": 16,      
                "color": '#000000',  
                "fontWeight": 'bold', 
                "padding": [5, 0, 0, 0], 
            },
            "axisLabel": {
                "interval": 0,  # This forces the display of all labels
                "rotate": 60    # Rotate the labels if they're overlapping
            }
        },
        
        "yAxis": { 
            "type": "value",
            "name": "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tFunds adjusted for population of state",
            "nameGap": 10,
            "nameLocation": 'end',
            "nameTextStyle": {
                "fontSize": 16,      
                "color": '#000000',  
                "fontWeight": 'bold', 
                "padding": [0, 50, 0, 0],
            },
            "axisLabel": {
                "formatter": '{value}',  
                "margin": 0 
            }
        },
        "series": [
            {
                "data": [
                    {"value": i[1], "itemStyle": {"color": get_bar_color(i[1])}}
                for i in updated_data], 
                "type": "bar", 

            }
        ],
    }
    

    st_echarts(
        options=option, height="700px", width="100%"
    )
    if start_year == end_year-1:
        st.markdown(f"List of states, that received no funds in the year **{start_year}**")
    else:
        st.markdown(f"List of states, that received no funds in the years **{start_year}** to **{end_year-1}**")
    all_states            = get_all_states()
    all_states            = [i.upper() for i in all_states]
    states                = []
    funds_received_states = [i[0] for i in data]

    for state in all_states:
        if state not in funds_received_states:
            states.append({"state" :state})
    population_percent_df = pd.read_csv("DataCSV/population.csv")
    new_df                = pd.merge(pd.DataFrame(states), population_percent_df, on='state')
    new_df                = new_df[['state', '% of Total','Population']]
    new_df                = new_df.rename(columns={'% of Total': 'Percentage of population', 'state': 'State'})
    # sort by percentage of population
    new_df                = new_df.sort_values(by='Percentage of population', ascending=False)
    st.markdown(f"Percentage of population that received no funds : **{round(sum(new_df['Percentage of population']),2)}%**")
    st.dataframe(new_df, hide_index=True)
    st.markdown("---")

def get_bar_color(value):
    if value < 0.5:
        return 'red'
    if value < 1:
        return 'orange'
    return 'green'

def add_population(data):
    population_df = pd.read_csv("DataCSV/population.csv")
    for i in range(len(data)):
        state   = data[i][0]
        percent = data[i][1]
        population_df['state'] = population_df['state'].str.upper()
        if state == 'BIMARU':
            bimaru_states = ['BIHAR', 'JHARKHAND', 'MADHYA PRADESH', 'CHHATTISGARH', 'RAJASTHAN', 'UTTAR PRADESH', 'UTTARAKHAND']
            population = population_df[population_df['state'].isin(bimaru_states)]['% of Total'].sum()
        else:
            population = population_df[population_df['state'] == state]['% of Total'].values[0]
        data[i]        = (f"{state} - {population}", percent)
    return data

def get_transformed_year_state_data():

    
    df              = pd.read_csv("DataCSV/per_population_state_year.csv")
    df              = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df['state']     = df['state'].apply(lambda x: " ".join([s.capitalize() for s in x.split()]))
    df              = df.sort_values(by='year')
    all_state_names = get_all_states()

    # Check for states not in the DataFrame and prepare data for them
    for y in df['year'].unique():
        missing_states = []
        present_states = df[df['year'] == y]['state'].unique()
        # total_amount = df[df['year'] == y]['state_total_amount'].sum()
        for state in all_state_names:
            if state not in present_states:
                # state,year,state_total_amount,total_amount,state_percentage,pop_adj_units
                missing_states.append({
                    'state': state, 
                    'year': y, 
                    'state_total_amount': 0, 
                    'total_amount': 0,
                    'state_percentage': 0,
                    'pop_adj_units': 0,

                })

        # If there are missing states, add them to the DataFrame
        if missing_states:
            df = pd.concat([df, pd.DataFrame(missing_states)], ignore_index=True)
    return  df


def plot_KPI(KPI_summary):
    cols = st.columns(3)
    
    for i,[key, value] in enumerate(KPI_summary.items()):
        cols[i%3].metric(
            label=value['title'],
            value=value['formatted_value'],
     
        )

def get_all_states():
    df = pd.read_csv("DataCSV/population.csv")
    
    states = list(df['state'].values )
    states.append('BIMARU')
    
    return sorted(states)

def get_funded_states(df, chapter):
    df = df[df['chapter'] == chapter]
    states : typing.List[str] = list(set(df['state'].values))
    return sorted(states)

def get_metrics(df: pd.DataFrame, chapter):
    
    df = df[df['chapter'] == chapter]
    df = df[df['state']!='BIMARU']
    
    lifetime_df = df.drop_duplicates(['year'])
    lifetime_df = lifetime_df.drop(['state','chapter'], axis=1)
    lifetime_donation = lifetime_df['total_amount'].sum()
    state_grouped = df.groupby(['state']).sum().reset_index()
    state_grouped = state_grouped.sort_values(by='state_total_amount', ascending=False)
    most_funded_state = state_grouped.iloc[0]['state']
    years_grouped = df.groupby(['year']).sum().reset_index()
    years_grouped = years_grouped.sort_values(by='state_total_amount', ascending=False)
    most_funded_year = years_grouped.iloc[0]['year']
    
 
    return [
        {
            "label" : "Lifetime Grant",
            "value": f"${lifetime_donation:,.2f}",
        },
        {
            "label" : "Most Grant Received State",
            "value": most_funded_state.title(),
        },
        {
            "label" : "Most Grant Received Year",
            "value": most_funded_year,
        }
    ]


def plot_year_wise_bar(df: pd.DataFrame, chapter):
    df = df[df['chapter'] == chapter]
    df = df[df['state']!='BIMARU']
    df = df.drop_duplicates(subset=['year'])
    df = df.sort_values(by='year')
    # drop duplicate years
    st.bar_chart(df, x='year', y='total_amount', height=500, use_container_width=True)

if __name__ == "__main__":
    main()
