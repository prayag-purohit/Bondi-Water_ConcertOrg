from Client_data_processing.client_data_processing import populate_client_data, get_property_metadata
from Alertlab_api.alertlab_api import get_token, get_list_timeseries
from Alertlab_api.aws_utils import upload_log_to_s3, get_logger_and_log_stream
import ast
import time
import pytz
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import altair as alt
import plotly.express as px
import streamlit_toggle as tog

logger, log_stream = get_logger_and_log_stream()
LOG_KEY = f"logs/{datetime.now().strftime('%Y-%m-%d')}/dashboard_log.txt"

def sum_columns(dataframes, column_names):
    if len(dataframes) > 1:
        # Initialize a dataframe with the structure of the first dataframe in the list
        summed_df = dataframes[0].copy()
        # Iterate through the list of dataframes, starting from the second dataframe
        for df in dataframes[1:]:
            for col in column_names:
                # Sum the specified columns
                summed_df[col] += df[col]
        return summed_df
    elif len(dataframes) == 1:
        return dataframes[0]

def get_7_day_night_average(sensor_list):
    if len(sensor_list) > 0:
        # Get today's date and convert to unix time
        today = datetime.now()
        today_unix = int(time.mktime(today.timetuple()))
        # Get 7 days ago date and convert to unix time
        seven_days_ago = today - timedelta(days=7)
        seven_days_ago_unix = int(time.mktime(seven_days_ago.timetuple()))
        # Query for all the sensors at the location
        seven_days_dataframes = get_list_timeseries(sensor_list, start_date=seven_days_ago_unix, end_date=today_unix, rate="h", series="W", token = st.session_state.token)
        # Sum the dataframes
        cumulative_seven_day_consumption = sum_columns(seven_days_dataframes, ['series'])
        # Convert the datetime strings into Datetime objects and adjust for UTC to EDT
        cumulative_seven_day_consumption['Datetime'] = pd.to_datetime(cumulative_seven_day_consumption['Datetime'])
        # Filter the data for Datetime values that are Mondays between 1AM and 5AM EDT
        filtered_data = cumulative_seven_day_consumption[(cumulative_seven_day_consumption['Datetime'].dt.hour >= 1) &
                                                         (cumulative_seven_day_consumption['Datetime'].dt.hour <= 5)]
        # Calculate the mean and median of the series column for the filtered data
        mean_series = filtered_data['series'].mean()
        median_series = filtered_data['series'].median()
        return mean_series, median_series, cumulative_seven_day_consumption
    
#Redundancy in first two functions. Have to combine later. 
def get_7_day_average(sensor_list):
    if len(sensor_list) > 0:
        # Get today's date and convert to unix time
        today = datetime.now()
        today_unix = int(time.mktime(today.timetuple()))
        # Get 7 days ago date and convert to unix time
        seven_days_ago = today - timedelta(days=7)
        seven_days_ago_unix = int(time.mktime(seven_days_ago.timetuple()))
        # Query for all the sensors at the location
        seven_day_dataframes = get_list_timeseries(sensor_list, start_date=seven_days_ago_unix, end_date=today_unix, rate="h", series="W", token = st.session_state.token)
        # Sum the dataframes
        cumulative_seven_day_consumption = sum_columns(seven_day_dataframes, ['series'])
        # Convert the datetime strings into Datetime objects and adjust for UTC to EDT
        cumulative_seven_day_consumption['Datetime'] = pd.to_datetime(cumulative_seven_day_consumption['Datetime'])
        # Calculate the mean of the series column for the past week
        mean_series = cumulative_seven_day_consumption['series'].mean()
        return mean_series, cumulative_seven_day_consumption
    
def generate_heatmap(sensor_list):
    if len(sensor_list) > 0:
        # Calculate the Unix timestamps for the start of last week (Monday) to the end of Sunday
        today = datetime.now()
        last_sunday = today - timedelta(days=today.weekday() + 1)
        start_of_last_week = last_sunday - timedelta(days=6)
        start_of_last_week_unix = int(time.mktime(start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
        end_of_last_week_unix = int(time.mktime(last_sunday.replace(hour=23, minute=59, second=59, microsecond=0).timetuple()))

        # Fetch the data for the entire last week
        seven_day_dataframes = get_list_timeseries(
            sensor_list, 
            start_date=start_of_last_week_unix, 
            end_date=end_of_last_week_unix, 
            rate="h", 
            series="W",
            token = st.session_state.token
        )
        cumulative_seven_day_consumption = sum_columns(seven_day_dataframes, ['series'])
        cumulative_seven_day_consumption['Datetime'] = pd.to_datetime(cumulative_seven_day_consumption['Datetime'], unit='ms')
        cumulative_seven_day_consumption['Day'] = cumulative_seven_day_consumption['Datetime'].dt.day_name()
        cumulative_seven_day_consumption['Hour'] = cumulative_seven_day_consumption['Datetime'].dt.hour
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        cumulative_seven_day_consumption['Day'] = pd.Categorical(cumulative_seven_day_consumption['Day'], categories=day_order, ordered=True)
        cumulative_seven_day_consumption['Litres'] = cumulative_seven_day_consumption['series'].round(2)
        start_last_display = start_of_last_week.strftime('%m/%d/%Y')
        last_sunday_display = last_sunday.strftime('%m/%d/%Y')

        # Custom colors for a more engaging look

        custom_colors = alt.Scale(
        scheme='blues', 
        domainMid=0,
        range=['#f7fbff', '#6baed6', '#08306b']
        )
        fig = alt.Chart(cumulative_seven_day_consumption).mark_rect().encode(
            x=alt.X('Hour:O', title='Hour of the Day'),
            y=alt.Y('Day:O', title='Day of the Week', sort=day_order),
            color=alt.Color('Litres:Q', scale=alt.Scale(scheme='blues'), title="L's Usage"),
            tooltip=['Day', 'Hour', 'Litres']
        ).properties(
            title=f"L's Usage Heatmap: Hourly Distribution Across Last Week ({start_last_display} - {last_sunday_display})",
            width=700,
            height=700
        ).configure_axis(
            grid=False
        ).configure_view(
            strokeWidth=0
        ).configure_mark(
            fontSize=32  # Adjust this to increase the size of hover text globally
        )
        return fig
    
def timeseries_bar_graph(dataframes):
    # Prepare an empty DataFrame to concatenate all data
    combined_df = pd.DataFrame()
    # Loop over each DataFrame and map the correct sensor names
    for idx, df in enumerate(dataframes):
        df['Source'] = f"{sensor_names[idx]}"  # Fallback if sensor names are missing
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Create a bar plot with different colors for each source file
    fig = px.bar(combined_df, x='Datetime', y='series', color='Source', title="Total Litres Over Time", height=600)

    # Customize hover template to include the total value for each datetime
    fig.update_traces(hovertemplate='<b>Date:</b> %{x}<br>' +
                                    '<b>Source:</b> %{customdata[0]}<br>' +
                                    '<b>Value:</b> %{y}<br>' +
                                    '<b>Total:</b> %{customdata[1]}<extra></extra>')

    # Add custom data for hover: Source and Total
    totals_df = combined_df.groupby('Datetime')['series'].sum().round(3).reset_index()
    combined_df = combined_df.merge(totals_df, on='Datetime', suffixes=('', '_Total'))
    fig.update_traces(customdata=combined_df[['Source', 'series_Total']].values)

    return fig

def make_timeseries_chart(queried_sensors, start_date, end_date, rate, series):
    if len(queried_sensors) != 0:
        time_series_data = get_list_timeseries(queried_sensors, start_date=start_date_unix, end_date=end_date_unix, rate=rate, series=series, token = st.session_state.token)
        #timeseries_bar_graph(time_series_data)
        # Sum the displayed dataframes
        cumulative_timeseries_data = sum_columns(time_series_data, ['series'])
        # Casting data type for time as string
        #cumulative_timeseries_data["series"] = cumulative_timeseries_data["Datetime"].astype(str)
        cumulative_timeseries_data['series'] = cumulative_timeseries_data['series'].fillna(0).astype(float)
        cumulative_timeseries_data['series'] = cumulative_timeseries_data['series'].apply(lambda x: round(x))
        cumulative_timeseries_data['change'] = cumulative_timeseries_data['series'].pct_change().mul(100).round(2)
        # Create an outier free column
        cumulative_timeseries_data['normalized'] = cumulative_timeseries_data['series']
        Q1 = cumulative_timeseries_data['normalized'].quantile(0.25)
        Q3 = cumulative_timeseries_data['normalized'].quantile(0.75)
        IQR = Q3 - Q1
        # Define the bounds for outliers
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        median_value = cumulative_timeseries_data['normalized'].median()
        # Replace outliers with the median
        cumulative_timeseries_data['normalized'] = cumulative_timeseries_data['normalized'].apply(
            lambda x: median_value if x < lower_bound or x > upper_bound else x
        )
        # Generate the chart
        fig = timeseries_bar_graph(time_series_data)
        fig2 = px.scatter(cumulative_timeseries_data, x="Datetime", y="normalized", height=700, trendline="ols", trendline_scope="overall", trendline_color_override="#d52b1e")
        fig2.update_layout(showlegend=False)   
        fig3 = generate_heatmap(queried_sensors)     
        st.plotly_chart(fig, theme="streamlit")
        st.plotly_chart(fig2, theme="streamlit")
        st.altair_chart(fig3, theme="streamlit", use_container_width=True)
        st.write(cumulative_timeseries_data)
        
# Settings
st.set_page_config(
    page_title="Bondi Water Corp",
    page_icon=":potable_water:",
    layout="wide",
    initial_sidebar_state="expanded")
alt.themes.enable("dark")

if 'token' not in st.session_state:
    token = get_token()
    st.session_state.token = token

if 'df' not in st.session_state:
    st.session_state.df = populate_client_data()
    logger.info("Dataframe populated successfully")
df = st.session_state.df

with st.sidebar:
    # Dashboard title
    st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        color: #122B46; 
    }
    </style>
    """, unsafe_allow_html=True)
    with open("app_title.txt", "r") as file:
        title = file.read().strip()
    
    st.markdown(f'<p class="big-font">{title}</p>', unsafe_allow_html=True)
    # Parent Organization filter
    parent_list = sorted(list(df.name_parent.unique())[::-1])
    selected_address = st.selectbox('Select Organization:', parent_list)
    logger.info(f"Selected_parent: {selected_address}")
    df_selected_parent = df[df.name_parent == selected_address]
    # Address Filter Dropdown
    address_list = list(df_selected_parent.name_child.unique())[::-1]
    selected_address = st.selectbox('Select Property:', address_list)
    logger.info(f"Selected child: {selected_address}")
    df_selected_address = df[df.name_child == selected_address]
    # Calendar widget 
    default_date_last_week = datetime.today() - timedelta(days=7)
    start_date = st.date_input("Start Date", default_date_last_week)
    end_date = st.date_input("End Date")
    
    start_date_unix = str(datetime.strptime(str(start_date), "%Y-%m-%d").timestamp())
    end_date_unix = str(datetime.strptime(str(end_date), "%Y-%m-%d").timestamp())

    # Rate selection
    rate_switch = tog.st_toggle_switch(label="Hour / Minute")
    rate = "h"
    if rate_switch == True:
        rate="m"
    # Series selection
    series_switch = tog.st_toggle_switch(label="Water / Temperature")
    series = "W"
    if series_switch == True:
        series="T_0"
    # List of sensors as buttons
    buttons = []
    sensor_list = df_selected_address["sensor_ids"].iloc[0]
    sensor_names = df_selected_address["sensor_names"].iloc[0]
    for i in sensor_list:
        buttons.append(tog.st_toggle_switch(label=sensor_names[sensor_list.index(i)],
                                            key=i))
    for button in buttons:
        if button:
            pass
    # Time series data list
    queried_sensors = []
    for sensor_id, button in zip(sensor_list, buttons):
        if button == True:
            queried_sensors.append(sensor_id)        
    # Get the selected suite numbers ##This workflow is switched to decrease boot time
    #amount_of_suites = df_selected_address["numberSuites"].iloc[0]
    #if not isinstance(amount_of_suites, (int, float)):
        #amount_of_suites = 1
    # Initiate Query and get list of dataframes from selected sensors
    submitted = st.button("Query")
    logger.info(f"BROWSING: Queried_sensors: {queried_sensors}, rate: {rate}, series: {series}, start_date: {start_date}, end_date: {end_date}")
    

if submitted == True:
    logger.info(f"QUERIED: Queried_sensors: {queried_sensors}, rate: {rate}, series: {series}, start_date: {start_date}, end_date: {end_date}")

    # Get the metadata for the selected address
    amount_of_suites, number_of_floors, CommercialPropertyType, property_age, number_of_users = get_property_metadata(df_selected_address["_id_child"].iloc[0])
    
    mean, median, cumulative_seven_night_consumption = get_7_day_night_average(sensor_list)
    seven_day_mean, cumulative_seven_day_consumption  = get_7_day_average(sensor_list)
    st.session_state.mean = mean
    st.session_state.median = median
    st.session_state.seven_day_mean = seven_day_mean
    st.session_state.suite_mean = (st.session_state.mean/amount_of_suites)
    st.markdown(
        """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 70px;
    }
    div[data-testid="stMarkdownContainer"] > p {
        font-size: 25px;
    }


    </style>
    """,
        unsafe_allow_html=True,
    )

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric(
        label="7 Day 12AM-5AM (Avg)",
        value=round(st.session_state.mean)
    )
    kpi2.metric(
        label="Ratio (Metric1, Metric3)",
        value=round(st.session_state.mean/st.session_state.seven_day_mean, 2)
    )
    kpi3.metric(
        label="Trailing 7 Day Average",
        value = round(st.session_state.seven_day_mean)
    )
    kpi4.metric(
        label="Per Suite Average (l/h/u)",
        value = round(st.session_state.suite_mean)
    )
    # Function to make timeseries chart  
    make_timeseries_chart(queried_sensors, start_date_unix, end_date_unix, rate, series)
    logger.info("Session ran successfully")
    # Upload logs to S3
    upload_log_to_s3(logger, log_stream)
    
st.write("How KPI 1 is calculated: This is the 1-5 AM (inclusive) average for the past 7 days")
st.write("How KPI 2 is calculated: This is the KPI1 divided by KPI3 and rounded to 2 decimals")
st.write("How KPI 3 is calculated: This is the mean of the water measures (7 days * 24 hours)")
st.write("How KPI 4 is calculated: This is KPI 1 divided by the number of suites")