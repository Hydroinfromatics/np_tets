from flask import Flask
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
import logging
from data_processing import filter_data_hourly, preprocess_data, API_URL
from running.get_data import fetch_data_from_api

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask server
server = Flask(__name__)

# Initialize Dash app
app = dash.Dash(__name__, server=server, url_base_pathname='/dashboard/')
app.title = "Real-Time Water Monitoring Dashboard"

# Initialize global variables
df = pd.DataFrame()
last_update_time = None
data_fetch_error = None

def verify_data_quality(new_df):
    """Verify the quality of incoming data"""
    if new_df is None or new_df.empty:
        return False, "No data received"
    
    required_columns = ['timestamp', 'FlowInd', 'TDS', 'pH', 'Depth']
    if not all(col in new_df.columns for col in required_columns):
        return False, f"Missing columns. Required: {required_columns}, Got: {list(new_df.columns)}"
    
    # Check for null values
    null_counts = new_df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Found null values: {null_counts[null_counts > 0]}")
    
    return True, "Data verified"

def fetch_latest_data():
    """Fetch and validate the latest data"""
    try:
        logger.info(f"Attempting to fetch data from {API_URL}")
        raw_data = fetch_data_from_api(API_URL)
        
        if raw_data:
            logger.info("Raw data received successfully")
            # Get hourly aggregated data
            new_df = filter_data_hourly()
            
            # Verify data quality
            is_valid, message = verify_data_quality(new_df)
            if is_valid:
                logger.info(f"Data verification successful: {message}")
                return new_df
            else:
                logger.error(f"Data verification failed: {message}")
                return None
        else:
            logger.error("No raw data received from API")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return None

def update_data():
    """Updates the global DataFrame with new data every 10 minutes"""
    global df, last_update_time, data_fetch_error
    
    while True:
        try:
            logger.info("Starting data update cycle")
            new_df = fetch_latest_data()
            
            if new_df is not None and not new_df.empty:
                # Compare with existing data to verify it's actually new
                if df.empty or new_df['timestamp'].max() > df['timestamp'].max():
                    df = new_df.copy()
                    last_update_time = datetime.now()
                    data_fetch_error = None
                    logger.info(f"Data updated successfully at {last_update_time}")
                    logger.info(f"Latest data timestamp: {df['timestamp'].max()}")
                    logger.info(f"Number of records: {len(df)}")
                else:
                    logger.warning("No new data received from sensor")
            else:
                data_fetch_error = "Failed to fetch new data"
                logger.error(data_fetch_error)
                
        except Exception as e:
            data_fetch_error = str(e)
            logger.error(f"Error in update cycle: {data_fetch_error}")
            
        # Wait for next update cycle (10 minutes)
        logger.info("Waiting for next update cycle...")
        time.sleep(600)  # 600 seconds = 10 minutes

# Start data update thread
data_thread = threading.Thread(target=update_data)
data_thread.daemon = True
data_thread.start()

# Layout of Dash app
app.layout = html.Div([
    html.H1("Real-Time Water Monitoring Dashboard", style={'textAlign': 'center'}),
    
    # Status section
    html.Div([
        html.Div(id='last-update-time', style={'textAlign': 'center', 'margin': '10px'}),
        html.Div(id='data-status', style={'textAlign': 'center', 'margin': '10px'}),
        html.Div(id='data-summary', style={'textAlign': 'center', 'margin': '10px'})
    ]),
    
    # Time range selector
    dcc.Dropdown(
        id='time-range-selector',
        options=[
            {'label': 'Last 1 Hour', 'value': 'hour'},
            {'label': 'Last 24 Hours', 'value': 'day'},
            {'label': 'Last Week', 'value': 'week'},
            {'label': 'Last Month', 'value': 'month'}
        ],
        value='day',
        style={'width': '200px', 'margin': '10px'}
    ),
    
    dcc.Graph(id='ph-tds-graph'),
    dcc.Graph(id='flow-depth-graph'),
    
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # Update every 30 seconds
        n_intervals=0
    )
])

@app.callback(
    [Output('last-update-time', 'children'),
     Output('data-status', 'children'),
     Output('data-summary', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_status(n):
    if df.empty:
        return (
            "No data available",
            html.Div("Waiting for initial data...", style={'color': 'orange'}),
            ""
        )
    
    status_color = 'red' if data_fetch_error else 'green'
    status_message = data_fetch_error if data_fetch_error else "Data feed active"
    
    # Calculate data summary
    latest_time = df['timestamp'].max()
    earliest_time = df['timestamp'].min()
    record_count = len(df)
    
    summary = f"Total Records: {record_count} | Data Range: {earliest_time.strftime('%Y-%m-%d %H:%M')} to {latest_time.strftime('%Y-%m-%d %H:%M')}"
    
    return (
        f"Last Updated: {last_update_time.strftime('%Y-%m-%d %H:%M:%S') if last_update_time else 'Never'}",
        html.Div(f"Status: {status_message}", style={'color': status_color}),
        summary
    )

@app.callback(
    [Output('ph-tds-graph', 'figure'),
     Output('flow-depth-graph', 'figure')],
    [Input('interval-component', 'n_intervals'),
     Input('time-range-selector', 'value')]
)
def update_graphs(n, time_range):
    if df.empty:
        return ({
            'data': [],
            'layout': {'title': 'Waiting for data...'}
        },) * 2
    
    # Calculate time range
    now = datetime.now()
    if time_range == 'hour':
        start_time = now - timedelta(hours=1)
    elif time_range == 'day':
        start_time = now - timedelta(days=1)
    elif time_range == 'week':
        start_time = now - timedelta(weeks=1)
    else:  # month
        start_time = now - timedelta(days=30)
    
    # Filter data based on time range
    mask = (df['timestamp'] >= start_time)
    filtered_df = df[mask].copy()
    
    if filtered_df.empty:
        logger.warning(f"No data available for selected time range: {time_range}")
        return ({
            'data': [],
            'layout': {'title': 'No data available for selected time range'}
        },) * 2
    
    # Create pH and TDS graph
    ph_tds_fig = go.Figure()
    
    ph_tds_fig.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['pH'],
        mode='lines+markers',
        name='pH',
        line=dict(color='green')
    ))
    
    ph_tds_fig.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['TDS'],
        mode='lines+markers',
        name='TDS (ppm)',
        line=dict(color='blue'),
        yaxis='y2'
    ))
    
    ph_tds_fig.update_layout(
        title=f'pH and TDS Over Time (Last {time_range})',
        xaxis_title='Time',
        yaxis_title='pH',
        yaxis2=dict(
            title='TDS (ppm)',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified'
    )
    
    # Create Flow and Depth graph
    flow_depth_fig = go.Figure()
    
    flow_depth_fig.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['FlowInd'],
        mode='lines+markers',
        name='Flow Indicator',
        line=dict(color='purple')
    ))
    
    flow_depth_fig.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['Depth'],
        mode='lines+markers',
        name='Depth (ft)',
        line=dict(color='orange'),
        yaxis='y2'
    ))
    
    flow_depth_fig.update_layout(
        title=f'Flow and Depth Over Time (Last {time_range})',
        xaxis_title='Time',
        yaxis_title='Flow Indicator',
        yaxis2=dict(
            title='Depth (ft)',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified'
    )
    
    return ph_tds_fig, flow_depth_fig

# Flask route to serve the homepage
@server.route('/')
def index():
    return '''
    <h1>Welcome to the Real-Time Water Monitoring Dashboard</h1>
    <p>Go to <a href="/dashboard/">Dashboard</a> to view the data.</p>
    '''

if __name__ == "__main__":
    logger.info("Starting dashboard application")
    app.run_server(debug=True, host='127.0.0.1', port=8050)