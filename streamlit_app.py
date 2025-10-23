import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import altair as alt

st.set_page_config(
    layout="wide",
    page_icon=":material/monitoring:",
    page_title="Cortex Cost Monitor"
)

def is_running_in_sis():
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except:
        return False

def get_sis_connection():
    from snowflake.snowpark.context import get_active_session
    return get_active_session()

def get_local_connection():
    from snowflake.snowpark import Session
    if 'session' not in st.session_state or st.session_state.session is None:
        st.session_state.session = Session.builder.config(
            'connection_name', 
            os.getenv("SNOWFLAKE_CONNECTION_NAME") or "demo"
        ).create()
    return st.session_state.session

def get_session():
    if is_running_in_sis():
        return get_sis_connection()
    else:
        return get_local_connection()

def test_connection(_session):
    try:
        result = _session.sql("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()").collect()
        return True, result[0].as_dict()
    except Exception as e:
        return False, str(e)

@st.cache_data(ttl=300)
def load_cortex_analyst_usage(_session, start_date, end_date):
    query = f"""
    SELECT 
        START_TIME,
        END_TIME,
        USERNAME,
        CREDITS,
        REQUEST_COUNT,
        'Cortex Analyst' as SERVICE_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
    WHERE START_TIME >= '{start_date}' AND END_TIME <= '{end_date}'
    ORDER BY START_TIME DESC
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_cortex_functions_usage(_session, start_date, end_date):
    query = f"""
    SELECT 
        f.START_TIME,
        f.END_TIME,
        f.FUNCTION_NAME,
        CASE 
            WHEN f.MODEL_NAME IS NULL OR TRIM(f.MODEL_NAME) = '' THEN 'internal-models'
            ELSE f.MODEL_NAME 
        END as MODEL_NAME,
        f.WAREHOUSE_ID,
        w.WAREHOUSE_NAME,
        f.TOKEN_CREDITS as CREDITS,
        f.TOKENS,
        'Cortex Functions' as SERVICE_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY f
    LEFT JOIN (
        SELECT DISTINCT WAREHOUSE_ID, WAREHOUSE_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    ) w ON f.WAREHOUSE_ID = w.WAREHOUSE_ID
    WHERE f.START_TIME >= '{start_date}' AND f.END_TIME <= '{end_date}'
    ORDER BY f.START_TIME DESC
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_cortex_search_usage(_session, start_date, end_date):
    query = f"""
    SELECT 
        START_TIME,
        END_TIME,
        DATABASE_NAME,
        SCHEMA_NAME,
        SERVICE_NAME,
        SERVICE_ID,
        CREDITS,
        'Cortex Search' as SERVICE_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
    WHERE START_TIME >= '{start_date}' AND END_TIME <= '{end_date}'
    ORDER BY START_TIME DESC
    """
    return _session.sql(query).to_pandas()

@st.cache_data(ttl=300)
def load_cortex_functions_query_usage(_session, start_date, end_date):
    query = f"""
    SELECT 
        q.QUERY_ID,
        q.WAREHOUSE_ID,
        w.WAREHOUSE_NAME,
        CASE 
            WHEN q.MODEL_NAME IS NULL OR TRIM(q.MODEL_NAME) = '' THEN 'internal-models'
            ELSE q.MODEL_NAME 
        END as MODEL_NAME,
        q.FUNCTION_NAME,
        q.TOKENS,
        q.TOKEN_CREDITS as CREDITS,
        qh.START_TIME,
        qh.END_TIME,
        'Cortex Functions Query' as SERVICE_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY q
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh 
        ON q.QUERY_ID = qh.QUERY_ID
    LEFT JOIN (
        SELECT DISTINCT WAREHOUSE_ID, WAREHOUSE_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    ) w ON q.WAREHOUSE_ID = w.WAREHOUSE_ID
    WHERE qh.START_TIME >= '{start_date}' AND qh.END_TIME <= '{end_date}'
    ORDER BY qh.START_TIME DESC
    """
    return _session.sql(query).to_pandas()

st.title(":material/monitoring: Cortex Cost Monitor")

session = get_session()

is_connected, conn_info = test_connection(session)

if not is_connected:
    st.error(f"Failed to connect to Snowflake: {conn_info}")
    st.stop()

with st.sidebar:
    st.header("Filters")
    
    date_preset = st.selectbox(
        "Date Range Preset",
        ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Custom"],
        index=1
    )
    
    if date_preset == "Last 7 Days":
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
    elif date_preset == "Last 30 Days":
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
    elif date_preset == "Last 90 Days":
        start_date = datetime.now() - timedelta(days=90)
        end_date = datetime.now()
    else:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", datetime.now())
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())
    
    st.divider()
    
    service_types = st.multiselect(
        "Service Types",
        ["Cortex Analyst", "Cortex Functions", "Cortex Search"],
        default=["Cortex Analyst", "Cortex Functions", "Cortex Search"]
    )

with st.spinner("Loading data..."):
    df_analyst = pd.DataFrame()
    df_functions = pd.DataFrame()
    df_search = pd.DataFrame()
    df_query_functions = pd.DataFrame()
    
    if "Cortex Analyst" in service_types:
        df_analyst = load_cortex_analyst_usage(session, start_date, end_date)
    
    if "Cortex Functions" in service_types:
        df_functions = load_cortex_functions_usage(session, start_date, end_date)
        df_query_functions = load_cortex_functions_query_usage(session, start_date, end_date)
    
    if "Cortex Search" in service_types:
        df_search = load_cortex_search_usage(session, start_date, end_date)

all_data = []
if not df_analyst.empty:
    all_data.append(df_analyst[['START_TIME', 'CREDITS', 'SERVICE_TYPE']])
if not df_functions.empty:
    all_data.append(df_functions[['START_TIME', 'CREDITS', 'SERVICE_TYPE']])
if not df_search.empty:
    all_data.append(df_search[['START_TIME', 'CREDITS', 'SERVICE_TYPE']])

if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    
    with st.sidebar:
        if not df_functions.empty:
            available_warehouses = df_functions['WAREHOUSE_NAME'].dropna().unique().tolist()
            if available_warehouses:
                selected_warehouses = st.multiselect(
                    "Warehouses",
                    available_warehouses,
                    default=available_warehouses
                )
            else:
                selected_warehouses = []
            
            available_models = df_functions['MODEL_NAME'].unique().tolist()
            if available_models:
                selected_models = st.multiselect(
                    "Models",
                    available_models,
                    default=available_models
                )
            else:
                selected_models = []
            
            available_functions = df_functions['FUNCTION_NAME'].dropna().unique().tolist()
            if available_functions:
                selected_functions = st.multiselect(
                    "Functions",
                    available_functions,
                    default=available_functions
                )
            else:
                selected_functions = []
    
    if not df_functions.empty:
        if selected_warehouses:
            df_functions = df_functions[df_functions['WAREHOUSE_NAME'].isin(selected_warehouses)]
        if selected_models:
            df_functions = df_functions[df_functions['MODEL_NAME'].isin(selected_models)]
        if selected_functions:
            df_functions = df_functions[df_functions['FUNCTION_NAME'].isin(selected_functions)]
    
    total_credits = combined_df['CREDITS'].sum()
    total_requests = 0
    if not df_analyst.empty:
        total_requests += df_analyst['REQUEST_COUNT'].sum()
    if not df_query_functions.empty:
        total_requests += len(df_query_functions)
    
    avg_cost_per_request = total_credits / total_requests if total_requests > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💳 Total Credits",
            f"{total_credits:.4f}",
            delta=None
        )
    
    with col2:
        st.metric(
            "📊 Total Requests",
            f"{int(total_requests):,}",
            delta=None
        )
    
    with col3:
        st.metric(
            "💰 Avg Cost/Request",
            f"{avg_cost_per_request:.6f}",
            delta=None
        )
    
    with col4:
        date_range_days = (end_date - start_date).days
        st.metric(
            "📅 Date Range",
            f"{date_range_days} days",
            delta=None
        )
    
    if not df_functions.empty and 'TOKENS' in df_functions.columns:
        total_tokens = df_functions['TOKENS'].sum()
        avg_credits_per_1m_tokens = (total_credits / total_tokens * 1_000_000) if total_tokens > 0 else 0
        
        model_efficiency = df_functions.groupby('MODEL_NAME').agg({
            'TOKENS': 'sum',
            'CREDITS': 'sum'
        }).reset_index()
        model_efficiency['CREDITS_PER_1M_TOKENS'] = (model_efficiency['CREDITS'] / model_efficiency['TOKENS']) * 1_000_000
        model_efficiency = model_efficiency.sort_values('CREDITS_PER_1M_TOKENS')
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "🎫 Total Tokens",
                f"{int(total_tokens):,}",
                delta=None
            )
        
        with col2:
            st.metric(
                "⚡ Avg Credits/1M Tokens",
                f"{avg_credits_per_1m_tokens:.2f}",
                delta=None
            )
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["Consumption over Time", "Consumption Breakdown", "Token Economy"])
    
    with tab1:
        st.subheader(":material/show_chart: Credits Over Time")
        
        time_series_data = combined_df.copy()
        time_series_data['DATE'] = pd.to_datetime(time_series_data['START_TIME']).dt.date
        daily_credits = time_series_data.groupby(['DATE', 'SERVICE_TYPE'])['CREDITS'].sum().reset_index()
        
        chart = alt.Chart(daily_credits).mark_area().encode(
            x=alt.X('DATE:T', title='Date'),
            y=alt.Y('CREDITS:Q', title='Credits', stack='zero'),
            color=alt.Color('SERVICE_TYPE:N', title='Service Type'),
            tooltip=[
                alt.Tooltip('DATE:T', format='%Y-%m-%d'),
                'SERVICE_TYPE:N',
                alt.Tooltip('CREDITS:Q', format='.6f')
            ]
        ).properties(height=400)
        st.altair_chart(chart, use_container_width=True)
        
        if not df_functions.empty and 'WAREHOUSE_NAME' in df_functions.columns:
            st.subheader(":material/warehouse: Credits Over Time Per Warehouse")
            time_series_warehouse = df_functions.copy()
            time_series_warehouse['DATE'] = pd.to_datetime(time_series_warehouse['START_TIME']).dt.date
            daily_credits_by_warehouse = time_series_warehouse.groupby(['DATE', 'WAREHOUSE_NAME'])['CREDITS'].sum().reset_index()
            daily_credits_by_warehouse = daily_credits_by_warehouse[daily_credits_by_warehouse['WAREHOUSE_NAME'].notna()]
            
            chart = alt.Chart(daily_credits_by_warehouse).mark_area().encode(
                x=alt.X('DATE:T', title='Date'),
                y=alt.Y('CREDITS:Q', title='Credits', stack='zero'),
                color=alt.Color('WAREHOUSE_NAME:N', title='Warehouse'),
                tooltip=[
                    alt.Tooltip('DATE:T', format='%Y-%m-%d'),
                    'WAREHOUSE_NAME:N',
                    alt.Tooltip('CREDITS:Q', format='.6f')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        
        if not df_functions.empty and 'TOKENS' in df_functions.columns:
            st.subheader(":material/trending_up: Token Consumption Over Time")
            time_series_tokens = df_functions.copy()
            time_series_tokens['DATE'] = pd.to_datetime(time_series_tokens['START_TIME']).dt.date
            daily_tokens_by_model = time_series_tokens.groupby(['DATE', 'MODEL_NAME'])['TOKENS'].sum().reset_index()
            
            chart = alt.Chart(daily_tokens_by_model).mark_area().encode(
                x=alt.X('DATE:T', title='Date'),
                y=alt.Y('TOKENS:Q', title='Tokens', stack='zero'),
                color=alt.Color('MODEL_NAME:N', title='Model'),
                tooltip=[
                    alt.Tooltip('DATE:T', format='%Y-%m-%d'),
                    'MODEL_NAME:N',
                    alt.Tooltip('TOKENS:Q', format=',')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(":material/memory: By Model")
            if not df_functions.empty and 'MODEL_NAME' in df_functions.columns:
                model_costs = df_functions.groupby('MODEL_NAME')['CREDITS'].sum().sort_values(ascending=False).head(10)
                if not model_costs.empty:
                    model_df = pd.DataFrame(model_costs).reset_index()
                    model_df = model_df.sort_values('CREDITS', ascending=False)
                    chart = alt.Chart(model_df).mark_bar().encode(
                        x=alt.X('CREDITS:Q', title='Credits'),
                        y=alt.Y('MODEL_NAME:N', sort='-x', title='Model'),
                        tooltip=['MODEL_NAME', 'CREDITS']
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No model data available")
            else:
                st.info("No model data available")
        
        with col2:
            st.subheader(":material/functions: By Function")
            if not df_functions.empty and 'FUNCTION_NAME' in df_functions.columns:
                function_costs = df_functions.groupby('FUNCTION_NAME')['CREDITS'].sum().sort_values(ascending=False).head(10)
                if not function_costs.empty:
                    function_df = pd.DataFrame(function_costs).reset_index()
                    function_df = function_df.sort_values('CREDITS', ascending=False)
                    chart = alt.Chart(function_df).mark_bar().encode(
                        x=alt.X('CREDITS:Q', title='Credits'),
                        y=alt.Y('FUNCTION_NAME:N', sort='-x', title='Function'),
                        tooltip=['FUNCTION_NAME', 'CREDITS']
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No function data available")
            else:
                st.info("No function data available")
        
        st.subheader(":material/warehouse: By Warehouse")
        if not df_functions.empty and 'WAREHOUSE_NAME' in df_functions.columns:
            warehouse_costs = df_functions.groupby('WAREHOUSE_NAME')['CREDITS'].sum().sort_values(ascending=False)
            if not warehouse_costs.empty:
                warehouse_df = pd.DataFrame(warehouse_costs).reset_index()
                warehouse_df = warehouse_df.sort_values('CREDITS', ascending=False)
                chart = alt.Chart(warehouse_df).mark_bar().encode(
                    x=alt.X('CREDITS:Q', title='Credits'),
                    y=alt.Y('WAREHOUSE_NAME:N', sort='-x', title='Warehouse'),
                    tooltip=['WAREHOUSE_NAME', 'CREDITS']
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No warehouse data available")
        else:
            st.info("No warehouse data available")
    
    with tab3:
        if not df_functions.empty and 'TOKENS' in df_functions.columns:
            function_tokens = df_functions.groupby('FUNCTION_NAME').agg({
                'TOKENS': 'sum',
                'CREDITS': 'sum'
            }).reset_index()
            function_tokens['CREDITS_PER_1M_TOKENS'] = (function_tokens['CREDITS'] / function_tokens['TOKENS']) * 1_000_000
            function_tokens = function_tokens.sort_values('TOKENS', ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(":material/bar_chart: Tokens by Model")
                if not model_efficiency.empty:
                    model_tokens_df = model_efficiency.sort_values('TOKENS', ascending=False).head(10)
                    chart = alt.Chart(model_tokens_df).mark_bar().encode(
                        x=alt.X('TOKENS:Q', title='Total Tokens'),
                        y=alt.Y('MODEL_NAME:N', sort='-x', title='Model'),
                        color=alt.Color('CREDITS_PER_1M_TOKENS:Q', 
                                      scale=alt.Scale(scheme='redyellowgreen', reverse=True),
                                      title='Credits/1M Tokens'),
                        tooltip=['MODEL_NAME', 
                                alt.Tooltip('TOKENS:Q', format=','),
                                alt.Tooltip('CREDITS:Q', format='.6f'),
                                alt.Tooltip('CREDITS_PER_1M_TOKENS:Q', format='.2f')]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No token data available")
            
            with col2:
                st.subheader(":material/functions: Tokens by Function")
                if not function_tokens.empty:
                    function_tokens_chart = function_tokens.sort_values('TOKENS', ascending=False)
                    chart = alt.Chart(function_tokens_chart).mark_bar().encode(
                        x=alt.X('TOKENS:Q', title='Total Tokens'),
                        y=alt.Y('FUNCTION_NAME:N', sort='-x', title='Function'),
                        color=alt.Color('CREDITS_PER_1M_TOKENS:Q', 
                                      scale=alt.Scale(scheme='redyellowgreen', reverse=True),
                                      title='Credits/1M Tokens'),
                        tooltip=['FUNCTION_NAME', 
                                alt.Tooltip('TOKENS:Q', format=','),
                                alt.Tooltip('CREDITS:Q', format='.6f'),
                                alt.Tooltip('CREDITS_PER_1M_TOKENS:Q', format='.2f')]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No function token data available")
            
            st.subheader(":material/efficiency: Credits per 1M Tokens Efficiency")
            if not model_efficiency.empty:
                efficiency_df = model_efficiency.sort_values('CREDITS_PER_1M_TOKENS', ascending=False).head(10)
                chart = alt.Chart(efficiency_df).mark_bar().encode(
                    x=alt.X('CREDITS_PER_1M_TOKENS:Q', title='Credits per 1M Tokens'),
                    y=alt.Y('MODEL_NAME:N', sort='-x', title='Model'),
                    color=alt.Color('CREDITS_PER_1M_TOKENS:Q',
                                  scale=alt.Scale(scheme='redyellowgreen', reverse=True),
                                  legend=None),
                    tooltip=['MODEL_NAME', 
                            alt.Tooltip('CREDITS_PER_1M_TOKENS:Q', format='.2f'),
                            alt.Tooltip('TOKENS:Q', format=','),
                            alt.Tooltip('CREDITS:Q', format='.6f')]
                ).properties(height=300)
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No efficiency data available")
            
            st.subheader(":material/table: Token Economy Summary")
            if not model_efficiency.empty:
                summary_df = model_efficiency.copy()
                summary_df['EFFICIENCY_RANK'] = summary_df['CREDITS_PER_1M_TOKENS'].rank(method='min').astype(int)
                summary_df = summary_df.sort_values('TOKENS', ascending=False)
                
                st.dataframe(
                    summary_df,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "MODEL_NAME": st.column_config.TextColumn("Model"),
                        "TOKENS": st.column_config.NumberColumn("Total Tokens", format="%d"),
                        "CREDITS": st.column_config.NumberColumn("Total Credits", format="%.6f"),
                        "CREDITS_PER_1M_TOKENS": st.column_config.NumberColumn("Credits per 1M Tokens", format="%.2f"),
                        "EFFICIENCY_RANK": st.column_config.NumberColumn("Efficiency Rank", format="%d")
                    }
                )
        else:
            st.info("Token economy data is only available for Cortex Functions")
    
    st.divider()
    
    st.subheader(":material/table: Detailed Data")
    
    tab1, tab2, tab3 = st.tabs(["Cortex Analyst", "Cortex Functions", "Cortex Search"])
    
    with tab1:
        if not df_analyst.empty:
            st.dataframe(
                df_analyst,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "START_TIME": st.column_config.DatetimeColumn("Start Time", format="DD/MM/YYYY HH:mm"),
                    "END_TIME": st.column_config.DatetimeColumn("End Time", format="DD/MM/YYYY HH:mm"),
                    "CREDITS": st.column_config.NumberColumn("Credits", format="%.6f"),
                    "REQUEST_COUNT": st.column_config.NumberColumn("Requests", format="%d")
                }
            )
            
            csv = df_analyst.to_csv(index=False)
            st.download_button(
                ":material/download: Download CSV",
                csv,
                "cortex_analyst_usage.csv",
                "text/csv"
            )
        else:
            st.info("No Cortex Analyst data available for selected filters")
    
    with tab2:
        if not df_functions.empty:
            st.dataframe(
                df_functions,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "START_TIME": st.column_config.DatetimeColumn("Start Time", format="DD/MM/YYYY HH:mm"),
                    "END_TIME": st.column_config.DatetimeColumn("End Time", format="DD/MM/YYYY HH:mm"),
                    "CREDITS": st.column_config.NumberColumn("Credits", format="%.6f"),
                    "TOKENS": st.column_config.NumberColumn("Tokens", format="%d")
                }
            )
            
            csv = df_functions.to_csv(index=False)
            st.download_button(
                ":material/download: Download CSV",
                csv,
                "cortex_functions_usage.csv",
                "text/csv"
            )
        else:
            st.info("No Cortex Functions data available for selected filters")
    
    with tab3:
        if not df_search.empty:
            st.dataframe(
                df_search,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "START_TIME": st.column_config.DatetimeColumn("Start Time", format="DD/MM/YYYY HH:mm"),
                    "END_TIME": st.column_config.DatetimeColumn("End Time", format="DD/MM/YYYY HH:mm"),
                    "CREDITS": st.column_config.NumberColumn("Credits", format="%.9f")
                }
            )
            
            csv = df_search.to_csv(index=False)
            st.download_button(
                ":material/download: Download CSV",
                csv,
                "cortex_search_usage.csv",
                "text/csv"
            )
        else:
            st.info("No Cortex Search data available for selected filters")

else:
    st.warning("No data available for the selected date range and service types")

with st.sidebar:
    st.divider()
    st.caption(f"Connected as: {conn_info.get('CURRENT_USER()', 'Unknown')}")
    st.caption(f"Role: {conn_info.get('CURRENT_ROLE()', 'Unknown')}")
