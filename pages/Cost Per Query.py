import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection parameters
snowflake_params = {
    'user': 'Avinash',
    'password': 'Covid@2021',
    'account': 'ym83942.ap-south-1',
    'warehouse': 'WH_XS',
    'database': 'UTIL_NEW',
    'schema': 'SAMPLE_SCH',
}

st.header('Cost Per Query')

st.markdown(
    """
    ### Cost Per Query.
    -- provides us with an approximate query cost for each query_id. To accommodate instances where the same query is executed multiple times within a given period, we have aggregated the results based on the query_text.
    **👈 Select a demo from the sidebar** to see some examples
    of what Streamlit can do!
    
"""
)

def calculate_and_display_warehouse_cost(snowflake_params):
    try:
        cost_per_query = """
        WITH
        warehouse_sizes AS (
            SELECT 'X-Small' AS warehouse_size, 1 AS credits_per_hour UNION ALL
            SELECT 'Small' AS warehouse_size, 2 AS credits_per_hour UNION ALL
            SELECT 'Medium'  AS warehouse_size, 4 AS credits_per_hour UNION ALL
            SELECT 'Large' AS warehouse_size, 8 AS credits_per_hour UNION ALL
            SELECT 'X-Large' AS warehouse_size, 16 AS credits_per_hour UNION ALL
            SELECT '2X-Large' AS warehouse_size, 32 AS credits_per_hour UNION ALL
            SELECT '3X-Large' AS warehouse_size, 64 AS credits_per_hour UNION ALL
            SELECT '4X-Large' AS warehouse_size, 128 AS credits_per_hour
        ),
        queries AS (
            SELECT
                qh.query_id,
                qh.query_text,
                qh.execution_time/(1000*60*60)*wh.credits_per_hour AS query_cost
            FROM snowflake.account_usage.query_history AS qh
            INNER JOIN warehouse_sizes AS wh
                ON qh.warehouse_size=wh.warehouse_size
            WHERE
                start_time >= CURRENT_DATE - 30
        )
        SELECT
            query_text,
            SUM(query_cost) AS total_query_cost_last_30d
        FROM queries
        GROUP BY 1;
        """
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**snowflake_params)
        
        # Execute the query
        cursor = conn.cursor()
        cursor.execute(cost_per_query)
        
        # Fetch the result
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        # Convert the result to a Pandas DataFrame for easier display with Streamlit
        df = pd.DataFrame(result, columns=column_names)
        
        # Display the result
        st.dataframe(df)
        
        # Optionally, you can format the cost column as currency
        # df['total_query_cost_last_30d'] = df['total_query_cost_last_30d'].map('${:,.2f}'.format)
        
        # Optionally, you can also create a bar chart
        # st.bar_chart(data=df, x="query_id", y="total_query_cost_last_30d")
        
        st.success('Warehouse costs calculated successfully!')
        
    except Exception as e:
        st.error(f'Error: {str(e)}')
    finally:
        # Close Snowflake connection
        conn.close()

calculate_and_display_warehouse_cost(snowflake_params)