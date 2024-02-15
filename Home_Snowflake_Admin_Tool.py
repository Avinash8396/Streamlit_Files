import streamlit as st
import pandas as pd
#from snowflake.snowpark.context import get_active_session
import snowflake.connector

st.write("# Snowflake Administration Tool! 👋")

st.markdown(
    """
    Please note that all user details are sourced from the account usage schema, and there exists a typical data latency ranging from 45 minutes to 3 hours, which may vary across different views.
    **👈 Select a demo from the sidebar** to see some examples
    of what Streamlit can do!
    ### Want to learn more?
    - Check out [Administering Snowflake](https://community.snowflake.com/s/snowflake-administration)
    - Jump into our [documentation](https://docs.streamlit.io)
    - Ask a question in our [community
        forums](https://discuss.streamlit.io)
    ### See more complex demos
    - Use a neural net to [analyze the Udacity Self-driving Car Image
        Dataset](https://github.com/streamlit/demo-self-driving)
    - Explore a [New York City rideshare dataset](https://github.com/streamlit/demo-uber-nyc-pickups)
"""
)



# Snowflake connection parameters
snowflake_params = {
    'user': 'Avinash',
    'password': 'Covid@2021',
    'account': 'ym83942.ap-south-1',
    'warehouse': 'WH_XS',
    'database': 'UTIL_NEW',
    'schema': 'SAMPLE_SCH',
}

# Function to create a new user
def create_new_user(user_name, user_password, conn):
    try:
        create_user_query = f'CREATE USER IF NOT EXISTS {user_name} PASSWORD = \'{user_password}\';'
        conn.cursor().execute(create_user_query)
        st.success(f'User {user_name} added successfully!')
    except Exception as e:
        st.error(f'Error adding user: {str(e)}')

# Function to drop a user
def drop_user(selected_user, conn):
    try:
        drop_user_query = f'DROP USER IF EXISTS {selected_user};'
        conn.cursor().execute(drop_user_query)
        st.success(f'User {selected_user} dropped successfully!')
    except Exception as e:
        st.error(f'Error dropping user: {str(e)}')

# Streamlit app
st.title('Snowflake Admin')

# Add new user section
st.header('Add New User')

# Input fields for new user
new_user_name = st.text_input('Enter New User Name:')
new_user_password = st.text_input('Enter New User Password:', type='password')

# Button to add new user
if st.button('Add New User'):
    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_params)

    # Create new user
    create_new_user(new_user_name, new_user_password, conn)

    # Close Snowflake connection
    conn.close()

# Drop user section
st.header('Drop User')

# Get list of existing users
conn = snowflake.connector.connect(**snowflake_params)
existing_users_query = """WITH CTE AS (
            Select USER_ID, NAME, LOGIN_NAME, CREATED_ON,DELETED_ON,
                   CASE WHEN CREATED_ON IS NOT NULL AND DELETED_ON IS NULL THEN 'Active'
                   WHEN CREATED_ON IS NOT NULL AND DELETED_ON IS NOT NULL THEN 'STALE'
                   END AS STATUS
            from SNOWFLAKE.ACCOUNT_USAGE.USERS)
SELECT NAME as USER FROM CTE WHERE STATUS ILIKE 'ACTIVE';"""
existing_users = [row[0] for row in conn.cursor().execute(existing_users_query).fetchall()]
# Add default selection
existing_users.insert(0, "Select User")
selected_user = st.selectbox('Select User to Drop:', existing_users)

# Button to drop selected user
if st.button('Drop Selected User'):
    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_params)
    # Drop selected user
    drop_user(selected_user, conn)

    # Close Snowflake connection
    conn.close()

# Input box for SQL query
st.header('Refresh To View All Users')
sql_query1 = st.text_area("","Please note that all user details are sourced from the account usage schema, and there exists a typical data latency ranging from 45 minutes to 3 hours, which may vary across different views.")
sql_query = """ Select USER_ID, NAME, LOGIN_NAME, CREATED_ON,DELETED_ON, 
                       CASE WHEN CREATED_ON IS NOT NULL AND DELETED_ON IS NULL THEN 'Active' 
                       WHEN CREATED_ON IS NOT NULL AND DELETED_ON IS NOT NULL THEN 'STALE' 
                       END AS STATUS 
                from SNOWFLAKE.ACCOUNT_USAGE.USERS; """
# Connect to Snowflake
conn = snowflake.connector.connect(**snowflake_params)

# Execute user-provided SQL query and fetch data into a Pandas DataFrame
def Refresh_user(sql_query, conn):
    try:
        df = pd.read_sql_query(sql_query, conn)
        if not df.empty:
            st.success('Query executed successfully!')
            st.dataframe(df)
        else:
            st.info('Query executed successfully, but no results were returned.')
    except Exception as e:
        st.error(f'Error executing query: {str(e)}')


# Button to add new user
if st.button('Refresh'):
    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_params)

    # Create new user
    Refresh_user(sql_query, conn)

    # Close Snowflake connection
    conn.close()

# Close Snowflake connection
conn.close()
