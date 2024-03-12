import streamlit as st
import pandas as pd

import snowflake.connector
from constant import *
#streamlifrom static import *
import streamlit.components.v1 as components
st.write("# Snowflake Administration Tool! 👋")

#image_url = "https://drive.google.com/uc?export=view&id=1x0ew-WjGcjaSPOMC8lPA0kWwTeu--qK6"

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
    'account': 'mf95899.ap-south-1',
    'warehouse': 'COMPUTE_WH',
    'database': 'UTIL_NEW',
    'schema': 'SAMPLE_SCH',
}

# -----------------  loading assets  ----------------- #
st.sidebar.markdown(info['image_url'])

######
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
            from SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE NAME NOT ILIKE 'Avinash')
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


#######
st.image("C:/code/folder/.venv/static/wallpaper1.png", use_column_width=True)
# -----------------  endorsement  ----------------- #
with st.container():
    # Divide the container into three columns
    col1,col2,col3 = st.columns([0.475, 0.475, 0.05])
    # In the first column (col1)        
    with col1:
        # Add a subheader to introduce the coworker endorsement slideshow
        st.subheader("Coworker Endorsements")
        # Embed an HTML component to display the slideshow
        components.html(
        f"""
        <!DOCTYPE html>
        <html>
        <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <!-- Styles for the slideshow -->
        <style>
            * {{box-sizing: border-box;}}
            .mySlides {{display: none;}}
            img {{vertical-align: middle;}}

            /* Slideshow container */
            .slideshow-container {{
            position: relative;
            margin: auto;
            width: 100%;
            }}

            /* The dots/bullets/indicators */
            .dot {{
            height: 15px;
            width: 15px;
            margin: 0 2px;
            background-color: #eaeaea;
            border-radius: 50%;
            display: inline-block;
            transition: background-color 0.6s ease;
            }}

            .active {{
            background-color: #6F6F6F;
            }}

            /* Fading animation */
            .fade {{
            animation-name: fade;
            animation-duration: 1s;
            }}

            @keyframes fade {{
            from {{opacity: .4}} 
            to {{opacity: 1}}
            }}

            /* On smaller screens, decrease text size */
            @media only screen and (max-width: 300px) {{
            .text {{font-size: 11px}}
            }}
            </style>
        </head>
        <body>
            <!-- Slideshow container -->
            <div class="slideshow-container">
                <div class="mySlides fade">
                <img src={endorsements["img1"]} style="width:100%">
                </div>

                <div class="mySlides fade">
                <img src={endorsements["img2"]} style="width:100%">
                </div>

                <div class="mySlides fade">
                <img src={endorsements["img3"]} style="width:100%">
                </div>

            </div>
            <br>
            <!-- Navigation dots -->
            <div style="text-align:center">
                <span class="dot"></span> 
                <span class="dot"></span> 
                <span class="dot"></span> 
            </div>

            <script>
            let slideIndex = 0;
            showSlides();

            function showSlides() {{
            let i;
            let slides = document.getElementsByClassName("mySlides");
            let dots = document.getElementsByClassName("dot");
            for (i = 0; i < slides.length; i++) {{
                slides[i].style.display = "none";  
            }}
            slideIndex++;
            if (slideIndex > slides.length) {{slideIndex = 1}}    
            for (i = 0; i < dots.length; i++) {{
                dots[i].className = dots[i].className.replace("active", "");
            }}
            slides[slideIndex-1].style.display = "block";  
            dots[slideIndex-1].className += " active";
            }}

            var interval = setInterval(showSlides, 2500); // Change image every 2.5 seconds

            function pauseSlides(event)
            {{
                clearInterval(interval); // Clear the interval we set earlier
            }}
            function resumeSlides(event)
            {{
                interval = setInterval(showSlides, 2500);
            }}
            // Set up event listeners for the mySlides
            var mySlides = document.getElementsByClassName("mySlides");
            for (i = 0; i < mySlides.length; i++) {{
            mySlides[i].onmouseover = pauseSlides;
            mySlides[i].onmouseout = resumeSlides;
            }}
            </script>

            </body>
            </html> 

            """,
                height=270,
    ) 