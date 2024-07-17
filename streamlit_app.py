import asyncio
import pandas as pd
import numpy as np
import streamlit as st
import os
import sys
# from sklearn.preprocessing import MinMaxScaler
from Functions.utils import (run_playwright,
                             save_df_in_pickle,
                             get_product_html)

os.system("playwright install")

##################################################################################

# PAGE CONFIGURATION

st.set_page_config(
    page_title='Amazon Recommender',
    layout='wide',
    initial_sidebar_state='auto',
)

##################################################################################

# LOADING FUNCTIONS

@st.cache_data
def load_df(filename):
    df = pd.read_pickle(f'{filename}.pkl')
    return df

def get_filtered_df(
    df,
    pop_weight=0.7,
    price_weight=0.2,
    disc_weight=0.1,
    ascending=False
):
    df['customized_score'] = np.round(pop_weight * df['popularity_score_norm'] + price_weight * df['disc_price_inv_norm'] + disc_weight * df['disc_amount_norm'], 2)
    
    return df.sort_values('customized_score', ascending=ascending).reset_index()

##################################################################################

# INPUT WIDGETS

st.write('<h1 style="text-align: center;">Amazon Product Search Optimizer</h1>', unsafe_allow_html=True)

search_submitted = True
with st.form('search_form'):
    search_term = st.text_input(
        label='Enter Search term below:'
    )
    
    # Submit button
    search_submitted = st.form_submit_button()

##################################################################################

# INTERNAL CALCULATIONS

# Run webscraping, fetching data from Amazon.
if search_submitted:
    # Search url.
    domain = 'https://www.amazon.'
    regional_domain = 'es'
    domain_url = domain + regional_domain
    start_url = domain_url + '/s?k=' + search_term

    # Filename.
    filename = '_'.join(search_term.split(' '))
    
    # Set up event loop using of ProactorEventLoop which is specifically tailored for Windows,
    # ensuring efficient handling of asynchronous I/O operations ().
    # Use SelectorEventLoop for non-Windows environments (like Linux or macOS).
    # Streamlit does not support asyncio.run(main()) for windows.
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    
    # Run event loop.
    with st.spinner('Fetching data from Amazon...'):
        data_list, time_elapsed, page_counter = loop.run_until_complete(run_playwright(
            start_url=start_url,
            domain_url=domain_url
        ))

        # Removes any previous pkl files in the directory.
        pkl_files = [file for file in os.listdir('./') if file.endswith('.pkl')]
        for pkl_file in pkl_files:
            os.remove(os.path.join('./', pkl_file))
        
        n_duplicates = save_df_in_pickle(data_list=data_list, search_term=search_term)

    # Data loading has been completed!
    df = load_df(filename)
    
    st.success(f"Done. It took **{time_elapsed:.0f}s** to complete!")
    st.info(f"A total of **{page_counter}** product pages have been scraped with a total of **{len(data_list)}** valid products. Found **{n_duplicates}** duplicated products, resulting in **{len(df)} final products**!")

##################################################################################

# SIDEBAR WIDGETS

with st.form('filter_form'):
    st.write('🧑‍🔧 Choose weights for customized score:')
    st.caption("""
    - Note that products are sorted based on **customized score (0-100)**.
    - Note all 3 weights **must sum up to 1**!
    """)
    st.write('')
    sort_order = st.radio(
      label='`Sort products by`:',
      options=['Highest customized score.', 'Lowest customized score'],
      index=0
    )
    col_wid1, col_wid2, col_wid3 = st.columns(3, gap='large')
    pop_weight = col_wid1.number_input(
        label="`Popularity weight`:",
        min_value=0.0,
        max_value=1.0,
        value=0.7,
        step=0.1,
        help="Weight applied to normalized popularity score (0-100). The larger the weight, the more it **emphasizes popular products in terms of review and number of reviews**."
    )
    price_weight = col_wid2.number_input(
        label="`Inverse price weight`:",
        min_value=0.0,
        max_value=1.0,
        value=0.2,
        step=0.1,
        help='Weight applied to normalized inverse price (0-100). The larger the weight, the more it **emphasizes inexpensive products**.'
    )
    disc_weight = col_wid3.number_input(
        label="`Discount weight`:",
        min_value=0.0,
        max_value=1.0,
        value=0.1,
        step=0.1,
        help='Weight applied to normalized discounted amount (0-100). The larger the weight, the more it **emphasizes products with large absolute discount in price**.'
    )
    
    # submit button.
    filter_submitted = st.form_submit_button('Filter')

##################################################################################

# BODY

# Displaying each product in html.
if filter_submitted:
    pkl_files = [file for file in os.listdir('./') if file.endswith('.pkl')]
    df = load_df(pkl_files[0].split('.')[0])

    ascending = False if sort_order == 'Highest customized score.' else True
    df = get_filtered_df(
        df=df,
        pop_weight=pop_weight,
        price_weight=price_weight,
        disc_weight=disc_weight,
        ascending=ascending
    )
    for _, row in df.iterrows():
        if row['currency'] is not 'Unknown':
            currency = row['currency']
            break

    st.write(f'<h3 style="text-align: center;">Search Results (price is {currency})</h3>', unsafe_allow_html=True)
             
    cols_per_row = 4
    # Get a list of str containers with different names for each product in df.
    containers_list = [f'container{i}' for i in range(len(df)+1)]
    for i in range(0, len(df)+1, cols_per_row):
        # extract chunks of 4-row df and 4-container variable (different names) at a time.
        df_chunk = df.iloc[i:i+cols_per_row]
        containers_chunk = containers_list[i:i+cols_per_row]
        # cols_row is a tuple where each 4 element is a col container. 
        # assign each col container to container variable. Not a list of str anymore: list[DeltaGenerator].
        containers_chunk = st.columns(cols_per_row)

        for (i, row), container in zip(df_chunk.iterrows(), containers_chunk):
            product_html = get_product_html(
                image=row['image'],
                product_name=row['product name'],
                url=row['url'],
                ranking=i+1,
                total_results=len(df),
                customized_score=row['customized_score'],
                popularity_score=row['popularity_score_norm'],
                stars=row['stars'],
                n_reviews=row['n_reviews'],
                discounted_price=row['discounted_price'],
                original_price=row['original_price'],
                discount_perc=row['discount_perc']
            )
            
            container.write(product_html, unsafe_allow_html=True)
