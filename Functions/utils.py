from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

##################################################################################

async def get_product_data(item_tag, domain_url):
    name = item_tag.select_one('h2 a span').text
    img = item_tag.select_one('span[data-component-type="s-product-image"] img').attrs['src']
    url = domain_url + item_tag.select_one('span[data-component-type="s-product-image"] a').attrs['href']
    
    # price_tag with discounted price: [<span class="a-offscreen">6,59 €</span>, <span class="a-offscreen">10,99 €</span>]
    # Could get a col % discount: (orig - disc / orig)* 100
    prices_list = [price_tag.text for price_tag in item_tag.select('div[data-cy="price-recipe"] span.a-offscreen')]
    if len(prices_list) == 0:
        print(f'Product not in sale|stock|cannot be shipped to your country|only_second_hand \n{url}\n')
        return None
    elif len(prices_list) == 1:
        orig_price, disc_price = prices_list[0], prices_list[0]
    # Cases where only one price (no discount) with additional (currency_unit/kg): "11.900,00$/kg". 
    elif (len(prices_list) >= 2) & ('.' in prices_list[-1]) & (',' in prices_list[-1]):
        orig_price, disc_price = prices_list[0], prices_list[0]
    else:
        orig_price, disc_price = prices_list[-1], prices_list[0]
    
    # Get currency unit.
    non_digit_pattern = '[^\d.,]+'
    try:
        currency_unit = re.findall(pattern=non_digit_pattern, string=orig_price)[0]
    except Exception as e:
        currency_unit = 'Unknown'
        # print(f"Error finding currency unit in {orig_price}: {e}. url: {url}")
        
    # Get only digits, '.', and ',' in prices.
    try:
        orig_price, disc_price = re.sub(pattern=non_digit_pattern, repl='', string=orig_price), re.sub(pattern=non_digit_pattern, repl='', string=disc_price)
    except Exception as e:
        pass
    
    # For EU: 49,50 is the equivalent for $49.50.
    # However for many countries in LA, India, ',' does not represent decimal places: ₹1,999 or $1,042.98.
    currency_price_comma = [
        '\xa0€',
        '€ ',   # netherlands
        ' zł',  # Poland
        ' kr'   # Sweden
    ]
    
    def modify_price_punctuations(price: str):
        # cases where price is 1.299,00 to 1299.00
        if len(price) > 6:
            return price.replace('.', '').replace(',', '.')
        # cases where price is 165,00 to 150.00
        else:
            return price.replace(',', '.')
    
    if currency_unit in currency_price_comma:
        orig_price = modify_price_punctuations(orig_price)
        disc_price = modify_price_punctuations(disc_price)
    else:
        orig_price, disc_price =  orig_price.replace(',', ''), disc_price.replace(',', '')

    review_tag = item_tag.select_one('div.a-row.a-size-small')
    if review_tag is None:
        stars, n_reviews = '0', '0'
    else:
        # review_tag.text = "4,6 de 5 estrellas 30.047".
        digit_patern = '[\d.,]+'
        review_numbers_list = re.findall(digit_patern, review_tag.text)
        # In japan amazon, first comes total 5 then the stars review.
        stars_position = 1 if currency_unit in ['￥'] else 0
        # Review_tag exist but no review in product but other text.
        if len(review_numbers_list) == 0:
            stars, n_reviews = '0', '0'
        # result: ['4,6', '5', '30.047']
        elif len(review_numbers_list) == 3:
            stars, n_reviews = review_numbers_list[stars_position], review_numbers_list[-1]
        # ['4,6', '5', '30', '047'] if "30 047 evaluation".
        elif len(review_numbers_list) == 4:
            stars, n_reviews = review_numbers_list[stars_position], ''.join(review_numbers_list[-2:])
        else:
            # Normalmente son productos sin reviews.
            stars, n_reviews = '0', '0'
            
        # Ensure stars value must be in [0, 5]. Most of the products having more than 5 stars have no review at all.
        if len(review_numbers_list) >= 3:
            stars_temp = float(review_numbers_list[stars_position].replace(',', '.'))
            if stars_temp > 5:
                stars, n_reviews = '0', '0'
            
            # En amazon español: 4,2.
            stars = stars.replace(',', '.')
            # En amazon español: 24.000 / inglés: 24,000.
            n_reviews = n_reviews.replace('.', '').replace(',', '')
    
    # Change dtypes.
    try:
        stars = float(stars)
        n_reviews = int(n_reviews)
        disc_price = float(disc_price)
        orig_price = float(orig_price)
    except Exception as e:
        print(f'\nProduct discarded "{name}" due to: {e}\n{url}')
        raise Exception('Stop here!')
    
    return (name, stars, n_reviews, disc_price, orig_price, currency_unit, img, url)

##################################################################################

async def fetch_page(page, page_url):
    await page.goto(page_url)
    return await page.inner_html('span.rush-component.s-latency-cf-section')

##################################################################################

async def extract_items_data(page_html, domain_url):
        bs = BeautifulSoup(page_html, 'lxml')
        item_tags = bs.select('div.sg-col-inner')
        
        # Store data in a list.
        data_list = []
        counter = 0
        for item_tag in item_tags:
            if item_tag.select_one('h2 a span') is not None:
                product_data_tuple = await get_product_data(item_tag, domain_url)
                if product_data_tuple is not None:
                    counter += 1
                    data_list.append(product_data_tuple)
        
        page_next_tag = bs.select_one('a.s-pagination-item.s-pagination-next.s-pagination-button.s-pagination-separator')
        return (data_list, page_next_tag)

##################################################################################

async def run_playwright(start_url: str, domain_url: str):
    # Url searched:
    start_time = time.time()
    
    async with async_playwright() as p:
        # Run headless to avoid captchas and cookies.
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        search_content = await fetch_page(page, start_url)
        data_list, page_next_tag = await extract_items_data(search_content, domain_url)
        
        # Pagination | Break out from loop when page_next_tag does not exist (None).
        page_counter = 1
        while page_next_tag is not None:
            next_url = domain_url + page_next_tag.attrs['href']
            search_content_next = await fetch_page(page, next_url)
            data_list_next, page_next_tag = await extract_items_data(search_content_next, domain_url)

            data_list.extend(data_list_next)
            page_counter+= 1
            
        await browser.close()
        
        end_time = time.time()
        time_elapsed = end_time -start_time
        
        return data_list, time_elapsed, page_counter

##################################################################################

def save_df_in_pickle(data_list, search_term):
    # creates df.
    df = pd.DataFrame(data=data_list, columns=['product name', 'stars', 'n_reviews', 'discounted_price', 'original_price', 'currency', 'image', 'url'])
    
    # Drop duplicates (there might be some).
    len_orig = len(df)
    df.drop_duplicates(subset=['product name'], inplace=True)
    n_duplicates = len_orig - len(df)
    
    # Get only products with stars between [0-5].
    df = df.loc[df.stars.between(0, 5)]
    
    # En españa productos con (1.4$/unidad) en price_tags...
    neg_disc_mask = df['discounted_price'] > df['original_price']
    df.loc[neg_disc_mask, 'original_price'] = df.loc[neg_disc_mask, 'discounted_price']

    df['discount_perc'] = np.round((df['original_price'] - df['discounted_price']) / df['original_price'] * 100, 2)
    df['popularity_score'] = df['stars'] + np.log10(1 + df['n_reviews'])
    df['inverse_discounted_price'] = 1/df['discounted_price']
    mm_scaler = MinMaxScaler(feature_range=(0, 100))
    # 100 - norm_value because we value lower price products: inverse of price.
    df['disc_price_inv_norm'] = mm_scaler.fit_transform(df[['inverse_discounted_price']])
    df['popularity_score_norm'] = mm_scaler.fit_transform(df[['popularity_score']])
    df['disc_amount'] = df['original_price'] - df['discounted_price']
    df['disc_amount_norm'] = mm_scaler.fit_transform(df[['disc_amount']])
    
    df = df[[
        'product name', 
        'popularity_score', 'stars', 'n_reviews',
        'currency', 'discounted_price', 'original_price', 'discount_perc', 'url', 'image',
        'inverse_discounted_price', 'popularity_score_norm', 'disc_price_inv_norm', 'disc_amount_norm'
    ]]

    filename = '_'.join(search_term.split(' '))
    df.to_pickle(f'{filename}.pkl')
    
    return n_duplicates

##################################################################################

def get_product_html(
    image,
    product_name,
    url,
    ranking,
    total_results,
    customized_score,
    popularity_score,
    stars,
    n_reviews,
    discounted_price,
    original_price,
    discount_perc
):
    html = f'''
    <tr>
        <td>
            <div style="text-align: center; font-size: 14px; background-color: brown; color: white; font-weight: bold;">Ranking: {ranking}/{total_results}</div>
            <img src="{image}" alt="{product_name}" style="display: block; margin-left: auto; margin-right: auto;"/>
            <div style="text-align: center; font-size: 11.5px; font-weight: bold;">
                <a href="{url}" target="_blank">{product_name}</a>
            </div>
            <div style="text-align: center; font-size: 13.5px;">
                <span style="font-weight: bold;">customized score:</span> {customized_score:.2f}
            </div>
            <div style="text-align: center; font-size: 13px;">
                <span style="font-weight: bold;">popu score: </span> {popularity_score:.2f} |
                <span style="font-weight: bold;">review: </span>{stars}
                <span style="font-weight: bold;"> | nº reviews: </span>{n_reviews}
            </div>
            <div style="text-align: center; font-size: 13px;">
                <span style="font-weight: bold;">discounted price: </span> | 
                <span style="background-color: orange;">{discounted_price}</span>
                <span style="font-weight: bold;">original price: </span> <s>{original_price}</s>
            </div>
            <div style="text-align: center; font-size: 13px;">
                <span style="font-weight: bold;">discount: </span>
                <span style="background-color: yellow;">{discount_perc}%</span>
            </div>
        </td>
    </tr>
    '''
    return html
