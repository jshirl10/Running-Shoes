import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import psycopg2
import pandas as pd
import numpy as np


def get_list_of_shoe_URLs(pages_of_shoes):
    full_list_of_shoe_urls = []
    for i in range(len(pages_of_shoes)):
        requestinfo = requests.get(pages_of_shoes[i])
        soup = BeautifulSoup(requestinfo.content, 'html.parser')
        for link in soup.find_all('a', class_ = 'cattable-wrap-cell-info'):
            full_list_of_shoe_urls.append(link.get('href'))
    return full_list_of_shoe_urls

def clean_up_list(full_list_of_shoe_urls):
    unique_shoe_set = set()
    saved_indices = []

    for i, j in enumerate(full_list_of_shoe_urls):

        before_length = len(unique_shoe_set)
        
        try:
            shoe_name_start = re.search('.com/', j).end()

            shoe_name_end = re.search('/', j[re.search('.com/', j).end():]).start()

            unique_shoe_set.add(j[shoe_name_start:shoe_name_start + shoe_name_end])

            after_length = len(unique_shoe_set)

            if before_length != after_length:
                saved_indices.append(i)
        
        except AttributeError:
            continue
    
    filtered_list_of_shoe_urls = [full_list_of_shoe_urls[x] for x in saved_indices]
    filtered_list_of_shoe_urls.remove('https://www.runningwarehouse.com/learningcenter/gear_guides/footwear/best_running_shoes.html')
    
    return filtered_list_of_shoe_urls

def construct_shoe_list(shoe_to_add, find_condition):
    if find_condition == None:
        shoe_to_add.append(None)
    else:
        shoe_to_add.append(find_condition.text)
    return shoe_to_add

def get_shoe_specs(shoe_to_add, find_condition):
  shoe_specs_names = []
  shoe_specs_values = []
  if find_condition != None:
    for i,j in enumerate(find_condition.find_all('td')):
      if i % 2 != 0:
        shoe_specs_values.append(j.text)

    for i,j in enumerate(find_condition.find_all('td')):
      if i % 2 == 0:
        shoe_specs_names.append(j.text)
        
    shoe_specs_dict = dict(zip(shoe_specs_names, shoe_specs_values))

    return shoe_specs_dict
  return None

def build_shoe_df(filtered_list_of_shoe_urls):
    shoes = pd.DataFrame(columns = ['Name', 'Average Rating', 'Number of Ratings', 'Price', 'Weight',
                                'Heel Stack', 'Forefoot Stack', 'Heel-Toe Offset', 'Cushioning', 'Stability'])
    column_list = ['Name', 'Average Rating', 'Number of Ratings', 'Price', 'Weight',
                'Heel Stack', 'Forefoot Stack', 'Heel-Toe Offset', 'Cushioning', 'Stability']

    for i in filtered_list_of_shoe_urls:

        shoe_to_add = []

        requestinfo = requests.get(i)
        soup = BeautifulSoup(requestinfo.content, 'html.parser')

        # Get Shoe Name
        shoe_to_add = construct_shoe_list(shoe_to_add, soup.find('h1', class_ = 'h2 desc_top-head-title'))

        # Get Star Review
        shoe_to_add = construct_shoe_list(shoe_to_add, soup.find('div', class_ = 'review_agg'))

        # Get Number of Reviews
        shoe_to_add = construct_shoe_list(shoe_to_add, soup.find('span', class_ = 'review_count'))

        # Get Shoe Price
        shoe_to_add = construct_shoe_list(shoe_to_add, soup.find('span', class_ = 'afterpay-full_price'))

        # Get Shoe Specs
        shoe_specs_dict = get_shoe_specs(shoe_to_add, soup.find('table', class_ = 'fit_table'))
        if shoe_specs_dict != None:
            # Get Shoe Weight
            if 'Weight' in shoe_specs_dict:
                shoe_to_add.append(shoe_specs_dict['Weight'])
            else:
                shoe_to_add.append(None)
            
            # Get Heel Stack
            if 'Heel Stack' in shoe_specs_dict:
                shoe_to_add.append(shoe_specs_dict['Heel Stack'])
            else:
                shoe_to_add.append(None)

            # Get Forefoot Stack
            if 'Forefoot Stack' in shoe_specs_dict:
                shoe_to_add.append(shoe_specs_dict['Forefoot Stack'])
            else:
                shoe_to_add.append(None)

            # Get Heel-Toe Offset
            if 'Heel-Toe Offset:' in shoe_specs_dict:
                shoe_to_add.append(shoe_specs_dict['Heel-Toe Offset:'])
            else:
                shoe_to_add.append(None)
        else:
            shoe_to_add.append(None)
            shoe_to_add.append(None)
            shoe_to_add.append(None)
            shoe_to_add.append(None)

        # Get Cushioning and Stability
        cushioning_rating = soup.find('div', class_ = 'row no-gutters bestuse is-range')
        if cushioning_rating == None:
            shoe_to_add.append(None)
            shoe_to_add.append(None)
        else:
            if cushioning_rating.text.find('Minimal') != -1:
                shoe_to_add = construct_shoe_list(shoe_to_add, cushioning_rating.find('div', class_ = 'col is-active'))
                stability_rating = cushioning_rating.find_next('div', class_ = 'row no-gutters bestuse is-range')
                if stability_rating == None:
                    shoe_to_add.append(None)
                else: 
                    shoe_to_add = construct_shoe_list(shoe_to_add, stability_rating.find('div', class_ = 'col is-active'))
            else:
                shoe_to_add.append(None)
                stability_rating = soup.find('div', class_ = 'row no-gutters bestuse is-range')
                if stability_rating == None:
                    shoe_to_add.append(None)
                else: 
                    shoe_to_add = construct_shoe_list(shoe_to_add, stability_rating.find('div', class_ = 'col is-active'))
        shoe_dataframe_row = dict(zip(column_list, shoe_to_add))
        shoes = shoes.append(shoe_dataframe_row, ignore_index = True)
    
    return shoes

def create_db_connection():
    conn = psycopg2.connect(
       database=database_name, 
       user=user_name, 
       password=user_password, 
       host=host, 
       port=port_num
    )
    return conn

def prepare_df_for_insertion(shoe_list):
    shoe_list = shoe_list.dropna(subset=['Name'])
    shoe_list = shoe_list.replace(to_replace=['', ' '], value=np.nan)
    shoe_list = [tuple(None if pd.isna(val) else val for val in x) for x in np.nan_to_num(shoe_list.to_numpy(), nan=np.nan)]
    return shoe_list


def insert_update_shoes(shoe_list, conn):
    sql = '''
        INSERT INTO shoes 
        (shoe_name, average_rating, num_ratings, price, weight, 
        heel_stack, forefoot_stack, stack_offset, cushioning, stability) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        ON CONFLICT (shoe_name) DO UPDATE SET
            shoe_name=EXCLUDED.shoe_name, average_rating=EXCLUDED.average_rating,
            num_ratings=EXCLUDED.num_ratings, price=EXCLUDED.price, weight=EXCLUDED.weight,
            heel_stack=EXCLUDED.heel_stack, forefoot_stack=EXCLUDED.forefoot_stack,
            stack_offset=EXCLUDED.stack_offset, cushioning=EXCLUDED.cushioning, stability=EXCLUDED.stability
        RETURNING shoe_id;
        '''
    try:
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,shoe_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':

    pages_of_shoes = [
        'https://www.runningwarehouse.com/catpage-MNROAD.html',
        'https://www.runningwarehouse.com/catpage-MSROAD.html',
        'https://www.runningwarehouse.com/trailshoesmen.html'
    ]

    print('Get Shoe URLs')
    list_of_shoe_urls = get_list_of_shoe_URLs(pages_of_shoes)

    print('Clean Up URL List')
    filtered_list_of_shoe_urls = clean_up_list(list_of_shoe_urls)

    print('Build Shoe Dataframe')
    shoes = build_shoe_df(filtered_list_of_shoe_urls)

    print('Create DB Connection')
    conn = create_db_connection()

    print('Prep Data for Insertion')
    shoe_list = prepare_df_for_insertion(shoes)

    print('Insert Shoe Data')
    insert_update_shoes(shoe_list, conn)







        