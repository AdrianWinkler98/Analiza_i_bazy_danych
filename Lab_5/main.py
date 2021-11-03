import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category, int) and not isinstance(category, str):
        return None

    quotation = f"""select film.title, language.name AS languge, cat.name AS category
                    from film
                    JOIN language ON language.language_id = film.language_id
                    JOIN film_category ON film_category.film_id = film.film_id
                    JOIN category cat ON cat.category_id = film_category.category_id
                    WHERE cat.category_id = {category}
                    ORDER BY film.title, languge"""

    quotation_2 = f"""select film.title, language.name AS languge, category.name AS category
                    from film
                    JOIN language ON language.language_id = film.language_id
                    JOIN film_category ON film_category.film_id = film.film_id
                    JOIN category ON category.category_id = film_category.category_id
                    WHERE category.name LIKE '{category}'
                    ORDER BY film.title, languge"""


    return pd.read_sql_query(quotation if isinstance(category, int) else quotation_2, con=connection)
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category, int) and not isinstance(category, str):
        return None

    quotation = f"""select film.title, language.name AS languge, cat.name AS category
                from film
                INNER JOIN language ON language.language_id = film.language_id
                INNER JOIN film_category ON film_category.film_id = film.film_id
                INNER JOIN category cat ON cat.category_id = film_category.category_id
                WHERE cat.category_id = {category}
                ORDER BY film.title, languge"""

    quotation_2 = f"""select film.title, language.name AS languge, cat.name AS category
                from film
                INNER JOIN language ON language.language_id = film.language_id
                INNER JOIN film_category ON film_category.film_id = film.film_id
                INNER JOIN category cat ON cat.category_id = film_category.category_id
                WHERE cat.name ILIKE '{category}'
                ORDER BY film.title, languge"""


    return pd.read_sql_query(quotation if isinstance(category, int) else quotation_2, con=connection)

def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    quotation = f"""select actor.first_name, actor.last_name
                from actor
                INNER JOIN film_actor ON film_actor.actor_id = actor.actor_id
                INNER JOIN film ON film.film_id = film_actor.film_id
                WHERE film.title LIKE '{title}'
                ORDER BY actor.last_name, actor.first_name"""

    return pd.read_sql_query(quotation, con=connection) if isinstance(title, str) else None
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''

    words_string = '|'.join(words)
    quotation = f"""select title
                FROM film
                WHERE title ~* '(?:^| )({words_string})""" + """{1,}(?:$| )'
                ORDER BY title"""

    return pd.read_sql_query(quotation, con=connection) if isinstance(words, list) else None