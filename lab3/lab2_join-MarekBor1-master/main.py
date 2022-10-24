import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');
#Łączenie z bazą danych 

from sqlalchemy import create_engine

db_string = "postgresql://wbauer_adb:adb2020@pgsql-196447.vipserv.org:5432/wbauer_adb"

db = create_engine(db_string)
connection_sqlalchemy = db.connect()

##

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id,int):
        query = f'''
                select
                        film.title,
                        language.name AS "languge",
                        category.name AS "category"
                from category
                inner Join film_category ON category.category_id=film_category.category_id
                INNER JOIN film ON film_category.film_id = film.film_id 
                INNER JOIN language ON film.language_id = language.language_id
                Where category.category_id = {category_id}
                Order by film.title , languge
                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else :
        return None
    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category_id,int):
        query = f'''
                select
                        category.name AS "category",Count(film_category.film_id) AS "count"
                from category
                inner Join film_category ON category.category_id=film_category.category_id
                INNER JOIN film ON film_category.film_id = film.film_id 
                INNER JOIN language ON film.language_id = language.language_id
                Where category.category_id = {category_id}
                Group BY category.name;

                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else :
        return None

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczególnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(min_length,(int,float)) and isinstance(max_length,(int,float)) and min_length < max_length:
        query = f'''
                select
                        film.length AS "length",Count(length) AS "count"
                from film
                Where film.length >= {min_length} AND film.length <= {max_length}
                Group BY film.length;
                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else:
        return None

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(city,str):
        query = f'''
                select
                       city.city, customer.first_name ,customer.last_name
                from customer
                inner Join address ON customer.address_id=address.address_id
                INNER JOIN city ON address.city_id = city.city_id 

                Where city.city IN ('{city}')
                Order by first_name , last_name

                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else :
        return None

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(length,(int,float)):
        query = f'''
                select
                        film.length AS "length",Avg(payment.amount) AS "avg"
                FROM payment 
                    INNER JOIN rental ON payment.rental_id = rental.rental_id
                    INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id 
                    INNER JOIN film ON inventory.film_id = film.film_id
                    WHERE film.length = {length}
                    GROUP BY film.length
                    
                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else:
        return None

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(sum_min,(int,float)) :
        query = f'''
        SELECT customer.first_name, customer.last_name, SUM(film.length) as sum
                    FROM film 
                    INNER JOIN inventory ON film.film_id = inventory.film_id 
                    INNER JOIN rental ON inventory.inventory_id = rental.inventory_id
                    INNER JOIN customer ON rental.customer_id = customer.customer_id
                    GROUP BY customer.first_name, customer.last_name
                    having Sum(film.length) > {sum_min}
                    ORDER BY sum , customer.last_name ,customer.first_name
                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else:
        return None 

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(name,(str)):
        query = f'''
        select
                category.name as category,
                Avg(film.length) AS "avg",
                Sum(film.length) AS "sum",
                Min(film.length) AS "min",
                Max(film.length) AS "max"
        from film
        Inner join film_category ON film.film_id=film_category.film_id
        Inner join category ON film_category.category_id=category.category_id
        Where category.name IN ('{name}')
        Group by category.name;
                '''
        df = pd.read_sql(query, con=connection_sqlalchemy)
        return df
    else:
        return None 
