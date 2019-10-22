#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
TheMoviePredictor script
Author: Arnaud de Mouhy <arnaud@admds.net>
"""

import mysql.connector
import sys
import argparse
import csv
import bs4
import requests

def connect_to_database():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnect_database(cnx):
    cnx.close()

def create_cursor(cnx):
    return cnx.cursor(dictionary=True)

def close_cursor(cursor):    
    cursor.close()

def find_query(table, id):
    return (f"SELECT * FROM `{table}` WHERE id = {id}")

def find_all_query(table):
    return ("SELECT * FROM {}".format(table))

def insert_people_query(lastname, firstname):
    return (f"""INSERT INTO people (firstname, lastname) VALUES("{lastname}", "{firstname}")""")

def insert_movie_query(title, duration, original_title, release_date):
    return (f"""INSERT INTO movies (title, duration, original_title, release_date) VALUES("{title}", {duration},"{original_title}", "{release_date}")""")
 
def find(table, id):
    cnx = connect_to_database()
    cursor = create_cursor(cnx)
    query = find_query(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    close_cursor(cursor)
    disconnect_database(cnx)
    return results

def find_all(table):
    cnx = connect_to_database()
    cursor = create_cursor(cnx)
    cursor.execute(find_all_query(table))
    results = cursor.fetchall()
    close_cursor(cursor)
    disconnect_database(cnx)
    return results

def insert_people(firstname, lastname):
    cnx = connect_to_database()
    cursor = create_cursor(cnx)
    cursor.execute(insert_people_query(firstname, lastname))
    people_id = cursor.lastrowid
    cnx.commit()
    close_cursor(cursor)
    disconnect_database(cnx)
    return people_id

def insert_movie(title, duration, original_title, release_date):
    cnx = connect_to_database()
    cursor = create_cursor(cnx)
    cursor.execute(insert_movie_query(title, duration, original_title, release_date))
    people_id = cursor.lastrowid
    cnx.commit()
    close_cursor(cursor)
    disconnect_database(cnx)
    return people_id

def print_person(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def print_movie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=['people', 'movies'], help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entitÃ©es du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exportÃ©')

find_parser = action_subparser.add_parser('find', help='Trouve une entitÃ© selon un paramÃ¨tre')
find_parser.add_argument('id' , help='Identifant Ã  rechercher')

insert_parser = action_subparser.add_parser('insert', help='Insert une entitées')
know_args = parser._parse_known_args()[0]

if know_args.context == "people":

insert_parser.add_argument('--firstname' , help='Prénom à insérer', required=True)
insert_parser.add_argument('--lastname' , help='Nom à insérer', required=True)

if know_args.context == "movies":
insert_parser.add_argument('--title' , help='Titre à insérer', required=True)
insert_parser.add_argument('--duration' , help='Durée à insérer', type=int, required=True)
insert_parser.add_argument('--original-title' , help='Titre original à insérer', required=True)
insert_parser.add_argument('--rating' , help='Rating  à insérer', choices=('TP', '-12', '-16'), required=True)
insert_parser.add_argument('--release_date' , help='Date de sortie à insérer', required=True)

import_parser = action_subparser.add_parser('import', help='Importer un fichier csv')
import_parser.add_argument('--file' , help='chemin vers le fichier à importer', required=True)

scrap_parser = action_subparser.add_parser('scrap', help='Scrap page wikipédia')
scrap_parser.add_argument('page' , help='Page à scraper')


args = parser.parse_args()

if args.context == "people":
    if args.action == "list":
        people = find_all("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                print_person(person)
    if args.action == "find":
        people_id = args.id
        people = find("people", people_id)
        for person in people:
            print_person(person)

    if args.action == "insert":
        print(f"Insertion d'une nouvelle personne {args.firstname} {args.lastname}")
        people_id = insert_people(firstname=args.firstname, lastname=args.lastname)
        print(f"l'entrée #{people_id} - {args.firstname} {args.lastname} a bien été ajouté")
        

if args.context == "movies":
    if args.action == "list":  
        movies = find_all("movies")
        for movie in movies:
            print_movie(movie)

    if args.action == "find":
        movie_id = args.id         
        movies = find("movies", movie_id)
        for movie in movies:
            print_movie(movie)

    if args.action == "insert":
        print(f"Insertion d'un nouveau film : {args.title}") 
        movie_id = insert_movie(title=args.title, duration=args.duration, original_title=args.original_title, release_date=args.release_date)
        print(f"l'entrée #{movie_id} - {args.title} a bien été ajouté")

    if args.action == "import":
        
        with open(args.file, 'r', encoding='utf-8', newline='\n') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=',')
            for row in csvreader:
                movie_id = insert_movie(row['title'], row['duration'],  row['original_title'], row['release_date'])
                print(f"l'entrée #{movie_id} - {row['title']} a bien été ajouté")

    if args.action == "scrap":
        page = args.page
        
        page = requests.get(page)
        if page.status_code == 200:
        
        
            soup = bs4.BeautifulSoup(page.text, 'html.parser')
            li_box = soup.find_all("li")
        
            find_title = soup.find_all(id="firstHeading")
            find_key = soup.find_all('th')
            find_cell = soup.find_all('td')
            film = {}
            
            for i,j in zip(find_key, find_cell):
                key = i.get_text()
                value = j.get_text()
                film[key] = value

           
            film['Titre'] = find_title[0].get_text()
            if find_key[0] != 'Titre original':
               film['Titre original'] = film['Titre']

            print("Title : {}, Original Title : {}, Duration : {} , Release on : {}".format(film['Titre'], film['Titre original'],  film['Durée'],  film['Sortie']))
    

            

        