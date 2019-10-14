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

def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnectDatabase(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(dictionary=True)

def closeCursor(cursor):    
    cursor.close()

def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def insertPeopleQuery(lastname, firstname):
    return (f"""INSERT INTO people (firstname, lastname) VALUES("{lastname}", "{firstname}")""")

def insertMovieQuery(title, duration, original_title, rating, release_date):
    return (f"""INSERT INTO movies (title, duration, original_title, rating, release_date) VALUES("{title}", {duration},"{original_title}", "{rating}", "{release_date}")""")
 
def find(table, id):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = findQuery(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findAll(table):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def insertPeople(firstname, lastname):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insertPeopleQuery(firstname, lastname))
    people_id = cursor.lastrowid
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return people_id

def insertMovie(title, duration, original_title, rating, release_date):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insertMovieQuery(title, duration, original_title, rating, release_date))
    people_id = cursor.lastrowid
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return people_id

def printPerson(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def printMovie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=['people', 'movies'], help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entitÃ©es du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exportÃ©')

find_parser = action_subparser.add_parser('find', help='Trouve une entitÃ© selon un paramÃ¨tre')
find_parser.add_argument('id' , help='Identifant Ã  rechercher')

insert_parser = action_subparser.add_parser('insert', help='Insert une entitÃ©es')
insert_parser.add_argument('--firstname' , help='Prénom à insérer')
insert_parser.add_argument('--lastname' , help='Nom à insérer')

insert_parser.add_argument('--title' , help='Titre à insérer')
insert_parser.add_argument('--duration' , help='Durée à insérer')
insert_parser.add_argument('--original-title' , help='Titre original à insérer')
insert_parser.add_argument('--rating' , help='Rating  à insérer')
insert_parser.add_argument('--release_date' , help='Date de sortie à insérer')

find_parser = action_subparser.add_parser('import', help='Import de fichier')
find_parser.add_argument('--file' , help='Fichier à importer')


args = parser.parse_args()

if args.context == "people":
    if args.action == "list":
        people = findAll("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                printPerson(person)
    if args.action == "find":
        peopleId = args.id
        people = find("people", peopleId)
        for person in people:
            printPerson(person)

    if args.action == "insert":
        peopleFirstname = args.firstname
        peopleLastname = args.lastname
        peopleId = insertPeople(peopleFirstname, peopleLastname)
        print(f"l'entrée #{peopleId} - {peopleFirstname} {peopleLastname} a bien été ajouté")
        

if args.context == "movies":
    if args.action == "list":  
        movies = findAll("movies")
        for movie in movies:
            printMovie(movie)
        movies = find("movies", movieId)
        for movie in movies:
            printMovie(movie)

    if args.action == "insert":
        title = args.title 
        duration = args.duration 
        original_title = args.original_title 
        rating = args.rating 
        release_date = args.release_date
        movieId = insertMovie(title, duration, original_title, rating, release_date)
        print(f"l'entrée #{movieId} - {title} a bien été ajouté")

    if args.action == "import":
        myfile = args.file
        with open(myfile, newline='') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=',')
            for row in csvreader:
                movieId = insertMovie(row['title'], row['duration'],  row['original_title'], row['rating'], row['release_date'])
                print(f"l'entrée #{movieId} - {row['title']} a bien été ajouté")
            

        