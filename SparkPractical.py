#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 21:26:38 2021

@author: ashraf sinan
"""

import pyspark 
from pyspark.sql import SQLContext
sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

mv1 = sqlContest.read.csv('ml-latest-small/movies.csv', header=True)
movies = sqlContest.read.csv('ml-latest-small/movies.csv', header=True)
links = sqlContest.read.csv('ml-latest-small/links.csv', header=True)
ratings = sqlContest.read.csv('ml-latest-small/ratings.csv', header=True)
tags = sqlContest.read.csv('ml-latest-small/tags.csv', header=True)
#print(mv1.first())

##Persist the data in memory
movies.persist()
links.persist()
ratings.persist()
tags.persist()

#print(ratings.count())
## Basic Requirements

def searchUserById(id_):
    filtered = ratings.where("userId =" + str(id_))
    print("The user with id:" + str(id_) + "  Have watched " + str(filtered.count()) +" MOVIES")
    

def searchUsersById(ids):
    
    return 0

def searchMovieById(id):
    
    return 0

def searchMovieByTitle(title):
    
    return 0

def searchGenre(gen):
    
    return 0

def searchGenres(gens):
    
    return 0

def searchMoviesByYear(year):
    
    return 0

def listTopNRated(n):
    
    return 0

def listTopNWatched(n):
    
    return 0


## Intermediate Requirements:

def findFavouriteGenre(userIds):
    return 0

def compareTastes(userId1, userId2):
    
    return 0

## Advanced Requirements:

def clusterUsersByTaste():
    
    return 0

def visualizeTheDataSet():
    
    return 0

def recommendMovie(userId):
    
    return 0


searchUserById(2)
searchUserById(22)
