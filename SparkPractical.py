#!/usr/bin/env python
# coding: utf-8

# In[58]:


import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ

sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

mv1 = sqlContest.read.csv('ml-latest-small/movies.csv', header=True)
movies = sqlContest.read.csv('ml-latest-small/movies.csv', header=True)
links = sqlContest.read.csv('ml-latest-small/links.csv', header=True)
ratings = sqlContest.read.csv('ml-latest-small/ratings.csv', header=True)
tags = sqlContest.read.csv('ml-latest-small/tags.csv', header=True)
#print(mv1.first())
##Convert the timestamp to date

ratings = ratings.withColumn('RatingDate', func.date_format(ratings.timestamp.
                                                       cast(dataType= typ.LongType()).
                                                       cast(dataType= typ.TimestampType()),
                                                       "yyyy-MM-dd"))

tags = tags.withColumn('tagDate', func.date_format(tags.timestamp.
                                                       cast(dataType= typ.LongType()).
                                                       cast(dataType= typ.TimestampType()),
                                                   "yyyy-MM-dd"))

## Add geners columns using binary encoding for each category
genersLabels = ["Action", "Adventure", "Animation", "Children's", "Comedy"
                "Crime", "Documentary","Drama", "Fantasy", "Film-Noir",
                "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
                "Thriller", "War", "Western"]

movies1 = movies
for label in genersLabels:
    movies = movies.withColumn(label, movies["genres"].like("%" + label +"%"))

geners = movies.select("genres").distinct()

geners.show()
#print(geners.count())
##Persist the data in memory
movies.persist()
links.persist()
ratings.persist()
tags.persist()

#Covert timestamp seconds to date


#print(ratings.count())
## Basic Requirements

def searchUserById(id_):
    filtered = ratings.where("userId =" + str(id_))
    filtered.show()
    filtered = filtered.join(movies, on=['movieId'], how ='inner')
    filtered.show()
    return filtered
def searchUsersById(ids):
    
    return 

def searchMovieById(id):
    filteredMovies = movies.where("movieId = " + str(id))
    filteredMovies.show()
    return 

def searchMovieByTitle(title):
    filteredMovies = movies.where("title like \'%" + str(title) + "%\'")
    filteredMovies.show()
    return 

def searchGenre(gen):
    filteredMovies = movies.where("genres like \'%" + str(gen) + "%\'")
    filteredMovies.show()
    return filteredMovies

def searchGenres(gens):
    
    return

def searchMoviesByYear(year):
    
    return

def listTopNRated(n):
    
    return

def listTopNWatched(n):
    
    return


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

searchMovieById(2)
searchGenre("Action")
searchMovieByTitle("Jum")
searchUserById(5)
searchUserById(22)


# In[ ]:





# In[ ]:




