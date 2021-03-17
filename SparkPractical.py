#!/usr/bin/env python
# coding: utf-8

# In[58]:


import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc

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

## Add genres columns using binary encoding for each category
genresLabels = ["Action", "Adventure", "Animation", "Children's", "Comedy"
                "Crime", "Documentary","Drama", "Fantasy", "Film-Noir",
                "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
                "Thriller", "War", "Western"]

movies1 = movies
for label in genresLabels:
    movies = movies.withColumn(label, movies["genres"].like("%" + label +"%"))

#genres = movies.select("genres").distinct()

#genres.show()
#print(genres.count())
##Persist the data in memory
movies.persist()
links.persist()
ratings.persist()
tags.persist()

movies.show()
tags.show()
ratings.show()
links.show()



#Covert timestamp seconds to date


#print(ratings.count())
## Basic Requirements

# Search user by id, show the number of movies that he/she has watched
def searchUserByIdMovies(id):
    # Get distinct userID in ratings DF
    # Count distinct movie IDs for userID
    
    # Get distinct userID in tags DF
    # Count distinct movie IDs for userID

    # Combine counts from above two 

    return filtered

# Count the number of movies each userId has watched
def countWatchedMovies():
    # Group by userId and movieId
    tagged_count = tags.groupBy("userId", "movieId").agg(func.countDistinct("tag").alias("taggedCount"))
    rated_count = ratings.groupBy("userId", "movieId").agg(func.countDistinct("rating").alias("ratedCount"))

    # Combine rows from ratings and tags tables
    rated_tagged_joined = tagged_count.join(rated_count, on=['userId', "movieId"], how ='fullouter')

    # Count unique number of movieIds per userId
    movies_watched_count = rated_tagged_joined.groupBy("userId").agg(func.countDistinct("movieId").alias("moviesWatched"))
    
    return movies_watched_count

# Count the number of genres each userId has watched
def count_watched_genres():
    # Group by userId and movieId
    tagged_count = tags.groupBy("userId", "movieId").agg(func.countDistinct("tag").alias("taggedCount"))
    rated_count = ratings.groupBy("userId", "movieId").agg(func.countDistinct("rating").alias("ratedCount"))

    # Combine rows from ratings and tags tables
    movies_watched = tagged_count.join(rated_count, on=['userId', "movieId"], how ='fullouter')

    # Combine with genres
    movies_watched_genres = movies_watched.join(movies, on=["movieId"], how='leftouter')
    
    # Split genres into array column
    movies_watches_split_genres = movies_watched_genres.select(movies_watched_genres.userId, split(col("genres"), "[|]").alias("genresArray"))
    movies_watches_split_genres.show()
    
    # Explode genres array into rows
    movies_watched_explode_genres = movies_watches_split_genres.select(movies_watches_split_genres.userId, explode("genresArray").alias("Genre"))
    movies_watched_explode_genres.show()

    # Group by userId, counting distinct number of genres watched
    genres_watched = movies_watched_explode_genres.groupBy("userId").agg(func.countDistinct("Genre").alias("genresWatched"))
    genres_watched.show()
    
    return genres_watched

def get_watch_count(id):
    movies_watched_count.where("userId = " + id).show()

def searchUsersById(ids):
    
    return 

def searchMovieById(id):
    return filtered

def searchMovieByTitle(title):
    
    return 

def searchGenre(gen):
    return

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


# Get count of movies watched
# movies_watched_count = countWatchedMovies()
# movies_watched_count.persist()
# get_watch_count("2")
count_watched_genres()


# searchMovieById(2)
# searchGenre("Action")
# searchMovieByTitle("Jum")
# searchUserById(5)
# searchUserById(22)
