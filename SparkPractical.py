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
def count_watched_movies():
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
    movies_genres_split = movies_watched_genres.select(movies_watched_genres.userId, split(col("genres"), "[|]").alias("genresArray"))
    
    # Explode genres array into rows
    movies_watched_explode_genres = movies_genres_split.select(movies_genres_split.userId, explode("genresArray").alias("Genre"))
    
    # Group by userId, counting distinct number of genres watched
    genres_watched = movies_watched_explode_genres.groupBy("userId")\
                                                  .agg(func.countDistinct("Genre").alias("genresWatched"))\
                                                  .sort(func.desc("genresWatched"))
    genres_watched.show()

    return genres_watched

def count_watched_movies_user(id):
    movies_watched_count.where("userId = " + id).show()

def count_watched_genres_user(id):
    genres_watched_count.where("userId = " + id).show()

def get_titles_movies_watched_user(id):
    # Group by userId and movieId
    tagged_count = tags.groupBy("userId", "movieId").agg(func.countDistinct("tag").alias("taggedCount"))
    rated_count = ratings.groupBy("userId", "movieId").agg(func.countDistinct("rating").alias("ratedCount"))

    # Combine rows from ratings and tags tables
    movies_watched = tagged_count.join(rated_count, on=['userId', "movieId"], how ='fullouter')

    # Combine with movies table, which contains movie title
    movies_watched_titles = movies_watched.join(movies, on=["movieId"], how='leftouter')
    
    # View titles of movies watched, filtered by provided userId 
    movies_watched_titles = movies_watched_titles.select(movies_watched_titles.title)\
                            .where("userId = " + str(id))\
                            .show(200)

def get_number_users_watched_movie(movieId):
    # Group by userId and movieId
    tagged_count = tags.groupBy("userId", "movieId").agg(func.countDistinct("tag").alias("taggedCount"))
    rated_count = ratings.groupBy("userId", "movieId").agg(func.countDistinct("rating").alias("ratedCount"))

    # Combine rows from ratings and tags tables
    movies_watched = tagged_count.join(rated_count, on=['userId', "movieId"], how ='fullouter')

    # Combine with movies table, which contains movie title
    movies_watched_titles = movies_watched.join(movies, on=["movieId"], how='leftouter')

    # Get count of unique users by movieId
    unique_users_per_movie = movies_watched_titles.groupBy("movieId", "title")\
                            .agg(func.countDistinct("userId").alias("watchCount"))\
                            .sort(func.desc("watchCount"))
    unique_users_per_movie.show(50)

    # View titles of movies watched, filtered by provided userId 
    movies_watched_titles = unique_users_per_movie\
                            .filter(col("movieId") == str(movieId))\
                            .select(unique_users_per_movie.title, unique_users_per_movie.watchCount)\
                            .show()

def search_movies_by_genre(genre):
    # Split genres into array column
    movies_genres_split = movies.select(movies.movieId,movies.title, split(col("genres"), "[|]").alias("genresArray"))
    movies_genres_split.show()
    
    # Explode genres array into rows
    movies_watched_explode_genres = movies_genres_split.select(movies_genres_split.movieId, movies_genres_split.title, explode("genresArray").alias("genre"))
    movies_watched_explode_genres.show()

    # Select movies of particular genre 
    movies_watched_explode_genres.filter(col("genre") == genre)\
                                 .show()

def genres_ratings_avg_per_user():
    # combine tags with movies, linking ratings with genres
    movies_ratings = ratings.join(movies, on=["movieId"], how="leftouter")
    
    # Explode genres into rows
    movies_ratings_exploded_genre = movies_ratings\
                                    .select(col("userId"), col("title"), col("rating"), split(col("genres"), "[|]").alias("genresArray"))\
                                    .select(col("userId"), col("title"), col("rating"), explode("genresArray").alias("genre"))
    
    # Group by userId, genre -> avg rating
    genres_rating_avg = movies_ratings_exploded_genre.groupBy("userId", "genre")\
                            .agg(func.mean("rating").alias("AvgRating"))\
                            .sort(func.desc("userId"))
    genres_rating_avg.show(30)
    return genres_rating_avg


def genres_ratings_count_per_user():
    # combine tags with movies, linking ratings with genres
    movies_ratings = ratings.join(movies, on=["movieId"], how="leftouter")
    
    # Explode genres into rows
    movies_ratings_exploded_genre = movies_ratings\
                                .select(col("userId"), col("title"), col("rating"), split(col("genres"), "[|]").alias("genresArray"))\
                                .select(col("userId"), col("title"), col("rating"), explode("genresArray").alias("genre"))
    
    # Group by userId, genre -> avg rating
    genres_rating_count = movies_ratings_exploded_genre.groupBy("userId", "genre")\
                            .agg(func.count("rating").alias("ratingsCount"))\
                            .sort(func.desc("userId"))
    genres_rating_count.show(30)
    return genres_rating_count

# Combine users' avg rating per genre with the count of how many ratings they've made per genre
def genres_rated_count_avg_per_user():
    genres_rated_count_avg_per_user = genres_ratings_count_per_user().join(genres_ratings_avg_per_user(), on=["userId","genre"], how="leftouter")\
                                     .filter(col("userId") == 1)\
                                     .sort(col("AvgRating").desc())
    genres_rated_count_avg_per_user.show()

    # Only consider genres which have a representative number of ratings
    genres_rated_count_avg_per_user.filter(col("ratingsCount") > 10)\
                                   .show()




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

#get_titles_movies_watched_user(2)
#get_number_users_watched_movie(1)
#search_movies_by_genre("Action")
#genres_rating_count_per_user(2)
#genres_watch_count_per_user(2)
genres_rated_count_avg_per_user()