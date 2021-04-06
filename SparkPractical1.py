import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, countDistinct, substring
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import HelperFunctions as helperFuncs

sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

SMALL_DATASET_PATH = 'ml-latest-small'
DATASET_PATH = 'ml-latest'

def select_visualisations(movies_genres_watched):
    while True:
        print("Select visualisation:")
        print("1- All visualisations | 2-User's avg rating of each genre | 3- User's watch count of each genre")
        print("4- How many users have genres as favourite | 5- How many users watched each genre")
        print("6- Distribution of ratings for genre | 7- Distribution of ratings for movie")
        print("8- Exit")
        
        try:
            choice = int(input('Enter your choice number from the list above:').strip())
            
            if choice == 1: # Show all visualisations
                # TODO
                pass

            elif choice == 2:   # Compare user's avg ratings of genres
                userId = helperFuncs.enterId("user ")
                visualise_user_ratings_genres(userId, movies_genres_watched)

            elif choice == 3:   # Compare user's watch count of genres
                userId = helperFuncs.enterId("user ")
                visualise_times_user_watched_genres(userId, movies_genres_watched)


            elif choice == 4:   # Compare which genres most users' favourites
                visualise_favourite_genres(movies_genres_watched)

            elif choice == 5:   # compare how many users have watched each genre
                visualise_times_watched_genres_all_users(movies_genres_watched)
            
            elif choice == 6:   # Distribution of ratings for selected genre
                genre = helperFuncs.input("Please enter the genre name:").strip()
                visualise_distribution_ratings_genre_avg_user(movies_genres_watched, genre)
            
            elif choice == 7:   # Distribution of ratings for selected movie
                movieTitle = input("Please enter the movie title:").strip()
                visualise_distribution_ratings_movie_avg_user(movies_genres_watched, movieTitle)

            elif choice == 8:
                break
            
        except ValueError:
            print("You should enter only a number between 1 and 8!")

def count_watched_movies(movies_genres_watched, userId):
    """Search user by id, show the number of movies that he/she has watched"""
    # Count unique number of movieIds per userId
    return movies_genres_watched.groupBy("userId")\
                                .agg(func.countDistinct("movieId").alias("moviesWatched"))\
                                .filter(col("userId") == userId)

def count_watched_genres(movies_genres_watched, userId):
    """Search user by id, show the number of genres that he/she has watched"""
    # Group by userId, counting distinct number of genres watched
    return movies_genres_watched.groupBy("userId")\
                                .agg(func.countDistinct("Genre").alias("genresWatched"))\
                                .filter(col("userId") == userId)

def get_titles_of_movies_watched_ids(movies_genres_watched, ids):
    """Given a list of users, search all movies watched by each user"""
    return movies_genres_watched.select(col("title"))\
                                .filter(col("userId").isin(ids))\
                                .distinct()

def summarise_avg_rating_watchcount_movie(movies):
    """Search movie by id/title, show the average rating,
     the number of users that have watched the movie"""
    return movies.groupBy("movieId", "title")\
                 .agg(\
                    countDistinct("userId").alias("usersWatchedMovie"),\
                    avg("rating").alias("averageRating")\
                )


def search_movies_in_genres(movies, genres):
    """Show all movies in genres"""
    # Select movies of particular genre 
    return movies.filter(col("genre").isin(genres))\
                 .select(col("title"))

def summarize_movie(movies, ratings):
    ##summarize movies by getting the number of watchings and average rating
    ## and rename the aggregated fields
    summary = movies.join(ratings, on=['movieId'], how='inner')\
    .groupBy("movieId")\
    .agg(avg(col("rating")), count(col("movieId")))\
    .withColumnRenamed("avg(rating)", "Average_Rating")\
    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")
    
    return summary

def summarize_genres(movies_genres, ratings):
    
    ##summarize movies by getting the number of watchings and average rating
    ## and rename the aggregated fields
    summary = movies_genres.join(ratings, on='movieId', how='inner')\
    .groupBy("Genr")\
    .agg(count(col('movieId')), average(col('rating')))\
    .withColumnRenamed("avg(rating)", "Average_Rating")\
    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")
    
    return summary



## Intermediate Requirements:

def find_users_genres(user_id, movies_genres, ratings, movies):
    ##Search the movies that the user has wathced
    ##group them by the user and Genre and aggregate their 
    user_movies = search_user_by_id(user_id, ratings, movies)
    return user_movies.join(movies_genres, on=['movieId'], how='inner')\
                      .groupBy("userId","Genr")\
                      .agg(func.round(avg(col("rating")),2), func.count(col("userId")))\
                      .withColumnRenamed("round(avg(rating), 2)", "Average_Rating")\
                      .withColumnRenamed("count(user_id)", "wachings")\
                      .sort(desc("Average_Rating"))#.where("Average_Rating >=3 ")\

def get_genres_avg_rating_watchcount(movies_genres_watched):
    """Creates table of users' avg ratings and watchcount for each genre"""
    return movies_genres_watched\
                .groupBy("userId", "genre")\
                .agg(avg("rating").alias("avgRating"), countDistinct("movieId").alias("watchCount"))


def get_users_favourite_genre(movies_genres_watched):
    """Finds every users favourite genre
        Favourite genre is highest avg rated genre
        of genres watched more than avg num of times"""
    
    # Avg ratings and watchcount per genre per user
    genres_avg_rating_watchcount = get_genres_avg_rating_watchcount(movies_genres_watched)

    # Get each users avg watchcount across genres, which serves as cut-off for favourite genre
    cut_off = genres_avg_rating_watchcount.groupBy("userId")\
                                          .agg(avg(col("watchCount")).alias("avgWatchCount"))
    
    # Filter genres which fall below watch count cut off
    genres_avg_rating_watchcount = genres_avg_rating_watchcount\
                                        .join(cut_off, on=["userId"], how='left')\
                                        .filter(col("watchCount") >= col("avgWatchCount"))

    # Highest avg rating
    highest_rated = genres_avg_rating_watchcount\
                                       .groupBy("userId")\
                                       .agg(func.max("avgRating").alias("maxAvgRating"))

    # Get the favourites
    return  genres_avg_rating_watchcount.join(highest_rated, on=["userId"], how='left')\
                                       .filter(col("maxAvgRating") >= col("avgRating"))                                    

def search_favourite_genre(userId, movies_genres_watched):
    return    get_users_favourite_genre(movies_genres_watched)\
                    .filter(col("userId")== userId)\
                    .sort(desc(col("watchCount"))).limit(1)\
                    .select(col("userId"), col("genre").alias("favouriteGenre"))
    

## Advanced Requirements:
# Visualisations
def visualise_user_ratings_genres(userId, movies_genres_watched):
    """Bar chart comparing how user's avg rating per genre"""
    genre_ratings = get_genres_avg_rating_watchcount(movies_genres_watched)\
                        .filter(col("userId") == userId)\
                        .sort(desc("genre"))\
                        .toPandas()

    plt.figure(figsize=(24,5))
    plt.bar(genre_ratings["genre"].to_list(), genre_ratings["avgRating"].to_list())
    plt.xticks(rotation = -30)
    plt.title("How user with id " + str(userId) + " rated each genre")
    plt.ylabel("Rating")
    plt.ylim([0,5.5])
    plt.show()

def visualise_times_user_watched_genres(userId, movies_genres_watched):
    """Bar chart comparing how many times user has watched each genre"""
    count_watches_genres = get_genres_avg_rating_watchcount(movies_genres_watched)\
                        .filter(col("userId") == userId)\
                        .sort(desc("genre"))\
                        .toPandas()

    plt.figure(figsize=(24,5))
    plt.bar(count_watches_genres["genre"].to_list(), count_watches_genres["watchCount"].to_list())
    plt.xticks(rotation = -30)
    plt.title("How many movies user with id " + str(userId) + " has watched of each genre")
    plt.ylabel("Number of movies")
    plt.show()

def favourite_genres_count(movies_genres_watched):
    """Count how many users have genres as their favourite"""
    return get_users_favourite_genre(movies_genres_watched)\
                                 .groupBy("genre").agg(count("userId").alias("favouriteGenreCount"))\
                                 .sort(func.desc("favouriteGenreCount"))

def visualise_favourite_genres(movies_genres_watched):
    """Bar chart comparing how many users have genre as their favourite"""
    pandasDf = favourite_genres_count(movies_genres_watched).toPandas()
    plt.figure(figsize=(24,5))
    plt.bar(pandasDf["genre"].to_list(), pandasDf["favouriteGenreCount"].to_list())
    plt.xticks(rotation = -30)
    plt.title("How many users have genre as their favourite genre")
    plt.ylabel("Number of users")
    plt.show()



# How many users have watched movies within each genre
def visualise_times_watched_genres_all_users(movies_genres_watched):
    """Bar chart comparing how many users have watched films in each genre"""
    count_users_watched_genres = movies_genres_watched\
                        .groupBy(col("genre"))\
                        .agg(func.countDistinct("userId").alias("Count"))\
                        .sort(desc("Count"))\
                        .toPandas()

    plt.figure(figsize=(24,5))
    plt.bar(count_users_watched_genres["genre"].to_list(), count_users_watched_genres["Count"].to_list())
    plt.xticks(rotation = -60)
    plt.title("How many users have watched movies within each genre")
    plt.ylabel("Number of users")
    plt.show()


# Distribution of users' average rating for selected genre
def visualise_distribution_ratings_genre_avg_user(movies_genres_watched, genre):
    """Distribution of avg ratings for selected genre"""
    # Get the average ratings users have given per genre
    avg_user_ratings_genre_list = get_genres_avg_rating_watchcount(movies_genres_watched)\
                                               .filter(col("genre") ==  genre)\
                                               .toPandas()["avgRating"]
    
    # Plot into histogram
    fig, ax = plt.subplots()
    ax.hist(avg_user_ratings_genre_list)
    ax.set_title("Distribution of users' avg rating of the genre: " + genre)
    plt.show()

# Distribution of users' average rating for selected movie
def visualise_distribution_ratings_movie_avg_user(movies_genres_watched, title):
    """Distribution of avg ratings for selected genre"""
    # Get the average ratings users have given per genre
    avg_user_ratings_movie_list = movies_genres_watched\
                                            .groupBy("userId", "title")\
                                            .agg(avg("rating").alias("avgRating"))\
                                            .filter(col("title") ==  title)\
                                            .toPandas()["avgRating"]
    # Plot into histogram
    fig, ax = plt.subplots()
    ax.hist(avg_user_ratings_movie_list)
    ax.set_title("Distribution of users' avg rating of the movie: " + title)
    plt.show()