import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, countDistinct, substring
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
#from SparkPractical1.py import * as functions1

sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

SMALL_DATASET_PATH = 'ml-latest-small'
DATASET_PATH = 'ml-latest'

def get_movies_genres_watched(ratings, tags, movies):
    """ Combines ratings, tags, movies by movieId. 
    Separate genres column into one row per genre."""

    # Combine rows from ratings and tags tables to get all films user interacted with
    # Drop duplicate movieId userId pairs, only care about tying user to the movie
    # Then combine with movies tables to get the genres for each movie
    # Explode genre column so one row per genre
    return tags.dropDuplicates(("userId", "movieId"))\
               .join(ratings.dropDuplicates(("userId", "movieId")), on=['userId', "movieId"], how ='fullouter')\
               .join(movies, on=["movieId"], how='leftouter')\
               .select(col("rating"),col("userId"), col("movieId"), col("title"), split(col("genres"), "[|]").alias("genresArray"))\
               .select(col("rating"),col("userId"), col("movieId"), col("title"), explode("genresArray").alias("Genre"))

def enterId(message):
    
    try:
        user_id = int(input('Enter the ' + message + ' ID (>0):').strip())
        
        if user_id < 0:            
            raise Exception( message +   " ID should be larger than or equal to 1!")
            
        return user_id
    
    except ValueError:
            print("You should enter only a number larger than 0!")
            

def stripList(lis):
    strippedList = []
    for item in lis:
        strippedList.append(item.strip())
    return strippedList

            
def enterUsersIds():
    usersIdsString = input("Enter a list of users IDs separated by comma (,). Note: IDs should be larger than 0!:").strip()
    usersIdsListOfStrings = usersIdsString.split(",")
    usersIdsListOfIntegers = []
    for user_id in usersIdsListOfStrings:
        try:
            user_idInt = int(user_id.strip())
            usersIdsListOfIntegers.append(user_idInt)
        except ValueError:
            print("Some inputs are not appropriate!")           
    
    print("HERES THE IDS")
    print(usersIdsListOfIntegers)
    return usersIdsListOfIntegers

def enterGenres():
    genresStr = input("Enter Genres list separated by comma (,):").strip()
    genresList = genresStr.split(",")
    genresList = stripList(genresList)
    
    return genresList

def load_data(dataPath):
    #Load the movies and rating CSV files
    movies = sqlContest.read.csv(dataPath + '/movies.csv', header=True)
    ratings = sqlContest.read.csv(dataPath + '/ratings.csv', header=True)
    tags = sqlContest.read.csv(dataPath + '/tags.csv', header=True)
    
    return ratings, movies, tags


def clean_data(ratings, movies):
    
    ##Convert the timestamp to date 
    # Drop NA values
    # Convert data Type of columns intro integer and float
    clean_ratings = ratings.na.drop()\
    .withColumn('RatingDate', func.date_format(ratings.timestamp.
                                                           cast(dataType= typ.LongType()).
                                                           cast(dataType= typ.TimestampType()),
                                                           "yyyy-MM-dd"))\
    .withColumn("rating", ratings.rating.cast(dataType = typ.FloatType()))\
    .withColumn("userId", ratings.userId.cast(dataType = typ.IntegerType()))\
    .withColumn("movieId", ratings.movieId.cast(dataType = typ.IntegerType()))
    
    # Drop NA values
    # Convert data Type of columns into integer
    
    clean_movies = movies.na.drop()\
    .withColumn("movieId", movies.movieId.cast(dataType = typ.IntegerType()))\
    .withColumn("production_year", substring(col("title"), -5, 4))
    
    #Split the genres column into rows based on the number of the separator | 

    movies_geners =movies.withColumn("Genr",explode(split("genres","[|]")))

    ## return the clean datafrmes (ratings, movies, and movies genres)
    
    return clean_ratings, clean_movies, movies_geners

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
                userId = enterId("user ")
                #visualise_user_ratings_genres(userId, movies_genres_watched)

            elif choice == 3:   # Compare user's watch count of genres
                userId = enterId("user ")
                #visualise_times_user_watched_genres(userId, movies_genres_watched)


            elif choice == 4:   # Compare which genres most users' favourites
                pass
                #visualise_favourite_genres(movies_genres_watched)

            elif choice == 5:   # compare how many users have watched each genre
                pass
                #visualise_times_watched_genres_all_users(movies_genres_watched)
            
            elif choice == 6:   # Distribution of ratings for selected genre
                genre = input("Please enter the genre name:").strip()
                #visualise_distribution_ratings_genre_avg_user(movies_genres_watched, genre)
            
            elif choice == 7:   # Distribution of ratings for selected movie
                movieTitle = input("Please enter the movie title:").strip()
                #visualise_distribution_ratings_movie_avg_user(movies_genres_watched, movieTitle)

            elif choice == 8:
                break
            
        except ValueError:
            print("You should enter only a number between 1 and 8!")



def controlPanel():
    
    #Select the dataset
    dataPath = SMALL_DATASET_PATH
    
    #Load the datasets
    ratings, movies, tags = load_data(dataPath)
    
    #Clean the dataset
    ratings_clean, movies_clean, movies_genres = clean_data(ratings, movies)
    #ratings.show()
    #movies.show()
    
    # Transform tables
    movies_genres_watched =  get_movies_genres_watched(ratings, tags, movies)

    #presist the data in memory
    movies.persist()
    ratings.persist()
    movies_genres_watched.persist()

    
    #Set the number of records to show
    N = 7
    
    #Interact with the dataset
    while True:
        print("Choices:")
        print("1- Search user | 2-Search Users | 3-Search Genre | 4-Search Genres")
        print("5- Count movies watched by ID | 6- Count genres watched by ID | 7- Search movies watched by IDs")
        print("8- Summarise movie average rating and watch count by id| 9- Summarise movie average rating and watch count by title")
        print("10- Search movies in genres| 11- List top rating movies | 12- search movies by year")
        print("13- Show user's favourite genres | 14- compare two users' taste | 15- Show visualizations ")
        print("16- Recommend Movies for user | 17- Exit")
        
        try:
            choice = int(input('Enter your choice number from the list above:').strip())
            
            if choice == 1:
                user_id = enterId("user ")
                #search_user_by_id(user_id, ratings, movies).show()

            elif choice == 2:   
                usersIds = enterUsersIds()
                #search_users_by_ids(usersIds, ratings, movies).show()

            elif choice == 3:   # Search for movies of specified genre
                genre = input("Please enter the genre name:").strip()
                #search_genre(genre, movies).show()

            elif choice == 4:   # Search for movies of specified genres
                genres = enterGenres()
                #search_genres(genres, movies_genres, movies).show()

            elif choice == 5:   # Count number of movies user has watched
                userId = enterId("user ")
                #count_watched_movies(movies_genres_watched, userId).show()

            elif choice == 6:   # Count number of genres user has watched
                userId = enterId("user ")
                #count_watched_genres(movies_genres_watched, userId).show()

            elif choice == 7:   # Get titles of movies watched by users
                userIds = enterUsersIds()
                #get_titles_of_movies_watched_ids(movies_genres_watched, userIds).show()
            
            elif choice == 8:   # Get watchcount and avg rating of movie by its id
                movieId = enterId("movie ")
                #summarise_avg_rating_watchcount_movie(search_movie_by_id(movieId, movies_genres_watched)).show()
            
            elif choice == 9:   # Get watchcount and avg rating of movie by its title
                title = input("Please enter the movie title:").strip()
                #summarise_avg_rating_watchcount_movie(search_movie_by_title(title, movies_genres_watched)).show()

            elif choice == 10:  # Search for movies in given genre
                genres = enterGenres()
                #search_movies_in_genres(movies_genres_watched, genres).show(100)

            elif choice == 11:  # Show top n rated movies 
                n = int(input("please enter the number of top watching movies to show:").strip())
                #list_top_watched(ratings, movies).show(n)
            
            
            elif choice == 12:  # Search movie by year
                year = input("Please enter a year between 1750 - 2018")
                #search_movies_by_year(year, movies).show()

            elif choice == 13:  # Find favourite genre of user
                user_id = enterId("user ")
                #search_favourite_genre(user_id, movies_genres_watched).show()

            elif choice == 14:  # Compare user tastes
                user1_Id = enterId("First user ")
                user2_Id = enterId("Second user ")
                #compareTastes(user1_Id, user2_Id, movies_genres, ratings_clean, movies_clean).show()

            elif choice == 15:  # Visualise
                pass
                #select_visualisations(movies_genres_watched)

            elif choice == 16:
                print("Recommend movies")

            elif choice == 17:
                break
            
        except ValueError:
            print("You should enter only a number between 1 and 17!")



controlPanel()