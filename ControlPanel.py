
import findspark
spark = findspark.init()

import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, countDistinct, substring, rtrim
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import SparkPractical1 as funcs1
import HelperFunctions as helperFuncs
import SparkPractical_module2 as module2
sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

SMALL_DATASET_PATH = 'ml-latest-small'
DATASET_PATH = 'ml-latest'




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
    .withColumn('watching_date', func.date_format(ratings.timestamp.
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
    .withColumn("production_year", substring(rtrim(col("title")), -5, 4))
    
    #Split the genres column into rows based on the number of the separator | 

    movies_geners =movies.withColumn("Genr",explode(split("genres","[|]")))

    ## return the clean datafrmes (ratings, movies, and movies genres)
    
    return clean_ratings, clean_movies, movies_geners





def controlPanel():
    
    #Select the dataset
    data_path = SMALL_DATASET_PATH 
    
    data_select = int(input("Please enter the number (1) to select the small dataset or (2) to use the big dataset:").strip())
    if data_select == 2:
        data_path = DATASET_PATH
    
    #Load the datasets
    ratings, movies, tags = load_data(data_path)
    
    #Clean the dataset
    ratings, movies, movies_genres = clean_data(ratings, movies)
    #ratings.show()
    #movies.show()
    
    # Transform tables
    movies_genres_watched =  helperFuncs.get_movies_genres_watched(ratings, tags, movies)

    #presist the data in memory
    movies.persist()
    ratings.persist()
    movies_genres_watched.persist()

    ## Prepare the recommendation system
    if data_select != 2:
        als_model = module2.train_als_model(ratings)
        print("Recommendations system is ready now....")
    else:
        print("Recommendation can work only with the small data because of the limited resources!")
    #Set the number of records to show
    N = 7
    
    #Interact with the dataset
    while True:
        print("Choices:")
        print("1- Search user | 2-Search Users | 3-Search Genre | 4-Search Genres")
        print("5- Count movies watched by ID | 6- Count genres watched by ID | 7- Search movies watched by IDs")
        print("8- Summarise movie average rating and watch count by id| 9- Summarise movie average rating and watch count by title")
        print("10- Search movies in genres| 11- List top wathed movies | 12- List most rated Movies | 13- search movies by year")
        print("14- Show user's favourite genres | 15- compare two users' taste | 16- Show visualizations ")
        print("17- Recommend Movies for user | 18- Print Data Sumary | 19- Exit")
        
        try:
            choice = int(input('Enter your choice number from the list above:').strip())
            
            if choice == 1: ## Search user by ID (Ashraf)
                user_id = helperFuncs.enterId("user ")
                module2.show_user_details(user_id, movies_genres, ratings, movies) 

            elif choice == 2: ## Search many Users by ID (Ashraf)
                users_ids = helperFuncs.enterUsersIds()
                for user_id in users_ids:
                    module2.show_user_details(user_id, movies_genres, ratings, movies)

            elif choice == 3:   # Search for movies of specified genre(Ashraf)
                genre = input("Please enter the genre name:").strip()
                module2.show_genre_details(genre, movies)

            elif choice == 4:   # Search for movies of specified genres (Ashraf)
                genres = helperFuncs.enterGenres()
                for genre in genres:
                    module2.show_genre_details(genre, movies)

            elif choice == 5:   # Count number of movies user has watched (Karl)
                userId = helperFuncs.enterId("user ")
                funcs1.count_watched_movies(movies_genres_watched, userId).show()

            elif choice == 6:   # Count number of genres user has watched (Karl)
                userId = helperFuncs.enterId("user ")
                funcs1.count_watched_genres(movies_genres_watched, userId).show()

            elif choice == 7:   # Get titles of movies watched by users (Karl)
                userIds = helperFuncs.enterUsersIds()
                funcs1.get_titles_of_movies_watched_ids(movies_genres_watched, userIds).show()
            
            elif choice == 8:   # Get watchcount and avg rating of movie by its id (Karl)
                movieId = helperFuncs.enterId("movie ")
                funcs1.summarise_avg_rating_watchcount_movie(helperFuncs.search_movie_by_id(movieId, movies_genres_watched)).show()
            
            elif choice == 9:   # Get watchcount and avg rating of movie by its title (Karl)
                title = input("Please enter the movie title:").strip()
                funcs1.summarise_avg_rating_watchcount_movie(helperFuncs.search_movie_by_title(title, movies_genres_watched)).show()

            elif choice == 10:  # Search for movies in given genre (Karl)
                genres = helperFuncs.enterGenres()
                funcs1.search_movies_in_genres(movies_genres_watched, genres).show(100)

            elif choice == 11:  # Show top n rated movies (Ashraf)
                n = int(input("please enter the number of top wacthed movies to show:").strip())
                module2.list_top_watched(ratings, movies).show(n)
            
            elif choice == 12:  # Search movie by year(Ashraf)
                n = input("Pplease enter the number of top rated movies to show:")
                module2.list_top_rated(ratings, movies).show(n)
            
            
            elif choice == 13:  # Search movie by year(Ashraf)
                year = input("Please enter a year between 1750 - 2018:  ")
                module2.search_movies_by_year(year, movies).show()

            elif choice == 14:  # Find favourite genre of user (Karl)
                user_id = helperFuncs.enterId("user ")
                funcs1.search_favourite_genre(user_id, movies_genres_watched).show()

            elif choice == 15:  # Compare user tastes (Ashraf)
                user1_Id = helperFuncs.enterId("First user ")
                user2_Id = helperFuncs.enterId("Second user ")
                summary, u1_diff, u2_diff, u1_and_u2_like, u1_and_u2_dislike, u1_like_u2_dislike, u1_dislike_u2_like = ( 
                module2.compareTastes(user1_Id, user2_Id, movies_genres, ratings, movies))
                
                print("Comparing the Tastes of user:" + 
                     str(user1_Id) + " , and user:" + str(user2_Id) + " :" ) 
                print("Both users are watching the following genres, the table below show compare there watching times and ratings:")
                summary.show()
                print("The next Genres are favourite for both user:" + 
                     str(user1_Id) + " , and user:" + str(user2_Id) + " :" )
                u1_and_u2_like.show()
                print("The next Genres are unfavourite for both user:" + 
                     str(user1_Id) + " , and user:" + str(user2_Id) + " :" )
                u1_and_u2_dislike.show()
                print("The next Genres are preffered by user:" + 
                     str(user1_Id) + " , but disliked by user:" + str(user2_Id) + " :" )
                u1_like_u2_dislike.show()
                print("The next Genres are preffered by user:" + 
                     str(user1_Id) + " , but disliked by user:" + str(user2_Id) + " :" )
                u1_dislike_u2_like.show()
                
                print("User " + str(user1_Id) + " watched " + str(u1_diff.count()) + " Genres that User " + str(user2_Id) + " have not watched :")
                u1_diff.show()
                print("User " + str(user2_Id) + " watched " + str(u2_diff.count()) + " Genres that User " + str(user1_Id) + " have not watched :")
                u2_diff.show()

            elif choice == 16:  # Visualise (Karl)
                funcs1.select_visualisations(movies_genres_watched)

            elif choice == 17: ## Recommend movies (Ashraf)
                user_id = module2.enterId("user ")
                print("The top 10 recommended movies for this user are:")
                rec_movies = module2.recommend_movies(als_model, user_id, 10, movies)
                rec_movies.show()
            elif choice == 18: ## Summarize Data (Ashraf)
                module2.summarize_all_data(ratings, movies, movies_genres)

            elif choice == 19:
                break
            
        except ValueError:
            print("You should enter only a number between 1 and 17!")



controlPanel()
