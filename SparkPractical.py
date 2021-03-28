import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, countDistinct, substring
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

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


def search_user_by_id(user_id, ratings, movies):
    
    ##filter the users by the ID
    filtered_users = ratings.where("userId =" + str(user_id))\
    .join(movies, on=['movieId'], how ='inner')
    
    return filtered_users

def search_users_by_ids(ids, ratings, movies):
    
    ## filtere users by a list of users ids using isin  
    filtered_users = ratings.filter(col("userId").isin(ids))\
    .join(movies, on=['movieId'], how ='inner')
                                  
    return filtered_users

def search_movie_by_id(movieId, movies):
    
    ##filter the movies by the movie id    
    filtered_movies = movies.where("movieId = " + str(movieId))
    
    return filtered_movies

def search_movie_by_title(title, movies):
    
    ##filter the movies by title
    filtered_movies = movies.where("title like \'%" + 
                                  str(title) + "%\'")
    
    return filtered_movies

def search_genre(genr, movies):
    #search one genre using like operator to accept incomplete names
    filtered_movies = movies.where("genres like \'%" + 
                                  str(genr) + "%\'")
    #filtered_movies.show()
    return filtered_movies

def search_genres(genrs, movies_geners, movies):
    
    ##filter all movies that are contained in the genres
    genrs_movies = movies_geners.filter(col("Genr").isin(genrs))
    ##join the filtered movies id with the movies dataframe
    filtered_movies = genrs_movies.join(movies, on=['movieId'], how = 'inner')
        
    return filtered_movies

def search_genres_separated(genrs, movies_geners, movies):
    
    ##Search each Genr, show how many movies it contains and show the first five movies
    for genr in genrs:
        genr_movies = search_genre(genr, movies)
        print("Genre (" + genr + ") contains " + str(genr_movies.count()))
        genr_movies.show(5)
    
    return


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

def search_movies_by_year(year, movies):
    
    ##filter movies by year
    filtered_movies = movies.where("title like \'%(" + 
                                   str(year) + ")\'")
    return filtered_movies

def list_top_rated(ratings, movies):
    
    ## group the movies by movie id and aggregate their rating
    ## then sort them by rating from heighst to lowest
    top_rated = ratings\
    .groupBy("movieId")\
    .agg(avg(col("rating")))\
    .withColumnRenamed("avg(rating)", "Average_Rating")\
    .join(movies, on=['movieId'], how='inner')\
    .sort(desc("Average_Rating"))
    
    
    return top_rated


def list_top_watched(ratings, movies):
    
    ## group the movies by movie id and aggregate their watchings
    ## then sort them by watchings from heighst to lowest
    
    most_popular = ratings\
    .groupBy("movieId")\
    .agg(func.count("movieId"))\
    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")\
    .join(movies, on=['movieId'], how='inner')\
    .sort(func.desc("Number_OF_Watchings"))
    
    return most_popular


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
    
def compareTastes(user1_Id, user2_Id, movies_genres, ratings, movies):
    u1 = find_users_genres(user1_Id, movies_genres, ratings, movies)
    u2 = find_users_genres(user2_Id, movies_genres, ratings, movies)
    u1 = u1.withColumnRenamed("wachings","User_" + str(user1_Id) +"_Watching_Times")\
    .withColumnRenamed("Average_Rating","User_" + str(user1_Id) +"_Rating")
    u2 = u2.withColumnRenamed("wachings","User_" + str(user2_Id) +"_Watching_Times")\
    .withColumnRenamed("Average_Rating","User_" + str(user2_Id) +"_Rating")    
    summary = u1.join(u2, on=['Genr'], how='inner')
    
    u1.select('Genr').subtract(u2.select('Genr')).join(u1, on=['Genr'], how='inner').show()
    u2.select('Genr').subtract(u1.select('Genr')).join(u2, on=['Genr'], how='inner').show()

    print("Comparing the Tastes of user:" +
         str(user1_Id) + " , and user:" + str(user2_Id) + " :" )
    
    return summary

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

def recommend_movies():
    
    ##Define ALS collaborative filtering model 
    ## Max iteration 10 with learning rate 0.2, specifing the 
    als_model = ALS(maxIter=10, regParam=0.2, userCol="user_id",
         itemCol='movieId', ratingCol= "rating", coldStartStrategy="drop")
    
    ##Split the data into training and testing data
    training_set, test_set = ratings.randomSplit([0.2,0.02])
    
    ##Train the model on the data
    als_trained_model = als_model.fit(training_set)
    
    ##Predict the test data set
    prediction = als_trained_model.transform(test)
   
    ##Evaluate the accuracy 
    evaluator = RegressionEvaluator(metricName="mse", 
                               labelCol = "rating", 
                               predictionCol="prediction")
    mse = evaluator.evaluate(prediction)
    print(alsModel.recommendForUserSubset([1,2,3]))
    print(mse)
    
    return 0

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
                visualise_user_ratings_genres(userId, movies_genres_watched)

            elif choice == 3:   # Compare user's watch count of genres
                userId = enterId("user ")
                visualise_times_user_watched_genres(userId, movies_genres_watched)


            elif choice == 4:   # Compare which genres most users' favourites
                visualise_favourite_genres(movies_genres_watched)

            elif choice == 5:   # compare how many users have watched each genre
                visualise_times_watched_genres_all_users(movies_genres_watched)
            
            elif choice == 6:   # Distribution of ratings for selected genre
                genre = input("Please enter the genre name:").strip()
                visualise_distribution_ratings_genre_avg_user(movies_genres_watched, genre)
            
            elif choice == 7:   # Distribution of ratings for selected movie
                movieTitle = input("Please enter the movie title:").strip()
                visualise_distribution_ratings_movie_avg_user(movies_genres_watched, movieTitle)

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
                search_user_by_id(user_id, ratings, movies).show()

            elif choice == 2:   
                usersIds = enterUsersIds()
                search_users_by_ids(usersIds, ratings, movies).show()

            elif choice == 3:   # Search for movies of specified genre
                genre = input("Please enter the genre name:").strip()
                search_genre(genre, movies).show()

            elif choice == 4:   # Search for movies of specified genres
                genres = enterGenres()
                search_genres(genres, movies_genres, movies).show()

            elif choice == 5:   # Count number of movies user has watched
                userId = enterId("user ")
                count_watched_movies(movies_genres_watched, userId).show()

            elif choice == 6:   # Count number of genres user has watched
                userId = enterId("user ")
                count_watched_genres(movies_genres_watched, userId).show()

            elif choice == 7:   # Get titles of movies watched by users
                userIds = enterUsersIds()
                get_titles_of_movies_watched_ids(movies_genres_watched, userIds).show()
            
            elif choice == 8:   # Get watchcount and avg rating of movie by its id
                movieId = enterId("movie ")
                summarise_avg_rating_watchcount_movie(search_movie_by_id(movieId, movies_genres_watched)).show()
            
            elif choice == 9:   # Get watchcount and avg rating of movie by its title
                title = input("Please enter the movie title:").strip()
                summarise_avg_rating_watchcount_movie(search_movie_by_title(title, movies_genres_watched)).show()

            elif choice == 10:  # Search for movies in given genre
                genres = enterGenres()
                search_movies_in_genres(movies_genres_watched, genres).show(100)

            elif choice == 11:  # Show top n rated movies 
                n = int(input("please enter the number of top watching movies to show:").strip())
                list_top_watched(ratings, movies).show(n)
            
            
            elif choice == 12:  # Search movie by year
                year = input("Please enter a year between 1750 - 2018")
                search_movies_by_year(year, movies).show()

            elif choice == 13:  # Find favourite genre of user
                user_id = enterId("user ")
                search_favourite_genre(user_id, movies_genres_watched).show()

            elif choice == 14:  # Compare user tastes
                user1_Id = enterId("First user ")
                user2_Id = enterId("Second user ")
                compareTastes(user1_Id, user2_Id, movies_genres, ratings_clean, movies_clean).show()

            elif choice == 15:  # Visualise
                select_visualisations(movies_genres_watched)

            elif choice == 16:
                print("Recommend movies")

            elif choice == 17:
                break
            
        except ValueError:
            print("You should enter only a number between 1 and 17!")



controlPanel()