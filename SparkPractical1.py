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

