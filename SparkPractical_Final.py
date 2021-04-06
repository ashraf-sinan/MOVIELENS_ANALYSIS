#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import pyspark 

import findspark
spark = findspark.init()
import random
import pyspark
from pyspark.sql import SQLContext, Row, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, substring, round, rtrim
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession


#spark = SparkSession.builder.getOrCreate()
sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

SMALL_DATASET_PATH = 'ml-latest-small'
DATASET_PATH = 'ml-latest'

def load_data(dataPath):
    #Load the movies and rating CSV files
    movies = sqlContest.read.csv(dataPath + '/movies.csv', header=True)
    ratings = sqlContest.read.csv(dataPath + '/ratings.csv', header=True)
    
    return ratings, movies


def clean_data(ratings, movies):
    
    ##Convert the timestamp to date 
    # Drop NA values
    # Convert data Type of columns intro integer and float
    clean_ratings = ratings.na.drop()    .withColumn('watching_date', func.date_format(ratings.timestamp.
                                                           cast(dataType= typ.LongType()).
                                                           cast(dataType= typ.TimestampType()),
                                                           "yyyy-MM-dd"))\
    .withColumn("rating", ratings.rating.cast(dataType = typ.FloatType()))\
    .withColumn("userId", ratings.userId.cast(dataType = typ.IntegerType()))\
    .withColumn("movieId", ratings.movieId.cast(dataType = typ.IntegerType()))
    
    # Drop NA values
    # Convert data Type of columns into integer
    
    clean_movies = movies.na.drop()    .withColumn("movieId", movies.movieId.cast(dataType = typ.IntegerType()))    .withColumn("production_year", substring(rtrim(col("title")), -5, 4))
    
    #Split the genres column into rows based on the number of the separator | 

    movies_geners =movies.withColumn("Genr",explode(split("genres","[|]")))

    ## return the clean datafrmes (ratings, movies, and movies genres)
    
    return clean_ratings, clean_movies, movies_geners

def save_as_csv(data, path):
    ##Save the dataframe to thepath
    data.write.format('com.databricks.spark.csv').mode("overwrite").save(path)

def save_data(movies_genres, ratings, movies):
    
    ##Save the three dataframes
    save_as_csv(movies_genres,"movies_genresSparkSave")
    save_as_csv(movies,"moviesSparkSave")
    save_as_csv(ratings,"ratingsSparkSave")
    

    return True
 

def search_user_by_id(user_id, ratings, movies):
    
    ##filter the users by the ID
    filtered_users = ratings.where("userId =" + str(user_id))    .join(movies, on=['movieId'], how ='inner')
    
    return filtered_users

def search_users_by_ids(ids, ratings, movies):
    
    ## filtere users by a list of users ids using isin  
    filtered_users = ratings.filter(col("userId").isin(ids))    .join(movies, on=['movieId'], how ='inner')
                                  
    return filtered_users

def search_movie_by_id(movie_id, movies):
    
    ##filter the movies by the movie id    
    filtered_movies = movies.where("movieId = " + str(movie_id))
    
    return filtered_movies

def search_movie_by_title(title, movies):
    
    ##filter the movies by title
    filtered_movies = movies.where("title ='" + 
                                  str(title) + "'" )
    
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


def summarize_movie(movies, ratings):
    
    ##summarize movies by getting the number of watchings and average rating
    ## and rename the aggregated fields
    summary = movies.join(ratings, on=['movieId'], how='inner')    .groupBy("movieId","title","genres")    .agg(round(avg(col("rating")),2), count(col("movieId")))    .withColumnRenamed("round(avg(rating), 2)", "Average_Rating")    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")
    
    return summary

def summarize_genres(movies_genres, ratings):
    
    ##summarize genres by getting the number of watchings and average rating
    ## and rename the aggregated fields
    summary = movies_genres.join(ratings, on='movieId', how='inner')    .groupBy("Genr")    .agg(count(col('movieId')), avg(col('rating')))    .withColumnRenamed("avg(rating)", "Average_Rating")    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")
    
    return summary

def search_movies_by_year(year, movies):
    
    ##filter movies by year
    filtered_movies = movies.where("title like \'%(" + 
                                   str(year) + ")\'")
    return filtered_movies

def list_top_rated(ratings, movies):
    
    ## group the movies by movie id and aggregate their rating
    ## then sort them by rating from heighst to lowest
    top_rated = ratings    .groupBy("movieId")    .agg(avg(col("rating")))    .withColumnRenamed("avg(rating)", "Average_Rating")    .join(movies, on=['movieId'], how='inner')    .sort(desc("Average_Rating"))
     
    
    return top_rated


def list_top_watched(ratings, movies):
    
    ## group the movies by movie id and aggregate their watchings
    ## then sort them by watchings from heighst to lowest
    
    most_popular = ratings    .groupBy("movieId")    .agg(func.count("movieId"))    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")    .join(movies, on=['movieId'], how='inner')    .sort(func.desc("Number_OF_Watchings"))
    
    return most_popular


## Intermediate Requirements:

def find_users_genres(user_id, movies_genres, ratings, movies):
    
    ##Search the movies that the user has wathced
    ##group them by the user and Genre and aggregate their 
    user_genres = search_user_by_id(user_id, ratings, movies)    .join(movies_genres, on=['movieId'], how='inner')    .groupBy("userId","Genr")    .agg(func.round(avg(col("rating")),2), func.count(col("userId")))    .withColumnRenamed("round(avg(rating), 2)", "Average_Rating")    .withColumnRenamed("count(userId)", "wachings")    .sort(desc("Average_Rating"))#.where("Average_Rating >=3 ")\
    
    return user_genres

def find_user_favourite_genres(user_id, movies_genres, ratings, movies):
    #Get teh users genres summary
    user_geners = find_users_genres(user_id, movies_genres, ratings, movies)
    
    #Extract the total average watching and average rating 
    user_averages = user_geners.groupBy("userId")    .agg(avg(col("Average_Rating")), avg(col("wachings")))

    average_rating = user_averages.collect()[0]['avg(Average_Rating)']
    average_wachings = user_averages.collect()[0]['avg(wachings)']
    
    #Filter the genres thaw was rated more than average and watched more than average
    user_fav = user_geners.where("Average_Rating >= " + str(average_rating))    .where("wachings >= " + str(average_wachings))
    
    return user_fav

def find_user_unfavourite_genres(user_id, movies_genres, ratings, movies):
    #Get teh users genres summary
    user_geners = find_users_genres(user_id, movies_genres, ratings, movies)
    
    #Extract the total average watching and average rating 
    user_averages = user_geners.groupBy("userId")    .agg(avg(col("Average_Rating")), avg(col("wachings")))

    average_rating = user_averages.collect()[0]['avg(Average_Rating)']
    average_wachings = user_averages.collect()[0]['avg(wachings)']
    
    #Filter the genres thaw was rated  less average and watched less than average
    user_fav = user_geners.where("Average_Rating < " + str(average_rating))    .where("wachings <" + str(average_wachings))
    
    return user_fav
    
def compareTastes(user1_Id, user2_Id, movies_genres, ratings, movies):
    
    ##Get the first user genres
    u1 = find_users_genres(user1_Id, movies_genres, ratings, movies)
    ##Get the second users genres
    u2 = find_users_genres(user2_Id, movies_genres, ratings, movies)
    ##Rename the columns to show users ids on them
    u1 = u1.withColumnRenamed("wachings","User_" + str(user1_Id) +"_Watching_Times")    .withColumnRenamed("Average_Rating","User_" + str(user1_Id) +"_Rating")
    u2 = u2.withColumnRenamed("wachings","User_" + str(user2_Id) +"_Watching_Times")    .withColumnRenamed("Average_Rating","User_" + str(user2_Id) +"_Rating") 
    
    ##Create a summary table that show the frequency of watching and average of ratings
    summary = u1.join(u2, on=['Genr'], how='inner').select("Genr", 
                                                           "User_" + str(user1_Id) + "_Watching_Times", 
                                                           "User_" + str(user2_Id) +"_Watching_Times",
                                                           "User_" + str(user1_Id) +"_Rating",
                                                           "User_" + str(user2_Id) +"_Rating")
                                                           
    ##See teh genres that every user watch but not the other
    u1_difference = u1.select('Genr').subtract(u2.select('Genr')).join(u1, on=['Genr'], how='inner')
    u2_difference = u2.select('Genr').subtract(u1.select('Genr')).join(u2, on=['Genr'], how='inner')
    u1_favourite = find_user_favourite_genres(user1_Id, movies_genres, ratings, movies)
    u2_favourite = find_user_favourite_genres(user2_Id, movies_genres, ratings, movies)    
    u1_unfavourite = find_user_unfavourite_genres(user1_Id, movies_genres, ratings, movies)
    u2_unfavourite = find_user_unfavourite_genres(user2_Id, movies_genres, ratings, movies)
    
    u1_and_u2_like = u1_favourite.select('Genr')    .join(u2_favourite.select('Genr'), on=['Genr'], how='inner')    .join(summary, on =['Genr'], how='inner')
    
    u1_and_u2_dislike = u1_unfavourite.select('Genr')    .join(u2_unfavourite.select('Genr'), on=['Genr'], how='inner')    .join(summary, on =['Genr'], how='inner')
    
    u1_like_u2_dislike = u1_favourite.select('Genr')    .join(u2_unfavourite.select('Genr'))    .join(summary, on =['Genr'], how='inner')
    
    u1_dislike_u2_like = u1_unfavourite.select('Genr')    .join(u2_favourite.select('Genr'))    .join(summary, on =['Genr'], how='inner')
    
    ##Return the results
    return summary, u1_difference, u2_difference, u1_and_u2_like, u1_and_u2_dislike, u1_like_u2_dislike, u1_dislike_u2_like


## Advanced Requirements:


def summarize_movies_produced_annualy(movies):
    ##Summarize the movies by production year
    annual_summary = movies.groupBy('production_year')    .agg(count(col('production_year')))    .withColumnRenamed('count(production_year)', 'num_of_movies')    .sort(desc('num_of_movies'))
    return annual_summary

def summarize_genres_produced_annualy(movies, movies_genres):
    ##Summarize the movies by Genre and production year
    annual_summary = movies.join(movies_genres, on=['movieId'], how='inner')    .groupBy('production_year', 'Genr')    .agg(count(col('production_year')))    .withColumnRenamed('count(production_year)', 'num_of_movies')    .sort(desc('num_of_movies'))
    
    return annual_summary

def summarize_genres_movies(movies, movies_genres):
    #Summarize the movies in each genre
    genre_summary = movies_genres.join(movies, on = ['movieId'], how = 'inner')    .groupBy('Genr')    .agg(count(col('Genr')))    .withColumnRenamed('count(Genr)', 'num_of_movies')    .sort(asc('Genr'))
    
    return genre_summary

def summarize_watchings_year(ratings):
    yearly_watchings = ratings.withColumn('watching_year', substring('watching_date',0,4))    .groupBy('watching_year')    .agg(count(col('watching_year')))    .withColumnRenamed('count(watching_year)', 'num_of_watchings')    .sort(asc('watching_year'))
    
    return yearly_watchings

def summarize_active_users(ratings):
    summary_data = ratings.select(['userId','watching_date']).distinct()    .withColumn('watching_year', substring('watching_date',0,4))    .groupBy('watching_year')    .agg(count(col('userId')))    .withColumnRenamed('count(userId)', 'num_of_users')    .sort(desc('watching_year'))
    
    return summary_data

def summarzie_watchings_of_genres(ratings, movies_genres):
    genres_summary = ratings.join(movies_genres, on=['movieId'], how='inner')    .groupBy('Genr')    .agg(count(col('Genr')), round(avg(col('rating')),2))    .withColumnRenamed('count(Genr)' , 'num_of_watchings')    .withColumnRenamed('round(avg(rating),2)', 'average_rating')
    return genres_summary

def export_csv(data, path):
    data.toPandas().to_csv(path)
     

def summarize_all_data(ratings, movies, movies_genres):
    users_count = ratings.select('userId').distinct().count()
    movies_count = movies.select('movieId').distinct().count()
    watchings_count = ratings.count()
    genres_count = movies_genres.select('Genr').distinct().count()
    print("In this dataset there are:")
    print(str(users_count) + " Users.")
    print(str(movies_count) + " Movies.")
    print(str(genres_count) + " Genres.")
    print(str(watchings_count) + " Ratings.")
    print("The table below shows the number of movies were poduced every year:")
    yearly_movies = summarize_movies_produced_annualy(movies)
    yearly_movies.show()
    print("The table below shows the number of movies of each genre were poduced every year:")
    yearly_genres_movies = summarize_genres_produced_annualy(movies, movies_genres)
    yearly_genres_movies.show()
    print("The table below shows the number of movies per Genre:")
    genres_movies = summarize_genres_movies(movies, movies_genres)
    genres_movies.show()
    print("The table below shows the number of Ratings were done every year:")
    yearly_watchings = summarize_watchings_year(ratings)
    yearly_watchings.show()    
    print("The table below shows the number of active users every year:")
    yearly_active_users = summarize_active_users(ratings)
    yearly_active_users.show()
    print("The table below shows the number of watchings of every Genre:")
    genres_watchings = summarzie_watchings_of_genres(ratings, movies_genres)
    genres_watchings.show()
    
    ##Export the data summaries to csv
    
    export_csv(yearly_movies,"yearly_movies.csv")
    export_csv(yearly_genres_movies,"yearly_genres_movies.csv")    
    export_csv(genres_movies,"genres_movies_summary.csv")    
    export_csv(yearly_watchings,"yearly_watchings.csv")    
    #export_csv(yearly_active_users,"yearly_active_users.csv")    
    export_csv(genres_watchings,"genres_watchings.csv")    
    
    
def train_als_model(ratings):
    ##Define ALS model 
    ## Max iteration 10 with learning rate 0.2, specifing the 
    als_model = ALS(maxIter=10, regParam=0.2, userCol="userId",
         itemCol='movieId', ratingCol= "rating", coldStartStrategy="drop")
    
     ##Split the data into training and testing data
    training_set, test_set = ratings.randomSplit([0.8,0.2])
    
    ##Train the model on the data
    print('Training the recommendation system in progress.......')
    als_trained_model = als_model.fit(training_set)
    
    ##Predict the test data set
    prediction = als_trained_model.transform(test_set)
    
    ##Evaluate the accuracy 
    evaluator = RegressionEvaluator(metricName="mse", 
                               labelCol = "rating", 
                               predictionCol="prediction")
    ##show the model cost function
    mse = evaluator.evaluate(prediction)
    print("Recommendation system mean squared eror = " + str(mse))
    
    return als_trained_model

    
def recommend_movies(als_model, user_id, num_of_movies, movies):

    #REcommend movies and filter recommendations for the specific user 
    recommendations = als_model.recommendForAllUsers(num_of_movies)
    recommendations = recommendations.where(recommendations.userId == user_id).    select('recommendations').collect()
    
    #Save the recommended movies ids into list
    rec_movies_ids = []
 
    for rec in recommendations[0][0]:
        rec_movies_ids.append(int(rec['movieId']))
    
    #Create a dataframe from the list and rename the value field into movieId
    recommended_movies_ids = spark.createDataFrame(rec_movies_ids, typ.IntegerType())    .withColumnRenamed("value","movieId") 
    
    #Join the movies Ids with the movies dataframe to show recommended movies details
    recommended_movies = recommended_movies_ids.join(movies, on=['movieId'], how='inner')
    
    
    return recommended_movies

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
    
    return usersIdsListOfIntegers

def enterGenres():
    genresStr = input("Enter Genres list separated by comma (,):").strip()
    genresList = genresStr.split(",")
    genresList = stripList(genresList)
    
    return genresList

def show_user_details(user_id, movies_genres, ratings, movies):
    user_genres = find_users_genres(user_id, movies_genres, ratings, movies)
    print("user " + str(user_id) + "  have watched " + str(user_genres.count()) + "  Genres.")
    user_movies = search_user_by_id(user_id, ratings, movies)
    print("user " + str(user_id) + "  have watched " + str(user_movies.count()) + "  movies.")
    print("User wacthed Genres:")
    user_genres.show()
    print("User watched Movies:")
    user_movies.select('title', 'watching_date', 'rating').show()

    return

def show_genre_details(genre, movies):
    genre_movies = search_genre(genre, movies)
    print("Genre " + genre + " contains " + str(genre_movies.count()) + " Movies.")
    print("Some movies in the genre:")
    genre_movies.show()

def controlPanel():
    
    #Select the dataset
    data_path = SMALL_DATASET_PATH 
    
    data_select = int(input("Please enter the number (1) to select the small dataset or (2) to use the big dataset:").strip())
    if data_select == 2:
        data_path = DATASET_PATH
    
    #Load the datasets
    ratings, movies = load_data(data_path)
    print("Data loaded successfuly......")
    
    #Clean the dataset
    
    ratings, movies, movies_genres = clean_data(ratings, movies)
    print("Data cleaned successfully......")
    
    #presist the data in memory
    movies.persist()
    ratings.persist()
    movies_genres.persist()
    if data_select != 2:
        print("Data is presist in memory now......")
        print()
        als_model = train_als_model(ratings)
        print("Recommendations system is ready now....")
    else:
        print("Recommendation can work only with the small data because of the limited resources!")
    #Set the number of records to show
    N = 7
    summarize_all_data(ratings, movies, movies_genres)
    #Interact with the dataset
    while True:
        print("___________________________________________________________________________________")
        print()
        print("Choices:")
        print("1- Search user | 2-Search Users | 3-Search Genre | 4-Search Genres")
        print("5- Search Movie By ID | 6-search Movie by title | 7- search movies by year")
        print("8-List top rating movies | 9- List top watching movies")
        print("10- Show User's favourite genres | 11- compare two users' taste | 12- Show visualizations")
        print("13- Recommend Movies for user | 14- Exit")
        print("___________________________________________________________________________________")
        try:
            print("_______________________________________________________________________")
            print()
            choice = int(input('Enter your choice number from the list above:').strip())
            
            if choice == 1:
                user_id = enterId("user ")
                show_user_details(user_id, movies_genres, ratings, movies)              
                

            elif choice == 2:
                users_ids = enterUsersIds()
                for user_id in users_ids:
                    show_user_details(user_id, movies_genres, ratings, movies)
               

            elif choice == 3:
                genre = input("Please enter the genre name:").strip()
                show_genre_details(genre, movies)
               

            elif choice == 4:
                genres = enterGenres()
                for genre in genres:
                    show_genre_details(genre, movies)

            elif choice == 5:
                movieId = enterId("Movie ")
                summarize_movie( search_movie_by_id(movieId, movies), ratings).show()

            elif choice == 6:
                title = input("Please enter the movie title:").strip()
                summarize_movie( search_movie_by_title(title, movies), ratings).show()

            elif choice == 7:
                year = input("Please enter a year between 1750 - 2018 :  ")
                years_movies = search_movies_by_year(year, movies)
                print("In " + str(year) + "  " +  str(years_movies.count()) + "  Movies were produced.")
                print("Some movies from the year:")
                years_movies.show()

            elif choice == 8:
                n = int(input("please enter the number of top ratings to show:").strip())
                list_top_rated(ratings, movies).show(n)

            elif choice == 9:
                n = int(input("please enter the number of top watching movies to show:").strip())
                list_top_watched(ratings, movies).show(n)

            elif choice == 10:
                user_id = enterId("user ")
                user_fav = find_user_favourite_genres(user_id, movies_genres, ratings, movies)
                print("user with id " + str(user_id) + " has  " +  str(user_fav.count()) + " Favourite genres:")
                user_fav.show()

            elif choice == 11:
                
                user1_Id = enterId("First user ")
                user2_Id = enterId("Second user ")
                summary, u1_diff, u2_diff, u1_and_u2_like, u1_and_u2_dislike, u1_like_u2_dislike, u1_dislike_u2_like = ( 
                compareTastes(user1_Id, user2_Id, movies_genres, ratings, movies))
                
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
                print("User " + str(user2_Id) + " watched " + str(u1_diff.count()) + " Genres that User " + str(user1_Id) + " have not watched :")
                u2_diff.show()
                
                
            elif choice == 12:
                genresSummary = summarize_genres(movies_genres, ratings)
                genresSummary.show()
                gens_df = genresSummary.toPandas()
                plt.bar(gens_df["Genr"], gens_df["Number_OF_Watchings"])
                plt.show()
            elif choice == 13:
                user_id = enterId("user ")
                print("The top 10 recommended movies for this user are:")
                rec_movies = recommend_movies(als_model, user_id, 10, movies)
                rec_movies.show()
            elif choice == 16:
                break
            
            
        
        
        except ValueError:
            print("You should enter only a number between 1 and 15!")
        print()
        print("***************************************************************************************************")
        print()



controlPanel()


# # 

# In[ ]:





# In[ ]:


1

