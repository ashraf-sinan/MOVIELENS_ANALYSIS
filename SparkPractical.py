import pyspark 
import random
from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


sc = pyspark.SparkContext.getOrCreate() 
## Read the data into memory

sqlContest = SQLContext(sc)

SMALL_DATASET_PATH = 'ml-latest_small'
DATASET_PATH = 'ml-latest'

def loadData(dataPath):
    #Load the movies and rating CSV files
    movies = sqlContest.read.csv(dataPath + '/movies.csv', header=True)
    ratings = sqlContest.read.csv(dataPath + '/ratings.csv', header=True)
    return movies, ratings


def cleanData(ratings, movies):
    
    ##Convert the timestamp to date 
    # Drop NA values
    # Convert data Type of columns intro integer and float
    cleanRatings = ratings.na.drop()\ 
    .withColumn('RatingDate', func.date_format(ratings.timestamp.
                                                           cast(dataType= typ.LongType()).
                                                           cast(dataType= typ.TimestampType()),
                                                           "yyyy-MM-dd"))\
    .withColumn("rating", ratings.rating.cast(dataType = typ.FloatType()))\
    .withColumn("userId", ratings.userId.cast(dataType = typ.IntegerType()))\
    .withColumn("movieId", ratings.movieId.cast(dataType = typ.IntegerType()))
    
    # Drop NA values
    # Convert data Type of columns into integer
    
    cleanMovies = movies.na.drop()\
    .withColumn("movieId", movies.movieId.cast(dataType = typ.IntegerType()))
    
    #Split the genres column into rows based on the number of the separator | 

    moviesGeners =movies.withColumn("Genr",explode(split("genres","[|]")))

    
    return cleanRatings, cleanMovies, moviesGeners



def searchUserById(userId, ratings, movies):
    filteredUsers = ratings.where("userId =" + str(userId))\
    .join(movies, on=['movieId'], how ='inner')
    
    return filteredUsers

def searchUsersByIds(ids, ratings):
    filteredUsers = ratings.filter(col("userId").isin(ids))
                                  
    return filteredUsers

def searchMovieById(movieId, movies):
    filteredMovies = movies.where("movieId = " + str(movieId))
    
    return filteredMovies

def searchMovieByTitle(title, movies):
    filteredMovies = movies.where("title like \'%" + 
                                  str(title) + "%\'")
    
    return filteredMovies

def searchGenre(genr, movies):
    filteredMovies = movies.where("genres like \'%" + 
                                  str(genr) + "%\'")
    #filteredMovies.show()
    return filteredMovies

def searchGenres(genrs, moviesGeners, movies):
    genersMovies = moviesGeners.filter(col("Genr").isin(genrs))
    filteredMovies = genersMovies.join(movies, on=['movieId'], how = 'inner')\
    .sort(asc("Genr"))
        
    return filteredMovies

def summarizeMovie(movies, ratings):
    summary = movies.join(ratings, on=['movieId'], how='inner')\
    .groupBy("movieId")\
    .agg(avg(col("rating")), count(col("movieId")))
    .withColumnRenamed("avg(rating)", "Average_Rating")\
    .withColumnRenamed("count(movieId)", "Number_OF_Watchings")
    
    return summary

def searchMoviesByYear(year, movies):
    filteredMovies = movies.where("title like \'%(" + 
                                   str(year) + ")\'")
    filteredMovies.show()
    return

def listTopNRated(ratings, movies):
    topRated = ratings\
    .groupBy("movieId")\
    .agg(avg(col("rating")))\
    .withColumnRenamed("avg(rating)", "Average_Rating")\
    .join(movies, on=['movieId'], how='inner')\
    .sort(desc("Average_Rating"))
    
    
    return topRated


def listTopNWatched(ratings, movies):
    mostPopular = ratings\
    .groupBy("movieId")\
    .agg(func.count("userId"))\
    .withColumnRenamed("count(userId)", "Number_OF_Watchings")\
    .sort(func.desc("Number_OF_Rating"))\
    .join(movies, on=['movieId'], how='inner')
    
    return mostPopular


## Intermediate Requirements:

def findUsersGenre(userId, moviesGenres, ratings, movies):
    
    userMovies = searchUserById(userId, ratings, movies)
    userGenres = userMovies.join(moviesGenres, on=['movieId'], how='inner')
    userGenres = userGenres.groupBy("userId","Genr")\
    .agg(func.round(avg(col("rating")),2), func.count(col("userId")))\
    .withColumnRenamed("round(avg(rating), 2)", "Average_Rating")\
    .withColumnRenamed("count(userId)", "wachings")\
    .sort(desc("Average_Rating"))#.where("Average_Rating >=3 ")\
    
    return userGenres

def findUsersFavourite(userId, moviesGenres, ratings, movies):
  
    userGeners = findUsersGenre(userId, moviesGenres, ratings, movies)
    userAverages = userGeners.groupBy("userId")\
    .agg(avg(col("Average_Rating")), avg(col("wachings")))
    averageRating = userAverages[0]['avg(Average_Rating)']
    averageWachings = userAverages[1]['avg(wachings)']
    userFav = userGeners(userId).where(("Average_Rating >= " + averageRating)  & 
                                       ("wachings >= " + averageWachings))
    
    return userFav
    
def compareTastes(userId1, userId2, moviesGenres, ratings, movies):
    u1 = findUsersGenre(userId1)
    u2 = findUsersGenre(userId2)
    u1 = u1.withColumnRenamed("wachings","User_" + str(userId1) +"_Watching_Times")\
    .withColumnRenamed("Average_Rating","User_" + str(userId1) +"_Rating")
    u2 = u2.withColumnRenamed("wachings","User_" + str(userId2) +"_Watching_Times")\
    .withColumnRenamed("Average_Rating","User_" + str(userId2) +"_Rating")    
    u1 = u1.join(u2, on=['Genr'], how='outer')
    print("Comparing the Tastes of user:" +
         str(userId1) + " , and user:" + str(userId2) + " :" )
    
    return u1

## Advanced Requirements:

def clusterUsersByTaste():
    #kmeans = 
    return 0

def visualizeTheDataSet():
    
    return 0

def recommendMovie():
    als = ALS(maxIter=10, regParam=0.1, userCol="userId",
         itemCol='movieId', ratingCol= "rating", coldStartStrategy="drop")

    train, test = ratings.randomSplit([0.2,0.02])
    alsModel = als.fit(train)
    prediction = alsModel.transform(test)
    prediction.show(10)
    evaluator = RegressionEvaluator(metricName="mse", 
                               labelCol = "rating", 
                               predictionCol="prediction")
    mse = evaluator.evaluate(prediction)
    print(mse)
    
    return 0

def enterId(message):
    
    try:
        userId = int(input('Enter the ' + message + ' ID (>0):'))
        
        if userId < 0:            
            raise Exception( message +   " ID should be larger than or equal to 1!")
            
        return userId
    
    except ValueError:
            print("You should enter only a number larger than 0!")
            
            
def enterUsersIds():
    
    usersIdsString = input("Enter a list of users IDs separated by comma (,). Note: IDs should be larger than 0!:").strip()
    usersIdsListOfStrings = usersIds.split(",")
    usersIdsListOfIntegers = []
    for userId in usersIdsListOfStrings:
        try:
            userIdInt = int(userId.strip())
            usersIdsListOfIntegers.append(userIdInt)
        except ValueError:
            print("Some inputs are not appropriate!")           
    
    return usersIdsListOfIntegers

def enterGenres():
    genresStr = input("Enter Genres list separated by comma (,):").strip()
    genresList = genresStr.split(",")
    
    
    return genresList




def controlPanel():
    
    #Select the dataset
    dataPath = SMALL_DATASET_PATH
    
    #Load the datasets
    ratings, movies = loadData(dataPath)
    
    #Clean the dataset
    ratings, movies, moviesGenres = cleanData(ratings, movies)
    
    #presist the data in memory
    movies.persist()
    ratings.persist()
    
    #Set the number of records to show
    N = 7
    
    #Interact with the dataset
    while True:
        print("Choices:")
        print("1- Search user | 2-Search Users | 3-Search Genre | 4-Search Genres")
        print("5- Search Movie By ID | 6-search Movie by title | 7- search movies by year")
        print("8-List top rating movies | 9- List top watching movies")
        print("10- Show User's favourite genres | 11- compare two users' taste | 12- Show visualizations")
        print("13- Recommend Movies for user | 14- Exit")
        
        try:
        choice = int(input('Enter your choice:').strip())
        
        if choice == 1:
            userId = enterId("user ")
            searchUserById(userId, ratings, movies).show()
            
        elif choice == 2:
            usersIds = enterUsersIds()
            searchUsersByIds(usersIds, ratings, movies)
            
        elif choice == 3:
            genre = input("Please enter the genre name:").strip()
            searchGenre(genre, movies).show()
            
        elif choice == 4:
            genres = enterGenres()
            searchGenres(genres, moviesGeners, movies).show()
            
        elif chocie == 5:
            movieId = enterId("Movie ")
            summarizeMovie( searchMovieById(movieId, movies), ratings).show()
            
        elif choice == 6:
            title = input("Please enter the movie title:").strip()
            summarizeMovie( searchMovieByTitle(title, movies), ratings).show()
            
        elif choice == 7:            
            searchMoviesByYear(year, movies).show()
            
        elif choice == 8:
            n = int(input("please enter the number of top ratings to show:").strip())
            listTopNRated(ratings, movies).show(n)
            
        elif choice == 9:
            n = int(input("please enter the number of top watching movies to show:").strip())
            listTopNWatched(ratings, movies).show(n)
            
        elif choice == 10:
            userId = enterId("user ")
            findUsersGenre(userIds, moviesGenres, ratings, movies).show()
            
        elif choice == 11:
            user1Id = enterId("first user ")
            user2Id = enterId("first user ")
            compareTastes(user1Id, user2Id, moviesGenres, ratings, movies).show()
        elif choice == 12:
            
        elif choice == 13
            
        elif choice == 14:
            
        elif choice == 15:
            
        else:
            
        
        
        except ValueError:
            print("You should enter only a number between 1 and 15!")



