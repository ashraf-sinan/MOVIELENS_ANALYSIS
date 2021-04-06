from pyspark.sql import SQLContext, functions as func, types as typ
from pyspark.sql.functions  import explode, split, avg, col, desc, asc, count, countDistinct, substring

def enterId(message):
    
    try:
        user_id = int(input('Enter the ' + message + ' ID (>0):').strip())
        
        if user_id < 0:            
            raise Exception( message +   " ID should be larger than or equal to 1!")
            
        return user_id
    
    except ValueError:
            print("You should enter only a number larger than 0!")
            
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


def search_movies_by_year(year, movies):
    
    ##filter movies by year
    filtered_movies = movies.where("title like \'%(" + 
                                   str(year) + ")\'")
    return filtered_movies

