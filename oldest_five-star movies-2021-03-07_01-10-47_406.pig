ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int , movieID:int, rating:int, ratingTime:int) ;

metadata = LOAD '/user/maria_dev/ml-100k/u.item' using PigStorage('|')
	AS (movieID:int,movieTitle:chararray , releaseDate:chararray,videoRelease:chararray,imbdLink:chararray);
nameLookup = FOREACH metadata GENERATE movieID,movieTitle,
	ToUnixTime(ToDate(releaseDate,'dd-MMM-yyyy'))  AS releaseTime , ToDate(releaseDate,'dd-MMM-yyyy')  AS release;
    
ratingsByMovie = GROUP ratings BY movieID ;

avgRatings = FOREACH ratingsByMovie Generate  group AS movieID, AVG(ratings.rating) as avgRating ;

fivestarmovie = FILTER avgRatings BY avgRating > 4.0 ;

fivestarmoviewithname = Join fivestarmovie BY movieID , nameLookup BY movieID ;

oldfivestar = Order fivestarmoviewithname by nameLookup::releaseTime ;

DUMP oldfivestar
store oldfivestar into '/user/maria_dev/ml-100k/output' using PigStorage(',');