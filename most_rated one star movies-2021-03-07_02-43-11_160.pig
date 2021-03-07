ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int , movieID:int, rating:int, ratingTime:int) ;

metadata = LOAD '/user/maria_dev/ml-100k/u.item' using PigStorage('|')
	AS (movieID:int,movieTitle:chararray , releaseDate:chararray,videoRelease:chararray,imbdLink:chararray);
nameLookup = FOREACH metadata GENERATE movieID,movieTitle;

groupbymovieID =group ratings by movieID ;

avgRatings = FOREACH groupbymovieID Generate group as movieID,
	AVG(ratings.rating) as avgRating, COUNT(ratings.rating) as numRatings ;
    
badmovies = Filter avgRatings by avgRating < 2.0 ;

t2 = join badmovies by movieID , nameLookup by movieID ;

t3 = FOREACH t2 GENERATE nameLookup::movieTitle AS movieName, badmovies::avgRating AS avgRating , badmovies::numRatings as numberofRatings ;

results = order t3 by numberofRatings Desc ;
Dump results ;
STORE results INTO '/user/maria_dev/ml-100k/output' using PigStorage('|');