/*get sentiment of tweets*/

--tokenize
meta = load 'tweets/tweets_*.txt' using PigStorage('|') as (dt, id, handle, name, description, country, city, lat, long, tweet);
tweets = foreach meta generate dt as dt, id as id, flatten(TOKENIZE(tweet)) as words;

--good & bad word prep
good = load 'tweets/good.txt' using PigStorage() as gw;
goodwords = foreach good generate gw as word, 1 as score;
bad = load 'tweets/bad.txt' using PigStorage() as bw;
badwords = foreach bad generate bw as word, -1 as score;
dict = union goodwords, badwords;

--score each tweet
scoredtweets = join tweets by LOWER($2), dict by word using 'replicated';
groupscore = group scoredtweets by ($0,$1);
sumscore = foreach groupscore generate group, SUM(scoredtweets.dict::score);

--get total number of positive & negative tweets
positive = filter sumscore by $1 > 0;
negative = filter sumscore by $1 < 0;
posgroup = group positive all;
neggroup = group negative all;
totpos = foreach posgroup generate 'Positive tweets: ', COUNT(positive.$0);
totneg = foreach neggroup generate 'Negative tweets: ', COUNT(negative.$0);
out = union totpos, totneg;
store out into 'tweetsentiment' using PigStorage();
