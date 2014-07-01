/*parse job descriptions*/

--tokenize
jobs = load 'jobs/*.csv' using PigStorage(',') as (jobkey, description);
samplejobs = limit jobs 10;
descriptions = foreach samplejobs generate jobkey as jobkey, flatten(TOKENIZE(description)) as word;

--remove stop words
stopwords = load 'jobs/stopwords-en.txt' using PigStorage() as stop;
removestop = join descriptions by word left outer, stopwords by stop using 'replicated';
filteredstop = filter removestop by stopwords::stop is null;

--stem
register porter.jar;
stemmed = foreach filteredstop generate descriptions::jobkey as jobkey, porter.Porter(descriptions::word) as stem;
grouped = group stemmed by stem;
uniquestems = foreach grouped generate group as stem, stemmed.jobkey as jobkeys;

--correct spelling
dictionary = load 'jobs/dictionary.txt' using PigStorage() as dict;
checkspelling = join dictionary by dict right outer, uniquestems by stem using 'skewed';
wrong = filter checkspelling by dictionary::dict is null;
groupdict = group dictionary all;
bagdict = foreach groupdict generate dictionary.dict as words;
combo = foreach wrong generate uniquestems::stem as stem, flatten(bagdict.words) as poss;
register getdist.jar;
distance = foreach combo generate stem as stem, poss as poss, getdist.Levenshtein((chararray)stem,(chararray)poss) as dist;
groupstems = group distance by stem;
limitright = foreach groupstems{
	mindist = order distance by dist;
	top = limit mindist 1;
	generate flatten(top);
}
rejoin = join limitright by stem, uniquestems by stem;
fixed = foreach rejoin generate limitright::top::poss as stem, uniquestems::jobkeys as jobkeys;

--regroup by jobkey
correct = filter checkspelling by dictionary::dict is not null;
alreadycorrect = foreach correct generate uniquestems::stem as stem, uniquestems::jobkeys as jobkeys;
allright = union alreadycorrect, fixed;
newset = foreach allright generate flatten(jobkeys) as jobkey, stem as words;
regroup = group newset by jobkey;
out = foreach regroup generate group, newset.words;
store out into 'jobdesc' using PigStorage();






