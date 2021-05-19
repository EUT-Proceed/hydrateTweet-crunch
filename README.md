# hydrateTweet-crunch

This repository has been created in order to analyze the tweets (their IDs can be found at [echen102/COVID-19-TweetIDs](https://github.com/echen102/COVID-19-TweetIDs)) and study the emotional impact of Covid19 on people.

## Documentation

Crunch the Tweets from the dataset and perform different kinds of operations such as

* sorting tweets by language and month while keeping less information
* analysing users' emotions and standardizing results obtained 
* analysing unique users and the number of their tweets across the entire dataset
* downloading users' profile images
* inferring users' gender, age and if they are an organization
* filtering the users inferred under certain constraints
* aggregating tweets of the same language per week (instead of day)

Each operation has been inserted to improve performance without the need of threads.

Below you can find a description and a usage example for each processor developed for this project.

### sort-lang

This was the first processor developed and its main purpose is to read the dataset and separate each valid tweet based on the language and the year/month. A tweet is considered valid if it's not a retweet.

Keep in mind that this is the only processor that should receive as input the entire dataset: invalid tweets will in fact be discarded and further analysis will be performed only on a subset of the original dataset. 

A folder structure of the following type will be available at the end (one for each recognized language) and a bunch of stats regarding the program performance.

```
sort-lang
├── en
│   ├── 2020-01
│   │   ├── coronavirus-tweet-2020-01-21.json.gz
│   │   ├── coronavirus-tweet-2020-01-22.json.gz
│   │   ├── coronavirus-tweet-2020-01-23.json.gz
│   │   ├── coronavirus-tweet-2020-01-24.json.gz
│   │   ├── coronavirus-tweet-2020-01-25.json.gz
│   │   ├── coronavirus-tweet-2020-01-26.json.gz
│   │   ├── coronavirus-tweet-2020-01-27.json.gz
│   │   ├── coronavirus-tweet-2020-01-28.json.gz
│   │   ├── coronavirus-tweet-2020-01-29.json.gz
│   │   ├── coronavirus-tweet-2020-01-30.json.gz
│   │   └── coronavirus-tweet-2020-01-31.json.gz
│   ├── 2020-02
│   │   ├── coronavirus-tweet-2020-02-01.json.gz
│   │   ...
│   └── 2021-03
│       ├── coronavirus-tweet-2021-03-01.json.gz
│       ├── ...
│       └── coronavirus-tweet-2021-03-31.json.gz
├── it
│   ├── 2020-01
...
```

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/COVID-19-TweetIDs/2020-01/coronavirus-tweet-id-*.gz output \
      sort-lang
```

### analyse-emotions

This processor is used to keep track of users' emotions of a specific language: the tweet text is analysed with the use of a NCR emotion lexicon to retrieve data. The result is a csv file with rows representing days while columns emotions.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      analyse-emotions
```

To get standardized values it is possible to run the program with the ``` --standardize ``` option. Keep in mind that in this case results will be available in a folder called *standardize*.

### standardize

The standardize operation described above can also be performed at different time: the result of *analyse-emotions* can be handled by this processor to standardize values obtained previously.

This processor is also used to standardize a file with similar structure but different contents from the one obtained from *analyse-emotions*, e.g. to standardize a subset of rows.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz \
      input/analyse-emotions/en-coronavirus-tweet.csv output \
      standardize
```

### analyse-users

The purpose of this processor is to keep track of users across the whole database: for each users, it saves information regarding the number of total tweets, the number of different days the user has tweeted, their description, etc

Some of these information will be used to infer other data such as the gender of the user, in order to conduct further analysis.

**WARNING:** this processor stores information about users in RAM, this is because data such as the total number of tweets can be retrieved only after the whole dataset has been analyzed. For this kind of reason it's not a good idea to give it every language as input. 

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      analyse-users
```

### infer-users

[m3inference](https://github.com/euagendas/m3inference) was used in this processor in order to infer data from user (e.g. gender, age, if it's the account of an organization, ...) given a tweet.

The idea is to run this after *analyse-users* in order to infer data about a subset of the users (e.g. those with more than 1 tweets for example).

It is also possible to specify a cache directory that is used to store users' profile images for m3inference.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      infer-users
```

The optional parameters in this case are 

* ``` --min-tweets x ```: used to specify the minimum number of tweets (x) that a user should have to be considered 
* ``` --cache-dir y ```: the name of the cache directory where to store images and files used for the inference process. The argument passed in input name is appended to *twitter_cache_* (e.g. given *--cache-dir it*, the folder *twitter_cache_it* will be created)

### download-images

To speed up the download of the users' profile image, it's possible to run this processor in order to download them in a specific cache directory that can be used later on by *infer-users*.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      download-images --cache-dir en
```

The optional parameters in this case is

* ``` --min-tweets x ```: used to specify the minimum number of tweets that a user should have to be considered

### filter-inferred

As the name suggests, this processor is in charge of creating a new file of inferred users under specific constraints. Giving a file generated by *infer-users*, *filter-interred* will filter out all those users with values of  ```--org-acc``` or ```--gender-acc``` below a certain threshold (which can be changed), while keeping only those above (with values greater or equal) it.

It must be specified that the filter operation is performed, for each and every users, in the following way:
1. check if the user is a organization by comparing the results obtained from m3inference with ```--org-acc```. If that's the case, we are talking about an organization, otherwise go to the following step.
2. check if the user is male (or female) by comparing the results obtained from m3inference with ```--gender-acc```. If that's the case, we are talking about a male (or female), otherwise go to the following step.
3. discard the user because none of the previous constraints were satisfied

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type csv \
      input/infer-users/en-users-inference-*.gz output \
      filter-inferred --org-acc 0.95 --gender-acc 0.95
```

### aggregate-tweets

Instead of changing the main idea behind *analyse-emotions*, I have decided to write a new processor whose purpose is to aggregate tweets in weekly files. This operation was mandatory to make the data more readable. 

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      aggregate-tweets --type week
```

## License

<!--TODO change the license-->

This project is realease unde GPL v3 (or later).

```plain
graphsnapshot: process links and other data extracted from the Wikipedia dump.

Copyright (C) 2020 Critian Consonni for:
* Eurecat - Centre Tecnològic de Catalunya

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
```

See the LICENSE file in this repository for further details.

