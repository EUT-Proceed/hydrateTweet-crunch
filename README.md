# hydrateTweet-crunch

This repository has been created in order to analyze the tweets (their IDs can be found at [echen102/COVID-19-TweetIDs](https://github.com/echen102/COVID-19-TweetIDs)) and study the emotional impact of Covid19 on people.

## Documentation

Crunches the Tweets from the dataset and perform different kinds of operations such as

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

This was the first processor developed and its main purpose is to read the dataset and separate each valid tweet based on the language and the year/month. A tweet is considered valid if it's not a retweet (achieved by checking if the field *'retweeted_status'* is NOT in the tweet structure).

Keep in mind that this is the only processor that should receive as input the entire dataset: invalid tweets will in fact be discarded and further analysis will be performed only on a subset of the original dataset. 

#### Output

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

The generated files are jsons but, to avoid misunderstandings, each line of every file is actually a json with the following structure (example from [Twitter Developer Documentation](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/example-payloads)):

```json
{
  "id": 1307025659294674945,
  "full_text": "Here’s an article that highlights the updates in the new Tweet payload v2 https:\/\/t.co\/oeF3ZHeKQQ",
  "lang": "en",
  "created_at": "Fri Sep 18 18:36:15 +0000 2020",
  "retweet_count": 11,
  "favorite_count": 70,
  "user": {
    "id": 2244994945,
    "id_str": "2244994945",
    "screen_name": "TwitterDev",
    "name": "Twitter Dev",
    "description": "The voice of the #TwitterDev team and your official source for updates, news, and events, related to the #TwitterAPI.",
    "location": "127.0.0.1",
    "followers_count": 513958,
    "statuses_count": 3635,
    "default_profile_image": false,
    "profile_image_url_https": "https:\/\/pbs.twimg.com\/profile_images\/1283786620521652229\/lEODkLTh_normal.jpg"
  }
}
```

The structure of the new json follows the original one obtained when using Twarc (i.e. the [Twitter API v1](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet) json response), while keeping only the relevant fields. 

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/COVID-19-TweetIDs/2020-01/coronavirus-tweet-id-*.gz output \
      sort-lang
```

### analyse-emotions

This processor is used to keep track of users' emotions of a specific language: the tweet text is analysed with the use of a NCR emotion lexicon to retrieve data.

#### Output

The result of the computation is a csv file which is available in *\<output_dir\>/analyse-emotions*.

Three different files can be generated from this processor:

1. *\<lang\>-coronavirus-tweet.csv*, the default csv where users' emotions are measured **binary** (e.g. the user 'pippo' expressed joy but not sadness); moreover, **users are considered** (NOT tweets) to keep track of the emotions, in order to avoid possible bias. This means that, if a user tweets 10 times in a day, and expresses each time anger, the final result will be that this user expressed anger. However, if only in a tweet he/she expresses sadness, this emotion will have the same weight of the anger. This preemptive measure resets itself every time a new file is analyzed.

2. *\<lang\>-coronavirus-tweet-filtered.csv*, because further computations required specific users (i.e. with at least **n tweet** in the entire dataset), this file is generated considering only users that can be found inside the file *\<output_dir\>/analyse-users/\<lang\>-users.json* (automatically generated using the [analyse-users](#analyse-users) processor)

3. *\<lang\>-coronavirus-tweet-per-category.csv*, it is possible to generate this file to keep records of **emotions expressed by different categories** (male, female, organization). In this case only the users that can be found in *\<output_dir\>/filter-inferred/\<lang\>-inferred-users.csv* (i.e.first identified by m3inference using [infer-users](#infer-users), and then filtered under specific criteria with [filter-inferred](#filter-inferred)) are considered and processed.

Aside from the files above, it is possible to use the optional argument ``` --per-tweet```, and generate a new file with the same purpose as the previous ones, but with a critical difference: in this case, **tweets are considered** (and NOT users as stated before), which means that emotions are still measured binary, but tweets from the same user are considered individually.

In any case, the structure of the files, regardless of whether ``` --per-tweet``` is used, is the following: 

1. for file 1. and 2. 
      
      * one field named **date**, that indicates when the tweets were created
      * one field named **\<emotion\>** for each emotion present in the lexicon (beside from ANY), that indicates the proportion of users that expressed \<emotion\>
      * one field named **total**, that indicates the total number of users
      * one field named **\<emotion\>_count** for each emotion present in the lexicon (beside from ANY), that indicates how many users expressed \<emotion\>

2. for file 3. the syntax is almost the same but:

      * instead of the **\<emotion\>** field, one named **\<category\>_\<emotion\>** is available and it indicates, for each emotion and category, the proportion of users who belong to \<category\> that expressed \<emotion\>
      * instead of the **\<emotion\>_count** field, one named **\<category\>_\<emotion\>_count** is available and it indicates, for each emotion and category, how many users who belong to \<category\> expressed \<emotion\>

If the optional parameter ```--standardize``` is used, then *\<output_dir\>/standardize/\<name_of_the_previous_file\>-standardized.csv* will be generated, where **\<name_of_the_previous_file\>** is one of the files discussed above. This new file will contain standardized values, which are calculated in the following way: **z-scores = (values - mean) / std**.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      analyse-emotions
```

##### Optional Parameters

* ```--standardize```: generate another file where values are standardized using z-score (i.e. (values - mean) / std)
* ```--per-tweet```: consider each tweet indipendently
* ```--filter-users {per-category, per-tweet-number}```: filter users in three main categories (male, female, org) or based on their number of tweets over the dataset

### analyse-users

The purpose of this processor is to keep track of the users across the whole dataset: for each user, informations such as the number of total tweets, the number of days he/she tweeted, their description and so on, are saved in a json file. Furthermore, the file generated from this processor will be also used

* to filter users by [analyse-emotions](#analyse-emotions)
* to infer gender, age and if the profile belongs to an organization by [infer-users](#infer-users)

**WARNING:** this processor stores information about users in RAM, this is because data such as the total number of tweets can be retrieved only after the whole dataset has been analyzed. For this kind of reason it's not a good idea to give it every language as input.

#### Output

At the end of the process one or more file named *\<lang\>-users.json* are generated inside of *\<output_dir\>/analyse-users/*. It is mandatory to specify that, for **each line**, a json with the following sintax can be found:

```json
{
	"id_str": "2244994945",
	"screen_name": "TwitterDev",
	"name": "Twitter Dev",
	"tweets": 12,
	"days_tweeted": 2,
	"location": "127.0.0.1",
	"description": "The voice of the #TwitterDev team and your official source for updates, news, and events, related to the #TwitterAPI.",
	"followers_count": 513958,
	"statuses_count": 3635,
	"profile_image_url_https": "https:\/\/pbs.twimg.com\/profile_images\/1283786620521652229\/lEODkLTh_normal.jpg",
	"default_profile_image": false
}
```

Most of the fields come from the [Twitter API v1](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet) response, however

* *tweets*, which indicates how many tweets with *id_str* there are in the dataset for a specific language
* *days_tweeted*, which indicates how many days the user has tweeted

are added to the base json structure.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      analyse-users
```

##### Optional Parameters

* ```--min-tweets {n}```: where *n* indicates the minimum number of tweets that a user should have in order to be saved in the output file [default: 1]

### infer-users

[m3inference](https://github.com/euagendas/m3inference) was used in this processor in order to infer data (i.e. gender, age, if it's the account of an organization) from users who can be found in the file generated from [analyse-users](#analyse-users) (i.e. with a certain number of tweets).

**WARNING**: To avoid errors during the process, it's better to download the code from m3inference GitHub repository, instead of installing the library using pip (it may not be up to date with the latest changes).

Due to the fact that users' profile images are needed in the process, it is also possible to download them separately and specify the cache directory where they are stored (see [download-images](#download-images) for further informations).

#### Output

After the inference process has ended, the file *\<output_dir\>/infer-users/\<lang\>-users-inference-\<pid\>.csv* will be available with the following structure:

* **id_str**, the id of the user
* **screen_name**, the screen name of the user 
* **name**, the name of the user
* **tweets**, the number of tweets the user sent (w.r.t the tweets in the dataset)
* **days_tweeted**, the number of days the user tweeted
* **location**, the location specified by the user
* **gender**, the gender inferred by m3inference (male or female)
* **gender_acc**, the confidence for the gender prediction of m3inference
* **age**, the age inferred by m3inference (<40 or >=40)
* **age_acc**, the confidence for the age prediction of m3inference
* **age_>=40_acc**, the confidence for the >=40 age prediction of m3inference
* **age_30-39_acc**, the confidence for the 30-39 age prediction of m3inference
* **age_19-29_acc**, the confidence for the 19-29 age prediction of m3inference
* **age_<=18_acc**, the confidence for the <=18 age prediction of m3inference
* **org**, indicates if the profile belongs to an organization (True or False)
* **org_acc**, the confidence for the org prediction of m3inference

It is important to underline that fields after **location** are added based on m3inference results and that all the *accuracies* are actually numbers between 0 and 1.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      infer-users
```

##### Optional Parameters

* ```--min-tweets {n}```: where *n* indicates the minimum number of tweets that a user should have in order to be processed [default: 2] 
* ```--cache-dir {name}```: where *name* is appended to *twitter_cache_* and indicates the cache directory where images and files used for the inference process are stored.
* ```--delete```: delete the cache directory after the process is complete.

### download-images

To speed up the download of the users' profile images, it's possible to run multiple instances of this processor and save them in the same specific cache directory. The same cache directory can be used later on by [infer-users](#infer-users) adn the images already inside the folder won't be downloaded.

#### Output

Each image that can be retrieved from Twitter (sometimes links don't work), will be downloaded and then resized. The final results will be a file named *\<id_str\>_224x224.jpg*, where *id_str* is the id of the user with that profile picture.


#### Usage Example

Single execution:

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      download-images --cache-dir en
```

To run multiple processes in parallel:

```bash
$ find output/analyse-users -name en-users-a*.gz -type f | parallel -j8 \
      --progress python3 -m hydrateTweet-crunch --output-compression gz \
      --input-type json input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      download-images --cache-dir en
```

You can split a file into multiple parts using:

```bash
$ split en-users.json en-users- -n l/8 --additional-suffix .json
```

##### Optional Parameters

* ```--min-tweets {n}```: where *n* indicates the minimum number of tweets that a user should have in order to be processed [default: 2]

### filter-inferred

As the name suggests, this processor is in charge of creating a new file of inferred users under specific constraints. Given a file generated by [infer-users](#infer-users), *filter-interred* will filter out all those users with values of ```--org-acc``` or ```--gender-acc``` below a certain threshold (which can be changed), while keeping only those above (with values greater or equal) it.

It must be specified that the filter operation is performed, for each and every user, in the following way:
1. check if the user is an organization by comparing the results obtained from m3inference with ```--org-acc```. If that's the case, we are talking about an organization, otherwise go to the following step.
2. check if the user is male (or female) by comparing the results obtained from m3inference with ```--gender-acc```. If that's the case, we are talking about a male (or female), otherwise go to the following step.
3. discard the user because none of the previous constraints were satisfied

#### Output

This processor will produce a csv file named *\<lang\>-inferred-users.csv*, which can be found inside *\<output_dir\>/filter-inferred*. 

The structure of the csv is the same that it's used for [infer-users](#infer-users), however

* **category**, which indicates the category of the user (male, female or org)

has been added for further processing.

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type csv \
      input/infer-users/en-users-inference-*.gz output \
      filter-inferred
```

##### Optional Parameters

* ```--gender-acc```: the minimum gender accuracy a user should (at least) have in order to be considered [default: 0.95]
* ```--org-acc```: the minimum organization accuracy an organization should (at least) have in order to be considered [default: 0.95]

### aggregate-tweets

Given the fact that an analysis of daily tweets can lead to very confusing results, I've decided to write a new processor whose purpose is to aggregate them in weekly batches. In this way, the main purpose of several processors, such as [analyse-emotions](#analyse-emotions), remained the same but data became more readable.

#### Output

Several json files will be created inside *\<output_dir\>/aggregate-tweets/groupby_week/\<lang\>*, named *coronavirus-tweet-YYYY-MM-DD.json.gz*, where **YYYY-MM-DD** indicates the first day of the week the file refers to.

The structure of the json file it's the same of [sort-lang](#sort-lang).

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/20*/coronavirus-tweet-*.gz output \
      aggregate-tweets --type week
```

### calc-words-frequency

After analyzing (and visualizing) some of the results from [analyse-emotions](#analyse-emotions), I've focused only on particular days (e.g. when users expressed lots of emotions and peaks could be seen) in order to understand which were the most used words and which emotions they conveyed.

#### Output

A csv file named *\<lang\>-coronavirus-tweets-YYYY-MM-DD-top-\<n\>-words.csv* will be available inside the folder *\<output_dir\>/words-frequency* after the computation. 

The following fields can be accessed inside of the file:

* **word**, the word used 
* **occurrences**, the number of times the word occurred in the file
* **occ/words**, the number of times the word occurred in the file w.r.t. the number of words (which convey at least an emotion)
* **occ/tweets**, the number of times the word occurred in the file w.r.t. the number of tweets
* **emotion**, the emotion conveyed by the word according to the lexicon

#### Usage Example

```bash
$ python3 -m hydrateTweet-crunch --output-compression gz --input-type json \
      input/sort-lang/en/2020-01/coronavirus-tweet-2020-01-20.json.gz output \
      calc-words-frequency
```

##### Optional Parameters

* ```--n-words {n}```: where *n* indicates the number of words per emotion that will be saved on the output file [default: 30]

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

