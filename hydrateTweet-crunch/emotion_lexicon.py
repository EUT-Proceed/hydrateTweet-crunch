import re
import os
import csv
import spacy
from enum import Enum
from . import utils
from typing import Counter, Dict, Iterator, List

EMOTIONS = ["Positive", "Negative", "Anger", "Anticipation", "Disgust", "Fear", "Joy", "Sadness", "Surprise", "Trust"]

SPACY_SUPP_LANG = {'en': 'en_core_web_sm', 'es': 'es_core_news_sm', 'it': 'it_core_news_sm'}

class Emotions(Enum):
    ANGER = 1 << 0
    ANTICIPATION = 1 << 1
    DISGUST = 1 << 2
    FEAR = 1 << 3
    JOY = 1 << 4
    NEGATIVE = 1 << 5
    POSITIVE = 1 << 6
    SADNESS = 1 << 7
    SURPRISE = 1 << 8
    TRUST = 1 << 9
    ANY = 1 << 10
    def __int__(self):
        return self.value

nlp = {}

dic: Dict[str, List[Emotions]] = {}

def getEmotionName(emotion: Emotions) -> str:
    if emotion == Emotions.ANGER:
        return "anger"
    if emotion == Emotions.ANTICIPATION:
        return "anticipation"
    if emotion == Emotions.DISGUST:
        return "disgust"
    if emotion == Emotions.FEAR:
        return "fear"
    if emotion == Emotions.JOY:
        return "joy"
    if emotion == Emotions.NEGATIVE:
        return "negative"
    if emotion == Emotions.POSITIVE:
        return "positive"
    if emotion == Emotions.SADNESS:
        return "sadness"
    if emotion == Emotions.SURPRISE:
        return "surprise"
    if emotion == Emotions.TRUST:
        return "trust"
    if emotion == Emotions.ANY:
        return "analized words"
    return "unknown"

def initEmotionLexicon(lang = 'en') -> bool:
    emotionOrder = [
        Emotions.POSITIVE, Emotions.NEGATIVE, Emotions.ANGER, Emotions.ANTICIPATION, Emotions.DISGUST,
        Emotions.FEAR, Emotions.JOY, Emotions.SADNESS, Emotions.SURPRISE, Emotions.TRUST
    ]
    path = 'hydrateTweet-crunch/assets/NRC-Emotion-Lexicon-v0.92-In105Languages-Nov2017Translations.csv'

    with open(path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for i, line in enumerate(csv_reader):
            if i == 0:
                languages =  {}
                n_languages = len(line) - 11
                count = 0
                for column_name in line:
                    if count > n_languages:
                        break
                    languages[column_name.split('(')[-1][:-1]] = column_name
                    count += 1
            if not lang in languages:
                utils.log(f'{lang} is not a recognized language, skipping File')
                return False
            term = line[languages[lang]]
            emotions = [Emotions.ANY]
            for j, emotion in enumerate(EMOTIONS):
                if emotion in line and line[emotion] == "1":
                    emotions.append(emotionOrder[j])
            dic[term] = emotions

    if lang in SPACY_SUPP_LANG:
        utils.log(f'Loading {lang} for spacy')
        global nlp
        nlp = spacy.load(SPACY_SUPP_LANG[lang])
    else:
        utils.log(f'{lang} is not supported by spacy, using default tokenizer')

    return True


def tokenize(text: str) -> Iterator[str]:
    if nlp:
        for word in nlp(u'{}'.format(text)):
            yield word.text
    else:
        for match in re.finditer(r'\w+', text, re.UNICODE):
            yield match.group(0)

def isWordOfEmotion(word: str, emotion: Emotions) -> bool:
    if word in dic:
        return emotion in dic[word]
    return False

def getEmotionsOfWord(word: str) -> List[Emotions]:
    if word not in dic:
        return []
    return dic[word]

def countEmotionsOfWords(words: Iterator[str], c: Counter[Emotions] = Counter()) -> Counter[Emotions]:
    for w in words:
        c.update(getEmotionsOfWord(w.lower()))
    return c

def countEmotionsOfText(text: str) -> Counter[Emotions]:
    c: Counter[Emotions] = Counter()
    return countEmotionsOfWords(tokenize(text), c)