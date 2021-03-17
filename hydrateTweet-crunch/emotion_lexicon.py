import re
import os
from enum import Enum
from . import utils
from typing import Counter, Dict, Iterator, List

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
    # # English
    # emotionOrder = [
    #     Emotions.ANGER, Emotions.ANTICIPATION, Emotions.DISGUST, Emotions.FEAR, Emotions.JOY,
    #     Emotions.NEGATIVE, Emotions.POSITIVE,Emotions.SADNESS, Emotions.SURPRISE, Emotions.TRUST
    # ]
    # path = './assets/NRC-Emotion-Lexicon-Wordlevel-v0.92.txt'
    # with open(path) as file:
    #     lines = [l for l in file]
    #     for i in range(0, len(lines), 10):
    #         wordLine = [l for l in lines[i:i + 10]]
    #         term = wordLine[0].split('\t')[0]
    #         emotions = [Emotions.ANY]
    #         for j in range(10):
    #             if int(wordLine[j].split('\t')[2][0]) == 1:
    #                 emotions.append(emotionOrder[j])
    #         dic[term] = emotions
    #         # bitmap = 1 << 10
    #         # for j in range(10):
    #         #     bitmap += int(wordLine[j].split('\t')[2][0]) << j
    #         # dic[term] = bitmap


    # Other languages
    emotionOrder = [
        Emotions.POSITIVE, Emotions.NEGATIVE, Emotions.ANGER, Emotions.ANTICIPATION, Emotions.DISGUST,
        Emotions.FEAR, Emotions.JOY, Emotions.SADNESS, Emotions.SURPRISE, Emotions.TRUST
    ]
    path = 'hydrateTweet-crunch/assets/NRC-Emotion-Lexicon-v0.92-In105Languages-Nov2017Translations.csv'
    with open(path) as file:
        lines = [l for l in file]
        # find header column
        header = lines[0].strip('\n').split(',')[:-10]
        langCol = -1
        for i, langHeader in enumerate(header):
            if langHeader.split('(')[1].split(')')[0] == lang:
                langCol = i
                break
        try:
            if langCol < 0:
                raise Exception(f'{lang} is not a recognized language, skipping File') 
        except Exception as ex:
            utils.log(ex)
            return False

        for l in lines[1:]:
            d = l.strip('\n').split(',')
            term = d[langCol]
            emotions = [Emotions.ANY]
            for i, x in enumerate(d[-10:]):
                if x == "1":
                    emotions.append(emotionOrder[i])
            dic[term] = emotions
        
        return True


def tokenize(text: str) -> Iterator[str]:
    for match in re.finditer(r'\w+', text, re.UNICODE):
        yield match.group(0)

def isWordOfEmotion(word: str, emotion: Emotions) -> bool:
    if word in dic:
        return emotion in dic[word]
        #return (dic[word] & emotion.value) != 0
    return False

def getEmotionsOfWord(word: str) -> List[Emotions]:
    if word not in dic:
        return []
    return dic[word]
    # d = dic[word]
    # for e in Emotions:
    #     if d & e.value != 0:
    #         yield e

def countEmotionsOfWords(words: Iterator[str], c: Counter[Emotions] = Counter()) -> Counter[Emotions]:
    for w in words:
        c.update(getEmotionsOfWord(w))
    return c

def countEmotionsOfText(text: str) -> Counter[Emotions]:
    c: Counter[Emotions] = Counter()
    return countEmotionsOfWords(tokenize(text), c)