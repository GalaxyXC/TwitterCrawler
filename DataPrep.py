#!/usr/bin/env python
# encoding: utf-8

# INPUT:    'alltweets.csv' file
# OUTPUT:   'data.csv'

import re
import os
import time
import pandas as pd
import math

import nltk
from nltk.tokenize import word_tokenize, TweetTokenizer
from nltk.corpus import stopwords
from textblob import TextBlob
# os.chdir('D:\workspace\TwCrawler')

# features:
# y1 - y7
# matched y ~ x with DayDiff: 0, 15, 30, 60, 90, 180, 360
# x1            | x2                | x3                | x4                    | x5
# daily tweets  | daily (#)trends   | interactions (@)  | links (http://t.co)   | Day of Year
# x6                    | x7                    | x8    | x9 .. x10
# sentiment: %pos/all   | sentiment: %neg/all   | Year  | reserved for other features
# x11 ... x110 .. x210
# Freq of hot words


# helper method: tokenizeTweets
# @s: string
# @return lst: parse and remove: links, foreign letters, '#' => make list of tokens => filter w/ stopwords
def tokenizeTweets(s, stopWords):
    sentence = s[2:-1] # b'TWEET' -> TWEET
    linkPattern = re.compile('http[s]*://[\w.#&+=/]*')
    sentence = linkPattern.sub('', sentence)
    hashtag = re.compile('#')
    sentence = hashtag.sub('', sentence)
    foreign = re.compile(r'\\x\w\w')
    sentence = foreign.sub('', sentence)

    tknz = TweetTokenizer(reduce_len=True, strip_handles=True)  # reduce_len: shorten repeated char, handles = @XXXX
    tokens = tknz.tokenize(sentence)
    lst = [word.lower() for word in tokens if (word.lower() not in stopWords)]
    return lst

# helper method: Token Frequencies Updater
# @tokens: a list of string tokens
# @dct: a counter dictionary to be updated word's Freq.
# void function, update the 'tokenFrequencies' table
def updateTokenFrequencies(tokens, dct):
    for token in tokens:
        dct[token] = dct.get(token, 0) + 1


def dataPrep(Xfilename, Yfilename, Ydiff, wordDimension):
    # read input as raw for building X
    print('Reading X from file:')
    raw = pd.read_csv(Xfilename)

    # matching date
    dateMDY = raw.time.str.split(' ', expand=True)[0]
    raw = raw.assign(date=dateMDY)

    # calc. feature for each tweet, then use split-apply-combine (S.A.C.) to calc. daily sum for each feature

    # x1: tweets = posts
    raw = raw.assign(x1=1)

    # x2: trends = #TRENDING
    p = re.compile('#[\w]*')
    matches = raw.content.str.findall(p)
    col = matches.apply(len)
    raw = raw.assign(x2=col)

    # x3: interactions = @OTHER
    p = re.compile('@[\w]*\s')
    matches = raw.content.str.findall(p)
    col = matches.apply(len)
    raw = raw.assign(x3=col)

    # x4: links = "http://t.co"
    p = re.compile('http[s]*://')
    matches = raw.content.str.findall(p)
    col = matches.apply(len)
    raw = raw.assign(x4=col)

    # x5: Day of Year - will parse after S.A.C.
    raw = raw.assign(x5=0)

    # x6 & x7: sentiment positive/negative
    analysisSeries = raw.content.apply(TextBlob)
    sentiment = analysisSeries.apply(lambda x:x.sentiment.polarity)
    subjectivity = analysisSeries.apply(lambda x:x.sentiment.subjectivity) # weighted sentiment
    raw = raw.assign(x6=sentiment)
    raw = raw.assign(x7=(subjectivity * subjectivity))

    # x8: Year - will parse after S.A.C.
    raw = raw.assign(x8=0)

    # x9, x10 - LEAVE BLANK
    raw = raw.assign(x9=0)
    raw = raw.assign(x10=0)

    # progress message
    print('x1-x10 created.')

    # import stopwords and merge with customized stopwords and punctuation
    try:
        nltk.download('stopwords')
        stop = set(stopwords.words('english'))
    except LookupError:
        print("Stop words importing failure. Select stopwords manually")
        stop = []
    finally:
        # run finally block after custom_stopwords is updated
        custom_stopwords = ['this', 'n', 'r', 'u', '2']
        punctuation = ['!', ',', '', '.', '\\', '?', ':', '\"', '&', '-', '/', '', '(', ')', '\'', '$', '@']
        stop = stop.union(custom_stopwords).union(punctuation)

    # Tokenize all tweets into word~freq. tables:
    print('Building token frequency table')
    tokenFreq = dict()
    for sentence in raw.content:
        updateTokenFrequencies(tokenizeTweets(sentence, stop), tokenFreq)

    sortedTokenFreq = sorted(tokenFreq.items(), key=lambda x:x[1], reverse=True)

    # update x11 - x..
    raw = raw.drop(['tweetId', 'time', 'id'], axis=1)
    print('Updating x11 - x' + str(wordDimension + 10))
    for iColumn in range(wordDimension):
        if iColumn % 10 == 0:
            print('Processing',iColumn+10,'-th column.')
        token = (sortedTokenFreq[iColumn][0]) # i-th most frequent token
        col = raw.content.apply(lambda x:(1 if token in tokenizeTweets(x, stopWords=stop) else 0))
        raw.insert(raw.shape[1], iColumn, col)

    # Split-Apply(sum)-Combine
    print('S.A.C.')
    grouped = raw.groupby('date')
    X = grouped.sum() # 'content' were dropped as they cannot sum. 'date' become INDEX of X
    col = pd.to_datetime(X.index, format = '%Y-%m-%d')
    X.insert(0, 'date', col)

    # fix x5 DoY and x8 Year
    X['x5'] = X.date.dt.dayofyear
    X['x8'] = X.date.dt.year

    # fixing col and row names
    X.index.name = 'row'
    XColumnNames = ['date']
    for i in range(wordDimension+10):
        XColumnNames.append('x'+str(i+1))
    X.columns = XColumnNames

    data = X

    # read Y variables
    print('Reading Y from file:')
    Y = pd.read_csv(Yfilename)
    Y['Date'] = pd.to_datetime(Y.Date, format='%m/%d/%Y')

    earliestDate = min(X.date)
    latestDate = max(X.date)

    # subset Y using X's boundaries
    Y = Y[(Y['Date'] >= earliestDate) & (Y['Date'] <= latestDate)]
    Y = Y.reset_index()
    Y.columns = ['index', 'date', 'S&P500(TR)', 'S&P500(NetTR)', 'sp500index']
    Y = Y.drop(['index', 'S&P500(TR)', 'S&P500(NetTR)'], axis=1)

    # interpolating weekend SP500 values
    fullDates = pd.DataFrame(pd.date_range(min(Y.date), max(Y.date)))
    fullDates.columns = ['date']
    Y = pd.merge(fullDates, Y, how='left', on=['date'])

    print('Interpolating SP500 NaN values, total lines:', Y.shape[0])
    for i in range(2, Y.shape[0]-2):
        if i % 500 == 0:
            print('Processing', i, '-th row.')
        if math.isnan(Y.sp500index[i]):
            try:
                Y.sp500index[i] = Y.sp500index[i-2] + Y.sp500index[i+2]
            except:
                continue

    # setting day difference between X('s date) and Y
    print('setting prediction interval = Day Diff')
    for d in Ydiff:
        Y = pd.concat([Y.date[:-d], Y.sp500index[d:].reset_index()], axis=1, ignore_index=True, join='inner')
        Y.columns = ['date', '_', ('Y'+str(d))]

        # merge X and Y = data
        print('matching X and Y', d, ' using date')
        data = pd.merge(data, Y, how='left', on=['date'])

    return(data)

if __name__ == '__main__':

    DayDiff = [1, 7, 15, 30, 60, 90, 180, 360]

    out = dataPrep(Xfilename='alltweets.csv',
                   Yfilename="sp500index.csv",
                   Ydiff=DayDiff,
                   wordDimension=100)

    out.to_csv('data.csv')
