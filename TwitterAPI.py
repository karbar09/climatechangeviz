import oauth2 as oauth
CONSUMER_KEY='mykey'
CONSUMER_SECRET='mysecret'
tokenkey='mytokenkey'
tokensecret='mytokensecret'


import oauth2 as oauth
import pymongo
import json
import math as math
import sys

def oauth_req(url, key, secret):
    consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth.Token(key=key, secret=secret)
    client = oauth.Client(consumer, token)
    resp, content = client.request(url)
    return resp,content

#loads twitter search result buffer into json object and returns the search_metadata and statuses independently
def processBuffer(search_result):
    res = json.loads(search_result)
    return res['search_metadata'],res['statuses']    

#returns min_id, as min of id's, from twitter statuses
#change this to open mongo connection and do #collection.find().sort({'id':1}).limit(1)['id']
def getmin_id(docs):
    min_id = sys.maxint
    for doc in docs:
        if doc['id'] < min_id:
            min_id = doc['id']
    if min_id != sys.maxint:
        return min_id
    else:
        print 'min_id is still sys.maxint'
        return None
    
#returns since_id, as max of max_ids, from twitter metadata  
def getsince_id(docs):
    since_id = -1
    for doc in docs:
        if doc['max_id'] > since_id:
            since_id = doc['max_id']
    if since_id != -1:
        return since_id
    else:
        print 'since_id is still -1'
        return None

#construct search url given query,count,max_id    
def constructQuery(q,count,max_id):
    base_url = 'https://api.twitter.com/1.1/search/tweets.json?'
    #some of the search url parameters
    query = 'q='+str(q)
    src = '&src=typd'
    f = '&f=realtime'
    count = '&count='+str(count)
    maxid = '&max_id='+str(max_id)
    #sinceid = '&since_id='+str(since_id)
    #concatenate url
    url = base_url + query + src + f + count + maxid
    # + sinceid
    return url

#construct search url given query,count
def constructFirstQuery(q,count):
    base_url = 'https://api.twitter.com/1.1/search/tweets.json?'
    #some of the search url parameters
    query = 'q='+str(q)
    src = '&src=typd'
    f = '&f=realtime'
    count = '&count='+str(count)
    #concatenate url
    url = base_url + query + src + f + count
    return url

#get tweets in batches of 100, the max allowable per request
def getNTweets(q, n):
    max_calls = math.ceil(n/100)
    loop = 0
    connection = pymongo.MongoClient('localhost',27017)
    db = connection['twitter']
    collection = db['statuses']
    while loop<max_calls:
        if max_calls - loop == 1:
            count = n - 100*loop
        else:
            count = 100
            
        statuses = collection.find()
        if statuses.explain()['allPlans'][0]['n'] == 0:
            url = constructFirstQuery(q,count)
            twitterToMongo(url)
            print url
            print 'Entered first'
        else:
            min_id =  getmin_id(statuses)
            print min_id
            url = constructQuery(q,count,min_id)
            twitterToMongo(url)
            print url
            print 'Entered second'

        loop += 1
        print loop
    connection.close()

def twitterToMongo(url):
    resp,search_result=oauth_req(url,tokenkey,tokensecret)
    metadata,statuses = processBuffer(search_result)
    #mongo
    #print metadata
    mongo_insert(metadata,'twitter','search_metadata',False)
    mongo_insert(statuses,'twitter','statuses',False)

##KARTHIK: This is inefficient, need to change this.
#insert twitter result buffer into mongo
def mongo_insert(docs,dbname,collectionname,dropcoll):
    connection = pymongo.MongoClient('localhost',27017)
    db = connection[dbname]
    collection = db[collectionname]
    if dropcoll:
        collection.drop()
    if (len(docs)<10000):
        collection.insert(docs)
        print 'number of docs inserted into collection %s is less than 10000'%collectionname
    else:
        collection.insert(docs[:10000])
        collection.insert(docs[10000:])
        print 'number of docs inserted into collection %s greater than 10000'%collectionname
    connection.close()

getNTweets('climate%20adaptation',10000)
