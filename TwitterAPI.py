import oauth2 as oauth
CONSUMER_KEY='mykey'
CONSUMER_SECRET='mysecret'
tokenkey='mytokenkey'
tokensecret='mytokensecret'


def oauth_req(url, key, secret, http_method="GET", post_body=None,
        http_headers=None):
    consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth.Token(key=key, secret=secret)
    client = oauth.Client(consumer, token)
 
    resp, content = client.request(
        url,
        method=http_method,
        body=post_body,
        headers=http_headers,
        force_auth_header=True
    )
    return content

url='https://api.twitter.com/1.1/search/tweets.json?q=climate%20adaptation'
search_result=oauth_req(url,tokenkey,tokensecret)
print search_result.split(',')