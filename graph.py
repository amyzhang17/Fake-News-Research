import json
import unicodecsv as csv
import urllib.parse as urlparse
import tweepy
import os
import time

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "client_secret.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'


# YouTube Authorization
def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


# Twitter Authorization
consumer_key = "jIHKFQUPIzHVDryIgfog2f0Ml"
consumer_secret = "DBa0VdeLuoMflgP6yAu3sdWK14FYcg4bGE1Zoc5BeYawUNwVRi"
access_token = "1739725010-oR0MS4tCK0f6Ji2kzUOBh9sLDV7zsfHNSmssnfH"
access_token_secret = "r0CVAeUjM9w1ePaz4mz9eogecs0Vn0dKhzICT2k6ABcKY"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
try:
    redirect_url = auth.get_authorization_url()
    print('TWEEPY WORKS')
except tweepy.TweepError:
    print('Error! Failed to get request token.')
# Get access token
# auth.get_access_token("verifier_value")
# Construct the API instance
api = tweepy.API(auth)


# For YouTube
def video_id(value):
    query = urlparse.urlparse(value)
    if query.hostname == 'youtu.be':
        return query.path[1:]
    if query.hostname in ('www.youtube.com', 'youtube.com'):
        if query.path == '/watch':
            p = urlparse.parse_qs(query.query)
            return p['v'][0]
        if query.path[:7] == '/embed/':
            return query.path.split('/')[2]
        if query.path[:3] == '/v/':
            return query.path.split('/')[2]
    # fail?
    return None


def channels_list_by_id(client, **kwargs):
    kwargs = remove_empty_kwargs(**kwargs)
    response = client.channels().list(**kwargs).execute()
    return response


def channels_list_by_username(client, **kwargs):
    kwargs = remove_empty_kwargs(**kwargs)
    response = client.channels().list(**kwargs).execute()
    return response


def channel_id(client, value):
    query = urlparse.urlparse(value)
    if query.hostname == 'youtu.be':
        return query.path[1:]
    if query.hostname in ('www.youtube.com', 'youtube.com'):
        if query.path[:9] == '/channel/':
            idd = query.path.split('/')[2]
            return idd
        if query.path[:6] == '/user/':
            usr = query.path.split('/')[2]
            chan = channels_list_by_username(client, part='id,snippet,contentDetails,statistics', forUsername=usr)
            return chan["items"][0]["id"]
        if query.path[:3] == '/c/':
            usr = query.path.split('/')[2]
            chan = channels_list_by_username(client, part='id,snippet,contentDetails,statistics', forUsername=usr)
            return chan["items"][0]["id"]
        if len(query.path.split('/')) == 2 or len(query.path.split('/')) == 3:
            usr = query.path.split('/')[1]
            chan = channels_list_by_username(client, part='id,snippet,contentDetails,statistics', forUsername=usr)
            return chan["items"][0]["id"]
    # fail?
    return None


# Remove keyword arguments that are not set
def remove_empty_kwargs(**kwargs):
    good_kwargs = {}
    if kwargs is not None:
        for key, value in kwargs.items():
            if value:
                good_kwargs[key] = value
    return good_kwargs


def videos_list_by_id(client, **kwargs):
    kwargs = remove_empty_kwargs(**kwargs)
    response = client.videos().list(**kwargs).execute()
    return response


def comment_threads_list_by_video_id(client, **kwargs):
    kwargs = remove_empty_kwargs(**kwargs)
    response = client.commentThreads().list(**kwargs).execute()
    return response


def comments_list(client, **kwargs):
    kwargs = remove_empty_kwargs(**kwargs)
    response = client.comments().list(**kwargs).execute()
    return response


# Start of Twitter
def TwittoDict(fileName):
    LDict = {}
    with open(fileName, 'r') as f:
        count = 0
        lin = 1
        DataList = json.load(f)
        for data in DataList:
            if "user" in data.keys() and "id" in data.keys():
                us = data["user"]
                twt = data["id"]
                TUser = dict(id=us["id_str"], name=us["name"], screen_name=us["screen_name"], url=us["url"],
                            tweet=twt)
                LDict[str(lin)] = [TUser]
                lin = lin + 1
            else:
                count = count + 1
    return LDict


def TwittoYou(fileName):
    LDict = {}
    with open(fileName) as f:
        count = 0
        for line in f:
            data = json.loads(line)
            if "user" in data.keys() and "id" in data.keys():
                us = data["user"]
                twt = data["id"]
                TUser = dict(id=us["id_str"], name=us["name"], screen_name=us["screen_name"], url=us["url"],
                             tweet=twt)
                urls = data["entities"]["urls"]
                for elem in urls:
                    line = elem["expanded_url"]
                    vid = video_id(line)
                    if vid is not None:
                        if line in LDict.keys():
                            LDict[line].append(TUser)
                        else:
                            LDict[line] = [TUser]
            else:
                count = count + 1
                # print(count)
    return LDict


def TwittoCom(Ldict):
    Cdict = dict()
    for url in Ldict.keys():
        lst = []
        for twit in Ldict[url]:
            replies = []
            name = twit['screen_name']
            time.sleep(10)
            for tweet in tweepy.Cursor(api.search, q='to:' + name, result_type='recent', timeout=999999).items(500):
                if hasattr(tweet, 'in_reply_to_status_id_str'):
                    if (tweet.in_reply_to_status_id_str == twit['id']):
                        j = json.dumps(tweet._json)
                        res = json.loads(j)
                        us = res['user']
                        TUser = dict(id=us["id_str"], url=us["url"], type="replied")
                        replies.append(TUser)
            lst.extend(replies)
        if len(lst) != 0:
            Cdict[url] = lst
            WriteJSON(Cdict, 'BBC_comment_update')
    print("Comment done")
    return Cdict


def TwittoCom1(Ldict):
    Cdict = dict()
    first = list(Ldict.keys())[0]
    temp = Ldict[first][0]
    name = temp['screen_name']
    for tweet in tweepy.Cursor(api.search, q='to:' + name, result_type='recent', timeout=999999).items(1000):
        for url in Ldict.keys():
            lst = []
            for twit in Ldict[url]:
                replies = []
                if hasattr(tweet, 'in_reply_to_status_id_str'):
                    if (tweet.in_reply_to_status_id_str == twit['id']):
                        j = json.dumps(tweet._json)
                        res = json.loads(j)
                        us = res['user']
                        TUser = dict(id=us["id_str"], url=us["url"], type="replied")
                        replies.append(TUser)
                lst.extend(replies)
            if len(lst) != 0:
                Cdict[url] = lst
    print("Comment done")
    return Cdict


def TwittoRetweet(Ldict):
    Rdict = dict()
    for url in Ldict.keys():
        list = []
        for twit in Ldict[url]:
            firstTweet = twit["tweet"]
            results = api.retweets(firstTweet)
            temp = []
            if len(results) != 0:
                for elem in results:
                    j = json.dumps(elem._json)
                    res = json.loads(j)
                    idd = res["user"]["id_str"]
                    d = dict(id=idd, url=res["user"]["url"], type="retweeted")
                    temp.append(d)
                list.extend(temp)
        if len(list) != 0:
            Rdict[url] = list
    return Rdict


def WriteText(Ldict, name):
    name = name.strip() + ".txt"
    with open(name, 'w', encoding='utf-8') as file:
        for k, v in Ldict.items():
            file.write(str(k) + " : " + str(v) + "\n")


def WriteJSON(Ldict, name):
    name = name.strip() + ".json"
    with open(name, 'w') as file:
        file.write(json.dumps(Ldict))


def WriteCSV(dict_data, csv_file):
    csv_file = csv_file.strip() + ".csv"
    with open(csv_file, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['URL', 'User'])
        for key, value in dict_data.items():
            for elem in value:
                writer.writerow([key, str(elem)])


def LoadDict(name):
    Dict = json.loads(name)
    return Dict


def YouandTwit(TDict, fileName):
    URLDict = dict()
    text_file = open(fileName, "r")
    lines = text_file.read().split(',')
    count = 0
    for k in TDict.keys():
        data = TDict[k]
        name = lines[count]
        URLDict[name] = data
        count = count + 1
    return URLDict


def YoutoCom(client, URLDict, nameC):
    datas = dict()
    count = 0
    for k in URLDict.keys():
        r = video_id(k)
        if r is not None:
            vid = videos_list_by_id(client, part='snippet,contentDetails,statistics', id=r)
            count = count + 1
            threads = []
            if len(vid["items"]) == 1:
                parent = vid["items"][0]["id"]
                parr = dict(id=parent, type="posted")
                threads.append(parr)
                comm_threads = comment_threads_list_by_video_id(client, part='snippet', videoId=parent)
                count = count + 1
                for elem in comm_threads['items']:
                    thread = dict()
                    thread['id'] = elem['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                    thread['type'] = "commented"
                    par_id = elem['snippet']['topLevelComment']['id']
                    count = count + 1
                    temp = comments_list(client, part='snippet', parentId=par_id)
                    count = count + 1
                    repl = []
                    for el in temp['items']:
                        rep = dict()
                        rep['id'] = el['snippet']['authorChannelId']['value']
                        rep['type'] = "replied"
                        repl.append(rep)
                    threads.append(thread)
                    threads.extend(repl)
                    if (count > 995):
                        print("beep")
                        WriteCSV(URLDict, nameC)
                        print("boop")
                        count = 0
            else:
                print("ERROR")
            datas[k] = threads
    return datas


def Merge(TDict, CDict, RDict):
    result = dict()
    for url in TDict.keys():
        list = []
        for tweet in TDict[url]:
            us = dict(id=tweet["id"], url=tweet["url"], type="tweeted")
            list.append(us)
        if url in CDict.keys():
            list.extend(CDict[url])
        if url in RDict.keys():
            list.extend(RDict[url])
        result[url] = list
    return result


def Hinge(client, TDict, YDict):
    HDict = dict()
    # compile list of YouTube Users
    for k in TDict.keys():
        users = TDict[k]
        for u in users:
            twitt_id = u["id"]
            if u["url"] != "None":
                url = u["url"]
                check = video_id(url)
                if check == None:
                    channel = channel_id(client, url)
                    for key in YDict.keys():
                        comms = YDict[key]
                        for elem in comms:
                            if elem["id"] == channel:
                                HDict[twitt_id] = elem["id"]
    print("working...")
    return HDict


def Work(client, fileName, name):
    LDict = TwittoYou(fileName)
    nameA = name + "_a"
    WriteText(LDict, nameA)
    WriteJSON(LDict, name)
    nameB = name + "_b"
    WriteCSV(LDict, nameB)
    nameC = name + "_c"
    TDict = YoutoCom(client, LDict, nameC)
    WriteCSV(TDict, nameC)
    nameD = name + "_d"
    print("TwittoCom")
    CDict = TwittoCom(LDict)
    WriteCSV(CDict, nameD)
    print("TwittoRetweet")
    RDict = TwittoRetweet(LDict)
    nameE = name + "_e"
    merged = Merge(LDict, CDict, RDict)
    HDict = Hinge(client, merged, TDict)
    WriteText(HDict, nameE)
    print("Complete")


def Work1(client, fileName, helpName, name):
    nameA = name + "_tweet"
    nameB = name + "_youtube"
    nameC = name + "_comment"
    nameD = name + "_retweet"
    nameE = name + "_twitter"
    nameF = name + "_hinge"
    temp = TwittoDict(fileName)
    TDict = YouandTwit(temp, helpName)
    WriteText(TDict, nameA)
    YDict = YoutoCom(client, TDict, nameB)
    WriteText(YDict, nameB)
    CDict = TwittoCom1(TDict)
    WriteText(CDict, nameC)
    RDict = TwittoRetweet(TDict)
    WriteText(RDict, nameD)
    MDict = Merge(TDict, CDict, RDict)
    WriteText(MDict, nameE)
    HDict = Hinge(client, MDict, YDict)
    WriteText(HDict, nameF)
    print("All Done")


def Data(name):
    tweets = api.user_timeline(screen_name="BBCWorld", count=20)
    DList = []
    for info in tweets:
        help = json.dumps(info._json)
        twt = json.loads(help)
        DList.append(twt)
    name1 = name + "_data.json"
    with open(name1, 'w') as outfile:
        json.dump(DList, outfile)
    print("Complete")


def main():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    client = get_authenticated_service()
    Data('BBC_tweets')
    Work1(client, 'BBC_tweets_data.json', 'BBC_videos_data.txt', 'BBC')
    #Work(client, 'blackpanther_subset_altright_20180322.json', 'Youtube_links_altright')
    #Work(client, 'blackpanther_subset_fake-scene_20180322.json', 'Youtube_links_fake-scene')
    #Work(client, 'blackpanther_subset_nonsatire_20180410.json', 'Youtube_links_nonsatire')
    #Work(client, 'blackpanther_subset_satire_20180410.json', 'Youtube_links_satire')
    #Work(client, 'controversial_topics_tweets_6_1_2018.json', 'Youtube_links_controversial')
    #Work(client, 'blackpanther_total_plus_timeline_dedupe.json', 'Youtube_links_total_plus_timeline')


if __name__ == "__main__":
    main()
