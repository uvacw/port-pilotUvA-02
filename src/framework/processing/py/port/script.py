import logging
import json
import io

import pandas as pd

import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandUIRender)

from ddpinspect import unzipddp
from ddpinspect import twitter
from ddpinspect import instagram
from ddpinspect import youtube
from ddpinspect import facebook
from ddpinspect import tiktok
from ddpinspect import scanfiles

from ddpinspect.validate import Language
from ddpinspect.validate import DDPFiletype

from datetime import datetime
def fix_timestamp(timestamp):
    try:
        return str(datetime.utcfromtimestamp(timestamp))
    except Exception as e:
        return str(e)



LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("yolo")

TABLE_TITLES = {
    "twitter_interests": props.Translatable(
        {
            "en": "Your interests according to Twitter:",
            "nl": "Jouw interesses volgens Twitter:",
        }
    ),

    "twitter_likes": props.Translatable(
        {
            "en": "Your likes according to Twitter:",
            "nl": "Jouw likes volgens Twitter:",
        }
    ),

    "twitter_following": props.Translatable(
        {
            "en": "Accounts you follow according to Twitter:",
            "nl": "Profielen door jou gevold volgens Twitter:",
        }
    ),

    "twitter_adengagement": props.Translatable(
        {
            "en": "Your activities with ads according to Twitter:",
            "nl": "Jouw activiteiten met ads volgens Twitter:",
        }
    ),

    "twitter_tweets": props.Translatable(
        {
            "en": "Your tweets according to Twitter:",
            "nl": "Jouw tweets volgens Twitter:",
        }
    ),

    "twitter_userlinkclicks": props.Translatable(
        {
            "en": "Your clicks according to Twitter:",
            "nl": "Jouw kliks volgens Twitter:",
        }
    ),
    "twitter_account_created_at": props.Translatable(
        {
            "en": "Date of your account creation on Twitter:",
            "nl": "Datum waarop je account is aangemaakt op Twitter:",
        }
    ),
    "instagram_your_topics": props.Translatable(
        {
            "en": "Topics in which you are interested in according to Instagram:",
            "nl": "Onderwerpen waar jij volgens Instagram geintereseerd in bent:",
        }
    ),
    "instagram_interests": props.Translatable(
        {
            "en": "Your interests according to Instagram:",
            "nl": "Jouw interesses volgens Instagram:",
        }
    ),
    "instagram_account_created_at": props.Translatable(
        {
            "en": "Date of your account creation on Instagram:",
            "nl": "Datum waarop je account is aangemaakt op Instagram:",
        }
    ),
    "instagram_your_topics": props.Translatable(
        {
            "en": "Topics in which you are interested in according to Instagram:",
            "nl": "Onderwerpen waar jij volgens Instagram geintereseerd in bent:",
        }
    ),
    "instagram_interests": props.Translatable(
        {
            "en": "Your interests according to Instagram:",
            "nl": "Jouw interesses volgens Instagram:",
        }
    ),
    "instagram_account_created_at": props.Translatable(
        {
            "en": "Date of your account creation on Instagram:",
            "nl": "Datum waarop je account is aangemaakt op Instagram:",
        }
    ),
    "instagram_your_topics": props.Translatable(
        {
            "en": "Topics in which you are interested in according to Instagram:",
            "nl": "Onderwerpen waar jij volgens Instagram geintereseerd in bent:",
        }
    ),
    "instagram_interests": props.Translatable(
        {
            "en": "Your interests according to Instagram:",
            "nl": "Jouw interesses volgens Instagram:",
        }
    ),
    "instagram_account_created_at": props.Translatable(
        {
            "en": "Date of your account creation on Instagram:",
            "nl": "Datum waarop je account is aangemaakt op Instagram:",
        }
    ),
    "instagram_videos_watched": props.Translatable(
        {
            "en": "Videos watched according to Instagram:",
            "nl": "Video's bekeken volgens Instagram",
        }
    ),
    "instagram_posts_viewed": props.Translatable(
        {
            "en": "Posts viewed according to Instagram:",
            "nl": "Gezien posts volgens Instagram:",
        }
    ),
    "instagram_ads_viewed": props.Translatable(
        {
            "en": "Ads viewed on Instagram:",
            "nl": "Advertenties gezien op Instagram:",
        }
    ),    
    "instagram_post_comments": props.Translatable(
        {
            "en": "Post comments on Instagram:",
            "nl": "Post comments op Instagram:",
        }
    ),    
    "instagram_reels_comments": props.Translatable(
        {
            "en": "Reels comments on Instagram:",
            "nl": "Reels comments op Instagram:",
        }
    ),    
    "instagram_following": props.Translatable(
        {
            "en": "Following on Instagram:",
            "nl": "Following op Instagram:",
        }
    ),    
    "instagram_liked_comments": props.Translatable(
        {
            "en": "Liked comments on Instagram:",
            "nl": "Liked comments op Instagram:",
        }
    ),    
    "instagram_liked_posts": props.Translatable(
        {
            "en": "Liked posts on Instagram:",
            "nl": "Liked posts op Instagram:",
        }
    ), 
    "instagram_story_likes": props.Translatable(
        {
            "en": "Liked stories on Instagram:",
            "nl": "Liked stories op Instagram:",
        }
    ), 
    "facebook_your_topics": props.Translatable(
        {
            "en": "Topics in which you are interested in according to Facebook:",
            "nl": "Onderwerpen waar jij volgens Facebook geintereseerd in bent:",
        }
    ),
    "facebook_interests": props.Translatable(
        {
            "en": "Your interests according to Facebook:",
            "nl": "Jouw interesses volgens Facebook:",
        }
    ),
    "facebook_account_created_at": props.Translatable(
        {
            "en": "Date of your account creation on Facebook:",
            "nl": "Datum waarop je account is aangemaakt op Facebook:",
        }
    ),
    "facebook_recently_viewed": props.Translatable(
        {
            "en": "Items recently viewed on Facebook:",
            "nl": "Items recently viewed on Facebook:",
        }
    ),
    "facebook_recently_visited": props.Translatable(
        {
            "en": "Items recently visited on Facebook:",
            "nl": "Items recently visited on Facebook:",
        }
    ),
    "facebook_posts_and_comments": props.Translatable(
        {
            "en": "Reactions to posts and comments on Facebook:",
            "nl": "Reactions to posts and comments on Facebook:",
        }
    ),    
    "youtube_watch_history": props.Translatable(
        {
            "en": "Videos you watched on YouTube:",
            "nl": "Videos die je op YouTube hebt gekeken:",
        }
    ),
    "youtube_subscriptions": props.Translatable(
        {
            "en": "Channels you are subscribed to on Youtube:",
            "nl": "Kanalen waarop je geabboneerd bent op Youtube:",
        }
    ),
    "youtube_comments": props.Translatable(
        {
            "en": "Comments you posted on Youtube:",
            "nl": "Reacties die je hebt geplaats op Youtube:",
        }
    ),
    "tiktok_data": props.Translatable(
        {
            "en": "AANPASSEN: Your data on TikTok",
            "nl": "AANPASSEN: Jouw data op TikTok",
        }
    ),
    "empty_result_set": props.Translatable(
        {
            "en": "We could not extract any data:",
            "nl": "We konden de gegevens niet in je donatie vinden:",
        }
    ),
}


def process(sessionId):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{sessionId}-tracking")

    platforms = [
        ("Twitter", extract_twitter),
        ("Instagram", extract_instagram),
        ("Facebook", extract_facebook),
        ("TikTok", extract_tiktok),        
        # ("YouTube", extract_youtube),
    ]

    # progress in %
    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    for platform in platforms:
        platform_name, extraction_fun = platform
        data = None

        # STEP 1: select the file
        progress += step_percentage
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{sessionId}-tracking")

            promptFile = prompt_file("application/zip, text/plain", platform_name)
            fileResult = yield render_donation_page(platform_name, promptFile, progress)

            if fileResult.__type__ == "PayloadString":
                validation, extractionResult = extraction_fun(fileResult.value)

                # Flow: Three paths
                # 1: Extracted result: continue
                # 2: No extracted result: valid package, generated empty df: continue
                # 3: No extracted result: not a valid package, retry loop

                if extractionResult:
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    data = extractionResult
                    break
                elif (validation.status_code.id == 0 and not extractionResult and validation.ddp_category is not None):
                    LOGGER.info("Valid zip for %s; No payload", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    data = return_empty_result_set()
                    break
                elif validation.ddp_category is None:
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name), progress)

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{sessionId}-tracking")
                        #data = return_empty_result_set()
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")
                break

        # STEP 2: ask for consent
        progress += step_percentage

        if data is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{sessionId}-tracking")
            prompt = prompt_consent(platform_name, data)
            consent_result = yield render_donation_page(platform_name, prompt, progress)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")

    yield render_end_page()


##################################################################
# helper functions

def prompt_consent(platform_name, data):
    table_list = []

    for k, v in data.items():
        df = v["data"]
        table = props.PropsUIPromptConsentFormTable(f"{platform_name}_{k}", v["title"], df)
        table_list.append(table)

    return props.PropsUIPromptConsentForm(table_list, [])


def return_empty_result_set():
    result = {}

    df = pd.DataFrame(["No data found"], columns=["No data found"])
    result["empty"] = {"data": df, "title": TABLE_TITLES["empty_result_set"]}

    return result


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream

    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


##################################################################
# Extraction functions

def extract_twitter(twitter_zip):
    result = {}

    validation = twitter.validate_zip(twitter_zip)

    # interests_bytes = unzipddp.extract_file_from_zip(twitter_zip, "personalization.js")
    # interests_listdict = twitter.bytesio_to_listdict(interests_bytes)
    # interests = twitter.interests_to_list(interests_listdict)

    # if interests:
    #     df = pd.DataFrame(interests, columns=["Interests"])
    #     result["interests"] = {"data": df, "title": TABLE_TITLES["twitter_interests"]}
 
    # account_created_at_bytes = unzipddp.extract_file_from_zip(twitter_zip, "account.js")  
    # account_created_at_listdict = twitter.bytesio_to_listdict(account_created_at_bytes)
    # account_created_at = twitter.account_created_at_to_list(account_created_at_listdict)

    # if account_created_at:
    #     df = pd.DataFrame(account_created_at, columns=["Account created at"])
    #     result["account_created_at"] = {"data": df, "title": TABLE_TITLES["twitter_account_created_at"]}

    ### PILOT 02 CODE

    like_bytes = unzipddp.extract_file_from_zip(twitter_zip, "like.js")
    like_listdict = twitter.bytesio_to_listdict(like_bytes)
    likes = [{'tweetdId': item['like']['tweetId'], 'text': item['like']['fullText']} for item in like_listdict if 'like' in item.keys()]


    if likes:
        df = pd.DataFrame(likes)
        result["likes"] = {"data": df, "title": TABLE_TITLES["twitter_likes"]}


    following_bytes = unzipddp.extract_file_from_zip(twitter_zip, "following.js")
    following_listdict = twitter.bytesio_to_listdict(following_bytes)
    following = [{'accountId': item['following']['accountId']} for item in following_listdict if 'following' in item.keys()]


    if following:
        df = pd.DataFrame(following)
        result["following"] = {"data": df, "title": TABLE_TITLES["twitter_following"]}

    adengagement_bytes = unzipddp.extract_file_from_zip(twitter_zip, "ad-engagements.js")
    adengagement_listdict = twitter.bytesio_to_listdict(adengagement_bytes)
    adengagement = []
    for item in adengagement_listdict:
        res = {}
        try: res['advertiserName'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['advertiserInfo']['advertiserName']
        except: 
            pass
        try: res['advertiserscreenName'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['advertiserInfo']['screenName']
        except: 
            pass
        try: res['impressionTime'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['impressionTime']
        except: 
            pass
        try: res['tweetId'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['promotedTweetInfo']['tweetId'] 
        except: 
            pass
        try: res['tweetText'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['promotedTweetInfo']['tweetText']
        except: 
            pass
        try: res['trendId'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['promotedTrendInfo']['trendId']
        except: 
            pass
        try: res['trendname'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['promotedTrendInfo']['name']
        except: 
            pass
        try: res['trenddescription'] = item['ad']['adsUserData']['adEngagements']['engagements'][0]['impressionAttributes']['promotedTrendInfo']['description']
        except: 
            pass
        try: res['engagementAttributes'] = str(item['ad']['adsUserData']['adEngagements']['engagements'][0]['engagementAttributes'])
        except: 
            pass


        adengagement.append(res)

    if adengagement:
        df = pd.DataFrame(adengagement)
        result["adengagement"] = {"data": df, "title": TABLE_TITLES["twitter_adengagement"]}


    ## AGGREGATE --> one table for user mentions (user --> # of mentions) one table for user replies (user --> # of replies)

    # tweets_bytes = unzipddp.extract_file_from_zip(twitter_zip, "tweet.js")
    # tweets_listdict = twitter.bytesio_to_listdict(tweets_bytes)
    # tweets = []
    # for item in tweets_listdict:
    #     res = {}
    #     try: res['in_reply_to_user_id'] = str(item['tweet']['in_reply_to_user_id'])
    #     except: 
    #         pass
    #     try: res['in_reply_to_screen_name'] = str(item['tweet']['in_reply_to_screen_name'])
    #     except: 
    #         pass
    #     try: res['full_text'] = str(item['tweet']['full_text'])
    #     except: 
    #         pass
    #     try: res['full_text'] = str(item['tweet']['full_text'])
    #     except: 
    #         pass
    #     try: res['retweet_count'] = str(item['tweet']['retweet_count'])
    #     except: 
    #         pass
    #     try: res['favorite_count'] = str(item['tweet']['favorite_count'])
    #     except: 
    #         pass
    #     try: res['urls'] = str(item['tweet']['entities']['urls'])
    #     except: 
    #         pass


    #     tweets.append(res)


    # if tweets:
    #     df = pd.DataFrame(tweets)
    #     result["tweets"] = {"data": df, "title": TABLE_TITLES["twitter_tweets"]}


    # userlinkclicks_bytes = unzipddp.extract_file_from_zip(twitter_zip, "user-link-clicks.js")
    # userlinkclicks_listdict = twitter.bytesio_to_listdict(userlinkclicks_bytes)
    # userlinkclicks = [{'res': str(userlinkclicks_listdict)},]

    # if userlinkclicks:
    #     df = pd.DataFrame(userlinkclicks)
    #     result["userlinkclicks"] = {"data": df, "title": TABLE_TITLES["twitter_userlinkclicks"]}



    return validation, result


def extract_instagram(instagram_zip):
    result = {}

    validation = instagram.validate_zip(instagram_zip)

    # interests_bytes = unzipddp.extract_file_from_zip(instagram_zip, "ads_interests.json")
    # interests_dict = unzipddp.read_json_from_bytes(interests_bytes)
    # interests = instagram.interests_to_list(interests_dict)
    # if interests:
    #     df = pd.DataFrame(interests, columns=["Interests"])
    #     result["interests"] = {"data": df, "title": TABLE_TITLES["instagram_interests"]}

    # your_topics_bytes = unzipddp.extract_file_from_zip(instagram_zip, "your_topics.json")
    # your_topics_dict = unzipddp.read_json_from_bytes(your_topics_bytes)
    # your_topics = instagram.your_topics_to_list(your_topics_dict)
    # if your_topics:
    #     df = pd.DataFrame(your_topics, columns=["Your Topics"])
    #     result["your_topics"] = {"data": df, "title": TABLE_TITLES["instagram_your_topics"]}
  
    # account_created_at_bytes = unzipddp.extract_file_from_zip(instagram_zip, "signup_information.json")
    # account_created_at_dict = unzipddp.read_json_from_bytes(account_created_at_bytes)
    # account_created_at = instagram.account_created_at_to_list(account_created_at_dict)
    # if account_created_at:
    #     df = pd.DataFrame(account_created_at, columns=["Account created at"])
    #     result["account_created_at"] = {"data": df, "title": TABLE_TITLES["instagram_account_created_at"]}

    ads_viewed_bytes = unzipddp.extract_file_from_zip(instagram_zip, "ads_viewed.json")
    ads_viewed_dict = unzipddp.read_json_from_bytes(ads_viewed_bytes)

    ads_viewed = [{'Author': item['string_map_data']['Author']['value'],
                    'Timestamp' : item['string_map_data']['Time']['timestamp']}
                    for item in ads_viewed_dict['impressions_history_ads_seen']
        ]
    
    
    if ads_viewed:
        df = pd.DataFrame(ads_viewed)
        df_agg = pd.DataFrame(df['Author'].value_counts()).reset_index().rename(columns={'index': 'Author', 'Author': 'Number of views'})
        df_timestamps_min = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Earliest view'})
        df_timestamps_max = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Latest view'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='Author')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='Author')

        df_agg['Earliest view'] = df_agg['Earliest view'].apply(fix_timestamp)
        df_agg['Latest view'] = df_agg['Latest view'].apply(fix_timestamp)

        result["ads_viewed"] = {"data": df_agg, "title": TABLE_TITLES["instagram_ads_viewed"]}


    posts_viewed_bytes = unzipddp.extract_file_from_zip(instagram_zip, "posts_viewed.json")
    posts_viewed_dict = unzipddp.read_json_from_bytes(posts_viewed_bytes)

    posts_viewed = [{'Author': item['string_map_data']['Author']['value'],
                    'Timestamp' : item['string_map_data']['Time']['timestamp']
                    }
                    for item in posts_viewed_dict['impressions_history_posts_seen']
        ]
    
    
    if posts_viewed:
        df = pd.DataFrame(posts_viewed)
        df_agg = pd.DataFrame(df['Author'].value_counts()).reset_index().rename(columns={'index': 'Author', 'Author': 'Number of views'})
        df_timestamps_min = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Earliest view'})
        df_timestamps_max = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Latest view'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='Author')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='Author')

        df_agg['Earliest view'] = df_agg['Earliest view'].apply(fix_timestamp)
        df_agg['Latest view'] = df_agg['Latest view'].apply(fix_timestamp)

        result["posts_viewed"] = {"data": df_agg, "title": TABLE_TITLES["instagram_posts_viewed"]}

    
    videos_watched_bytes = unzipddp.extract_file_from_zip(instagram_zip, "videos_watched.json")
    videos_watched_dict = unzipddp.read_json_from_bytes(videos_watched_bytes)
    # videos_watched = [{'res': str(videos_watched_dict)},]


    videos_watched = [{'Author': item['string_map_data']['Author']['value'],
                    'Timestamp' : item['string_map_data']['Time']['timestamp'],
                    'Time': item['string_map_data']['Time']['value']}
                    for item in videos_watched_dict['impressions_history_videos_watched']
        ]


    if videos_watched:
        df = pd.DataFrame(videos_watched)
        df_agg = pd.DataFrame(df['Author'].value_counts()).reset_index().rename(columns={'index': 'Author', 'Author': 'Number of videos watched'})
        df_timestamps_min = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Earliest video'})
        df_timestamps_max = df[['Author', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['Author']).rename(columns={'Timestamp': 'Latest video'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='Author')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='Author')

        df_agg['Earliest video'] = df_agg['Earliest video'].apply(fix_timestamp)
        df_agg['Latest video'] = df_agg['Latest video'].apply(fix_timestamp)

        result["videos_watched"] = {"data": df_agg, "title": TABLE_TITLES["instagram_videos_watched"]}


    post_comments_bytes = unzipddp.extract_file_from_zip(instagram_zip, "post_comments.json")
    post_comments_dict = unzipddp.read_json_from_bytes(post_comments_bytes)
    # post_comments = [{'res': str(post_comments_dict)},]


    post_comments = [{'Media owner': item['string_map_data']['Media owner']['value'],
                    'Timestamp' : item['string_map_data']['Comment creation time']['timestamp'],
                    # 'Time': item['string_map_data']['Comment creation time']['value'],
                    # 'Comment': item['string_map_data']['Comment']['value'],
                    }
                    for item in post_comments_dict['comments_media_comments']]
    
    if post_comments:
        df = pd.DataFrame(post_comments)
        df_agg = pd.DataFrame(df['Media owner'].value_counts()).reset_index().rename(columns={'index': 'Media owner', 'Media owner': 'Number of comments'})
        df_timestamps_min = df[['Media owner', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['Media owner']).rename(columns={'Timestamp': 'Earliest comment'})
        df_timestamps_max = df[['Media owner', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['Media owner']).rename(columns={'Timestamp': 'Latest comment'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='Media owner')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='Media owner')

        df_agg['Earliest comment'] = df_agg['Earliest comment'].apply(fix_timestamp)
        df_agg['Latest comment'] = df_agg['Latest comment'].apply(fix_timestamp)


        result["post_comments"] = {"data": df_agg, "title": TABLE_TITLES["instagram_post_comments"]}


    try:
        reels_comments_bytes = unzipddp.extract_file_from_zip(instagram_zip, "reels_comments.json")
        reels_comments_dict = unzipddp.read_json_from_bytes(reels_comments_bytes)
        # reels_comments = [{'res': str(reels_comments_dict)},]


        reels_comments = [{'Media owner': item['string_map_data']['Media owner']['value'],
                        'Timestamp' : item['string_map_data']['Comment creation time']['timestamp'],
                        }
                        for item in reels_comments_dict['comments_reels_comments']]
   
    except:
        reels_comments = None


    if reels_comments:
        df = pd.DataFrame(reels_comments)
        df_agg = pd.DataFrame(df['Media owner'].value_counts()).reset_index().rename(columns={'index': 'Media owner', 'Media owner': 'Number of comments'})
        df_timestamps_min = df[['Media owner', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['Media owner']).rename(columns={'Timestamp': 'Earliest comment'})
        df_timestamps_max = df[['Media owner', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['Media owner']).rename(columns={'Timestamp': 'Latest comment'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='Media owner')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='Media owner')

        df_agg['Earliest comment'] = df_agg['Earliest comment'].apply(fix_timestamp)
        df_agg['Latest comment'] = df_agg['Latest comment'].apply(fix_timestamp)


        result["reels_comments"] = {"data": df, "title": TABLE_TITLES["instagram_reels_comments"]}

    following_bytes = unzipddp.extract_file_from_zip(instagram_zip, "following.json")
    following_dict = unzipddp.read_json_from_bytes(following_bytes)
    # following = [{'res': str(following_dict)},]


    following = [{
                    'value': item['string_list_data'][0]['value'],
                    'timestamp': item['string_list_data'][0]['timestamp'],

                    }
                    for item in following_dict['relationships_following']]
    
    if following:
        df = pd.DataFrame(following)
        result["following"] = {"data": df, "title": TABLE_TITLES["instagram_following"]}


    # liked_comments_bytes = unzipddp.extract_file_from_zip(instagram_zip, "liked_comments.json")
    # liked_comments_dict = unzipddp.read_json_from_bytes(liked_comments_bytes)
    # # liked_comments = [{'res': str(liked_comments_dict)},]


    # liked_comments = [{'href': item['string_list_data'][0]['href'],
    #                 'title': item['title'],
    #                 'timestamp': item['string_list_data'][0]['timestamp'],

    #                 }
    #                 for item in liked_comments_dict['likes_comment_likes']]
    
    # if liked_comments:
    #     df = pd.DataFrame(liked_comments)
    #     result["liked_comments"] = {"data": df, "title": TABLE_TITLES["instagram_liked_comments"]}

    ## AGGREGATE

    liked_posts_bytes = unzipddp.extract_file_from_zip(instagram_zip, "liked_posts.json")
    liked_posts_dict = unzipddp.read_json_from_bytes(liked_posts_bytes)
    # liked_posts = [{'res': str(liked_posts_dict)},]


    liked_posts = [{
                    # 'href': item['string_list_data'][0]['href'],
                    'title': item['title'],
                    'Timestamp': item['string_list_data'][0]['timestamp'],

                    }
                    for item in liked_posts_dict['likes_media_likes']]
    
    if liked_posts:
        df = pd.DataFrame(liked_posts)

        df_agg = pd.DataFrame(df['title'].value_counts()).reset_index().rename(columns={'index': 'title', 'title': 'Number of posts'})
        df_timestamps_min = df[['title', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['title']).rename(columns={'Timestamp': 'Earliest like'})
        df_timestamps_max = df[['title', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['title']).rename(columns={'Timestamp': 'Latest like'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='title')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='title')

        df_agg['Earliest like'] = df_agg['Earliest like'].apply(fix_timestamp)
        df_agg['Latest like'] = df_agg['Latest like'].apply(fix_timestamp)

        result["liked_posts"] = {"data": df_agg, "title": TABLE_TITLES["instagram_liked_posts"]}



    try:
        story_likes_bytes = unzipddp.extract_file_from_zip(instagram_zip, "story_likes.json")
        story_likes_dict = unzipddp.read_json_from_bytes(story_likes_bytes)
        # liked_posts = [{'res': str(liked_posts_dict)},]


        story_likes = [{
                        # 'href': item['string_list_data'][0]['href'],
                        'title': item['title'],
                        'Timestamp': item['string_list_data'][0]['timestamp'],

                        }
                        for item in story_likes_dict['story_activities_story_likes']]
    except:
        story_likes = None
    
    if story_likes:
        df = pd.DataFrame(story_likes)

        df_agg = pd.DataFrame(df['title'].value_counts()).reset_index().rename(columns={'index': 'title', 'title': 'Number of stories'})
        df_timestamps_min = df[['title', 'Timestamp']].sort_values(by='Timestamp', ascending=True).drop_duplicates(subset=['title']).rename(columns={'Timestamp': 'Earliest like'})
        df_timestamps_max = df[['title', 'Timestamp']].sort_values(by='Timestamp', ascending=False).drop_duplicates(subset=['title']).rename(columns={'Timestamp': 'Latest like'})
        df_agg = df_agg.merge(df_timestamps_min, how='left', on='title')
        df_agg = df_agg.merge(df_timestamps_max, how='left', on='title')

        df_agg['Earliest like'] = df_agg['Earliest like'].apply(fix_timestamp)
        df_agg['Latest like'] = df_agg['Latest like'].apply(fix_timestamp)

        result["liked_posts"] = {"data": df_agg, "title": TABLE_TITLES["instagram_story_likes"]}




    return validation, result


def extract_facebook(facebook_zip):
    result = {}

    validation = facebook.validate_zip(facebook_zip)

    # recently_visited_bytes = unzipddp.extract_file_from_zip(facebook_zip, "recently_visited.json")
    # recently_visited_dict = unzipddp.read_json_from_bytes(recently_visited_bytes)
    # recently_visited = []

    # for category in recently_visited_dict['visited_things_v2']:
    #     for item in category['entries']:
    #         if 'data' in item.keys():
    #             if 'name' in item['data'].keys():
    #                 # if 'name' in []: ### LIST OF NEEDED CATEGORIES
    #                 recently_visited.append({'category': category['name'],
    #                                         'item': item['data']['name'],
    #                                         'uri': item['data']['uri'],
    #                                         'timestamp': item['timestamp']
    #                                     })

    # if len(recently_visited) == 0:
    #     recently_visited = None

    # if recently_visited:
    #     df = pd.DataFrame(recently_visited)
    #     df['timestamp'] = df['timestamp'].apply(fix_timestamp)
    #     result["recently_visited"] = {"data": df, "title": TABLE_TITLES["facebook_recently_visited"]}

    recently_viewed_bytes = unzipddp.extract_file_from_zip(facebook_zip, "recently_viewed.json")
    recently_viewed_dict = unzipddp.read_json_from_bytes(recently_viewed_bytes)
    # recently_viewed = [{'res': str(recently_viewed_dict)}]
    recently_viewed = []

    for category in recently_viewed_dict['recently_viewed']:
        if 'children' in category.keys():
            items = category['children']
        if 'entries' in category.keys():
            items = category['entries']
        for item in items:
            if 'data' in item.keys():
                if 'name' in item['data'].keys():
                    # if 'name' in []: ### LIST OF NEEDED CATEGORIES
                    recently_viewed.append({'category': category['name'],
                                            'item': item['data']['name'],
                                            'uri': item['data']['uri'],
                                            'timestamp': item['timestamp']
                                        })

    if len(recently_viewed) == 0:
        recently_viewed = None

    if recently_viewed:
        df = pd.DataFrame(recently_viewed)
        df['timestamp'] = df['timestamp'].apply(fix_timestamp)
        result["recently_viewed"] = {"data": df, "title": TABLE_TITLES["facebook_recently_viewed"]}

    posts_and_comments_bytes = unzipddp.extract_file_from_zip(facebook_zip, "posts_and_comments.json")
    posts_and_comments_dict = unzipddp.read_json_from_bytes(posts_and_comments_bytes)
    posts_and_comments = [{'res': str(posts_and_comments_dict)}]
    posts_and_comments = []

    for item in posts_and_comments_dict['reactions_v2']:
        if 'data' in item.keys():
            posts_and_comments.append({'reaction': item['data'][0]['reaction']['reaction'],
                                    'title': item['title'],
                                    'timestamp': item['timestamp']
                                    })

    if len(posts_and_comments) == 0:
        posts_and_comments = None

    if posts_and_comments:
        df = pd.DataFrame(posts_and_comments)
        df['timestamp'] = df['timestamp'].apply(fix_timestamp)
        result["posts_and_comments"] = {"data": df, "title": TABLE_TITLES["facebook_posts_and_comments"]}

    return validation, result


def extract_tiktok(tiktok_zip):
    result = {}

    validation = tiktok.validate_zip(tiktok_zip) ## need validation

    videos_source_bytes = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    videos_source_dict = unzipddp.read_json_from_bytes(videos_source_bytes)

    if videos_source_dict:
        videos_source_dict = scanfiles.dict_denester(videos_source_dict)
        tiktok_data = [{"Key":k, "Value": v} for k, v in videos_source_dict.items()]
        df = pd.DataFrame(tiktok_data)
        result["tiktok_data"] = {"data": df, "title": TABLE_TITLES["tiktok_data"]}

    # posts_and_comments = []

    # for item in posts_and_comments_dict['reactions_v2']:
    #     if 'data' in item.keys():
    #         posts_and_comments.append({'reaction': item['data'][0]['reaction']['reaction'],
    #                                 'title': item['title'],
    #                                 'timestamp': item['timestamp']
    #                                 })

    #if len(videos_source) == 0:
    #    videos_source = None

    #if videos_source:
    #    df = pd.DataFrame(videos_source)
    #    # df['timestamp'] = df['timestamp'].apply(fix_timestamp)
    #    result["videos_source"] = {"data": df, "title": TABLE_TITLES["facebook_posts_and_comments"]}

    return validation, result


# def extract_youtube(youtube_zip):
#     result = {}

#     validation = youtube.validate_zip(youtube_zip)
#     if validation.ddp_category is not None:
#         if validation.ddp_category.language == Language.EN:
#             subscriptions_fn = "subscriptions.csv"
#             watch_history_fn = "watch-history"
#             comments_fn = "my-comments.html"
#         else:
#             subscriptions_fn = "abonnementen.csv"
#             watch_history_fn = "kijkgeschiedenis"
#             comments_fn = "mijn-reacties.html"

#         # Get subscriptions
#         subscriptions_bytes = unzipddp.extract_file_from_zip( youtube_zip, subscriptions_fn)
#         subscriptions_listdict = unzipddp.read_csv_from_bytes(subscriptions_bytes)
#         df = youtube.to_df(subscriptions_listdict)
#         if not df.empty:
#             result["subscriptions"] = {"data": df, "title": TABLE_TITLES["youtube_subscriptions"]}
        
#         # Get watch history
#         if validation.ddp_category.ddp_filetype == DDPFiletype.JSON:
#             watch_history_fn = watch_history_fn + ".json"
#             watch_history_bytes = unzipddp.extract_file_from_zip(youtube_zip, watch_history_fn)
#             watch_history_listdict = unzipddp.read_json_from_bytes(watch_history_bytes)
#             df = youtube.to_df(watch_history_listdict)
#         if validation.ddp_category.ddp_filetype == DDPFiletype.HTML:
#             watch_history_fn = watch_history_fn + ".html"
#             watch_history_bytes = unzipddp.extract_file_from_zip(youtube_zip, watch_history_fn)
#             df = youtube.watch_history_html_to_df(watch_history_bytes)
#         if not df.empty:
#             result["watch_history"] = {"data": df, "title": TABLE_TITLES["youtube_watch_history"]}

#         # Get comments
#         comments_bytes = unzipddp.extract_file_from_zip(youtube_zip, comments_fn)
#         df = youtube.comments_to_df(comments_bytes)
#         if not df.empty:
#             result["comments"] = { "data": df, "title": TABLE_TITLES["youtube_comments"]}

#     return validation, result


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)
