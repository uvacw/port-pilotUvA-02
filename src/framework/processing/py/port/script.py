import logging
import json
import io

import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.twitter as twitter
import port.facebook as facebook
import port.instagram as instagram

from port.api.commands import (CommandSystemDonate, CommandUIRender)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    #stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [
        ("Twitter", extract_twitter, twitter.validate),
        ("Instagram", extract_instagram, instagram.validate),
        ("Facebook", extract_facebook, facebook.validate),
    ]

    # progress in %
    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None
        progress += step_percentage

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Render the propmt file page
            promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
            file_result = yield render_donation_page(platform_name, promptFile, progress)

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    table_list = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name), progress)

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                break

        progress += step_percentage

        # Render data on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Check if extract something got extracted
            if len(table_list) == 0:
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_donation_page(platform_name, prompt, progress)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def create_consent_form_tables(unique_table_id: str, title: props.Translatable, df: pd.DataFrame) -> list[props.PropsUIPromptConsentFormTable]:
    """
    This function chunks extracted data into tables of 5000 rows that can be renderd on screen
    """

    df_list = helpers.split_dataframe(df, 5000)
    out = []

    if len(df_list) == 1:
        table = props.PropsUIPromptConsentFormTable(unique_table_id, title, df_list[0])
        out.append(table)
    else:
        for i, df in enumerate(df_list):
            index = i + 1
            title_with_index = props.Translatable({lang: f"{val} {index}" for lang, val in title.translations.items()})
            table = props.PropsUIPromptConsentFormTable(f"{unique_table_id}_{index}", title_with_index, df)
            out.append(table)

    return out


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


##################################################################
# Extraction functions

def extract_youtube(youtube_zip: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    """
    Main data extraction function
    Assemble all extraction logic here
    """
    tables_to_render = []

    # Extract comments
    df = youtube.my_comments_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube comments", "nl": "Youtube comments"})
        tables = create_consent_form_tables("youtube_comments", table_title, df) 
        tables_to_render.extend(tables)

    # Extract Watch later.csv
    df = youtube.watch_later_to_df(youtube_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch later", "nl": "Youtube watch later"})
        tables = create_consent_form_tables("youtube_watch_later", table_title, df) 
        tables_to_render.extend(tables)

    # Extract subscriptions.csv
    df = youtube.subscriptions_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube subscriptions", "nl": "Youtube subscriptions"})
        tables = create_consent_form_tables("youtube_subscriptions", table_title, df) 
        tables_to_render.extend(tables)

    # Extract subscriptions.csv
    df = youtube.watch_history_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch history", "nl": "Youtube watch history"})
        tables = create_consent_form_tables("youtube_watch_history", table_title, df) 
        tables_to_render.extend(tables)

    # Extract live chat messages
    df = youtube.my_live_chat_messages_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube my live chat messages", "nl": "Youtube my live chat messages"})
        tables = create_consent_form_tables("youtube_my_live_chat_messages", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render



def extract_twitter(twitter_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = twitter.like_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter likes", "nl": "Twitter likes"})
        tables = create_consent_form_tables("twitter_like", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.following_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter following", "nl": "Twitter following"})
        tables = create_consent_form_tables("twitter_following", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.ad_engagements_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter ad engagements", "nl": "Twitter ad engagements"})
        tables = create_consent_form_tables("twitter_ad_engagements", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.replies_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter replies", "nl": "Twitter replies"})
        tables = create_consent_form_tables("twitter_replies", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.mentions_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter mentions", "nl": "Twitter mentions"})
        tables = create_consent_form_tables("twitter_mentions", table_title, df) 
        tables_to_render.extend(tables)


    return tables_to_render



def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = facebook.recently_viewed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently viewed", "nl": "Facebook recently viewed"})
        tables = create_consent_form_tables("facebook_recently_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook likes and reactions", "nl": "Facebook likes and reactions"})
        tables = create_consent_form_tables("facebook_recently_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook comments", "nl": "Facebook comments"})
        tables = create_consent_form_tables("facebook_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.who_you_follow_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook who you follow", "nl": "Facebook who you follow"})
        tables = create_consent_form_tables("facebook_group_interactions", table_title, df) 
        tables_to_render.extend(tables)


    #df = facebook.likes_and_reactions_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook likes and reactions", "nl": "Facebook likes and reactions"})
    #    tables = create_consent_form_tables("facebook_likes_and_reactions", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.your_badges_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook your badges", "nl": "Facebook your badges"})
    #    tables = create_consent_form_tables("facebook_your_badges", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.your_posts_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook your posts", "nl": "Facebook your posts"})
    #    tables = create_consent_form_tables("facebook_your_posts", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.your_search_history_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook your searh history", "nl": "Facebook your search history"})
    #    tables = create_consent_form_tables("facebook_your_search_history", table_title, df) 
    #    tables_to_render.extend(tables)


    #df = facebook.recently_visited_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook recently visited", "nl": "Facebook recently visited"})
    #    tables = create_consent_form_tables("facebook_recently_visited", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.feed_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook feed", "nl": "Facebook feed"})
    #    tables = create_consent_form_tables("facebook_feed", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.controls_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook controls", "nl": "Facebook controls"})
    #    tables = create_consent_form_tables("facebook_controls", table_title, df) 
    #    tables_to_render.extend(tables)

    #df = facebook.group_posts_and_comments_to_df(facebook_zip)
    #if not df.empty:
    #    table_title = props.Translatable({"en": "Facebook group posts and comments", "nl": "Facebook group posts and comments"})
    #    tables = create_consent_form_tables("facebook_group_posts_and_comments", table_title, df) 
    #    tables_to_render.extend(tables)
        
    return tables_to_render



def extract_instagram(instagram_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = instagram.ads_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram ads viewed", "nl": "Instagram ads viewed"})
        tables = create_consent_form_tables("instagram_ads_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.posts_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram posts viewed", "nl": "Instagram posts viewed"})
        tables = create_consent_form_tables("instagram_posts_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.videos_watched_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram videos watched", "nl": "Instagram videos watched"})
        tables = create_consent_form_tables("instagram_videos_watched", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.post_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram post comments", "nl": "Instagram posts post comments"})
        tables = create_consent_form_tables("instagram_post_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.reels_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram reels comments", "nl": "Instagram reels comments"})
        tables = create_consent_form_tables("instagram_reels_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.liked_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram liked posts", "nl": "Instagram posts liked posts"})
        tables = create_consent_form_tables("instagram_liked_posts", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.story_likes_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram story likes", "nl": "Instagram story likes"})
        tables = create_consent_form_tables("instagram_liked_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.saved_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram saved posts", "nl": "Instagram saved posts"})
        tables = create_consent_form_tables("instagram_liked_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.following_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram following", "nl": "Instagram following"})
        tables = create_consent_form_tables("instagram_liked_comments", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render



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
