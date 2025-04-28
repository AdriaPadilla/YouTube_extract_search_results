import credentials
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import json
import time
def yt_search_api_query(query, next_token, after, before, loop):

    filename = f"{query.search_folder}/search_results-from-{after}-to-{before}-{loop}.json"

    # This block checks if file exists
    if os.path.exists(filename):
        print(f"{filename} yet exists --> PASS")

        # IF file exists, check if paginated results for this day exists
        with open(filename) as f:
            parsed_json = json.load(f)
            try:
                next_token = parsed_json["api_data"]['nextPageToken']
                loop = loop + 1
                yt_search_api_query(query, next_token, after, before, loop)
            except KeyError:
                pass

    # If file not in path, execute API QUERY
    else:
        try:
            youtube = build("youtube", "v3", developerKey=credentials.API_KEY)
            yt_api_response = {}

            response = youtube.search().list(
                    q=query.search_keyword,
                    part="id",
                    maxResults=query.max_results, # This is max in each request
                    type="video",
                    safeSearch="none",
                    relevanceLanguage=query.language,
                    videoDuration="any",
                    pageToken=next_token, # Will use this to iterate over the same day
                    publishedAfter=after,
                    publishedBefore=before,
                    order=query.order,
                ).execute()

            # ADD CONTEXTUAL QUERY INFO TO JSON FILE
            query.query_time = str(datetime.now())
            query.query_after = str(after)
            query.query_before = str(before)
            query.search_start_date = str(query.search_start_date)
            query.search_end_date = str(query.search_end_date)
            yt_api_response["search_params"] = query.__dict__
            yt_api_response["api_data"] = response
            total_videos_in_response = response['pageInfo']['resultsPerPage']

            # Dump API response to JSON file
            with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(yt_api_response, f, ensure_ascii=False, indent=4)

            print(f"Loop Nº {loop} for query: '{query.search_keyword}' from {after} to {before} | VIDEOS in Reponse: {total_videos_in_response}")

            try:
                # Check for more than 50 results in a day
                next_token = response["nextPageToken"]
                loop = loop + 1
                yt_search_api_query(query, next_token, after, before, loop)

            except KeyError:
                # Key error on next_token == no next token
                pass

        # API ERROR HANDLER
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                print(f"==> ¡ALERT! API ERROR")

                # QUOTA EXCEEDED ERROR TERMINATING
                if "quota" and "exceeded" in err.reason:
                    print(f"==> ¡ALERT! QUOTA EXCEEDED || TERMINATING EXECUTION")
                    raise

                # OTHER ERRORS
                else:
                    print(f"ALERT: {err.resp.status} {err.reason} ")
                    print(f"ALERT: RETRY AFTER 15 SECONDS ")
                    time.sleep(15)
                    query(item)

            # UNKNOWN ERRORS
            else:
                print(f"UNKNOWN ERROR")
                raise

def search_controller(query):
    # SET COUNTERS FOR THE QUERY TO 0
    next_token = None

    # Preparing day by day search. 1 day steps
    delta = timedelta(days=1)
    start_date = query.search_start_date
    end_date = query.search_end_date
    date_diff = end_date-start_date
    days = date_diff.days
    days = days +1
    quota = days*100
    loop = 1

    print(f"==> ALERT: You're going to perform at least {days} 'search:list' YouTube API endpoint requests")

    # ITERATE BETWEEN AFTER AND BEFORE DATES
    while start_date <= end_date:
        after = start_date.strftime("%Y-%m-%dT00:00:00Z")
        before = start_date + delta
        before = before.strftime("%Y-%m-%dT00:00:00Z")
        start_date += delta

        yt_search_api_query(query, next_token, after, before, loop)


