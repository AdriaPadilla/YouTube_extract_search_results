from tqdm import tqdm
from datetime import datetime, timedelta
import glob
import json
import os
import time
import credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

youtube = build("youtube", "v3", developerKey=credentials.API_KEY)

def get_video_data(query, video_items):
    for video in video_items:
        v_id = video["id"]["videoId"]
        filename = f"{query.video_folder}/video-data-{v_id}.json"
        if os.path.exists(filename):
            print(f"{filename} Yet exists")
            pass
        else:
            # Adds contextual data to JSON FILE
            video_data = {}
            video_data["video_Id"] = v_id
            video_data["search_string"] = query.search_keyword
            video_data["search_language"] = query.language
            video_data["search_region"] = query.region
            video_data["search_time"] = str(datetime.now())

            try:
                # Get Video info
                yt_api_video = youtube.videos().list(part="snippet,statistics,contentDetails", id=v_id).execute()
                video_data["video_info"] = yt_api_video

                # Get Owners (Channel) Info
                try:
                    channel_id = video_data["video_info"]["items"][0]["snippet"]["channelId"]
                    yt_api_channel = youtube.channels().list(id=channel_id, part="snippet, statistics", maxResults=2).execute()
                    video_data["API_CHANNEL_INFO"] = yt_api_channel
                except:
                    print(f"Error on {filename}")
                    pass

                # DUMP ALL
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(video_data, f, ensure_ascii=False, indent=4)

            except HttpError as err:
                if err.resp.status in [403, 500, 503]:
                    print(f"==> ¡ALERT! API ERROR {err.reason}")
                    print(f"==> ¡ALERT! SLEEPING FOR 5 SECONDS AND RETRY")
                    if "quota" and "exceeded" in err.reason:
                        exit()
                    else:
                        time.sleep(5)
                        query(item)
                else:
                    print(yt_api_video)
                    print(f"============> ¡ALERT! RETRY FAILED")
                    time.sleep(5)
                    raise



def video_data_controller(query):
    search_results_folder = glob.glob(f"{query.search_folder}/*.json")
    pbar = tqdm(total=len(search_results_folder))

    for file in search_results_folder:
        with open(file) as f:
            parsed_json = json.load(f)
            video_items = parsed_json["api_data"]["items"]
            pbar.set_description(f"Working on: {file.split("/")[-1]} | Contains {len(video_items)} videos")
            get_video_data(query, video_items)
        pbar.update(1)
    pbar.close()

