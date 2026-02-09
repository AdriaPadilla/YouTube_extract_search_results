from tqdm import tqdm
from datetime import datetime
import glob
import json
import os
import credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import concurrent.futures


def get_youtube_client():

    return build("youtube", "v3", developerKey=credentials.API_KEY, cache_discovery=False)

def process_single_video(args):
    video, query = args
    v_id = video["id"]["videoId"]
    filename = f"{query.video_folder}/video-data-{v_id}.json"

    if os.path.exists(filename):
        return

    local_youtube = get_youtube_client()
    video_data = {}
    video_data["video_Id"] = v_id
    video_data["search_string"] = query.search_keyword
    video_data["search_language"] = query.language
    video_data["search_region"] = query.region
    video_data["search_time"] = str(datetime.now())

    try:
        yt_api_video = local_youtube.videos().list(
            part="snippet,statistics,contentDetails", 
            id=v_id
        ).execute()
        
        video_data["video_info"] = yt_api_video

        # Get Owners (Channel) Info
        try:
            if "items" in video_data["video_info"] and len(video_data["video_info"]["items"]) > 0:
                channel_id = video_data["video_info"]["items"][0]["snippet"]["channelId"]
                yt_api_channel = local_youtube.channels().list(
                    id=channel_id, 
                    part="snippet, statistics", 
                    maxResults=2
                ).execute()
                video_data["API_CHANNEL_INFO"] = yt_api_channel
            else:
                pass 
        except Exception as e:
            pass

        # DUMP ALL
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(video_data, f, ensure_ascii=False, indent=4)
            
        local_youtube.close()

    except HttpError as err:
        if err.resp.status in [403, 500, 503]:
            if "quota" in str(err.reason).lower() and "exceeded" in str(err.reason).lower():
                print("!!! QUOTA EXCEEDED - STOPPING !!!")
                os._exit(1) 
        else:
            print(f"FAILED on {v_id}: {err}")
    except Exception as e:
        print(f"General Error on {v_id}: {e}")

def video_data_controller(query):
    search_results_folder = glob.glob(f"{query.search_folder}/*.json")
    
    all_tasks = []
    print("Recopilando lista de videos para procesar...")
    
    for file in search_results_folder:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                parsed_json = json.load(f)
                if "api_data" in parsed_json and "items" in parsed_json["api_data"]:
                    video_items = parsed_json["api_data"]["items"]
                    for video in video_items:
                        all_tasks.append((video, query))
        except Exception as e:
            print(f"Error leyendo archivo {file}: {e}")

    total_videos = len(all_tasks)
    print(f"Total de videos a procesar: {total_videos}")

    WORKERS = 8 
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        list(tqdm(executor.map(process_single_video, all_tasks), total=total_videos, unit="video"))
