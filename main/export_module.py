import json
from tqdm import tqdm
import pandas as pd
import glob
from isodate import parse_duration
from datetime import datetime
import os
import re

def clean_illegal_chars(value):
    if isinstance(value, str):
        # Elimina caracteres de control no permitidos por Excel
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', value)
    return value

def cleaner(query, df, final_file):
    ## INFORMATION
    ## Sometimes people use strange characters in video titles or description. This will break the dataset when import in excel or similar.
    ## The following function will discard all rows with NAN values in "query" column. You'll lose about 1, 2 or 3 video for each 50.000
    ## It's not a big problem, and will save a lot of time trying to compose the Dataset in Excel.

    print(f"===> TOTAL UNIQUE VIDEOS IN DATASET (Drop Duplicated Video_id): {df.shape[0]}")

    # Reemplazos básicos
    df = df.replace({'\t': ' ', '\r': ' ', '\n': ' ', '"': "'"}, regex=True)

    # Elimina filas sin fecha de publicación
    df = df[df['video_published_at'].notna()]

    # Limpieza numérica
    numeric_cols = [
        "channel_view_count", "channel_video_count", "video_duration_seconds",
        "video_view_count", "video_category", "channel_subscribers_count"
    ]
    for col in numeric_cols:
        df[col] = df[col].fillna(0)
        df = df[df[col].notna()]
        print(f"Drop '{col}' NANS {df.shape}")
        df[col] = df[col].replace({',': '.', 'null': 0, ' ': 0}, regex=True)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col].notna()]
        df[col] = df[col].astype(int)



    # Limpieza de caracteres ilegales en todo el DataFrame
    df = df.applymap(clean_illegal_chars)

    print(f"===> Dataset shape AFT Cleaning: {df.shape}")
    print(f"===> Output file in: {final_file}")

    # Exporta a Excel (puedes cambiar a CSV si prefieres)
    df.to_excel(f"{final_file}", index=False)


def iso8601_to_seconds(duration):
    duration_obj = parse_duration(duration)
    total_seconds = duration_obj.total_seconds()
    return total_seconds

def iso8601_to_datetime(date_str):
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    datetime_obj = datetime.strptime(date_str, date_format)
    return datetime_obj

def parser(query):
    video_files = glob.glob(f"{query.video_folder}/*.json")

    if len(video_files) == 0:
        print(f"==> No Videos to export in {raw_video_data_folder}/")
        pass
    else:
        final_file = f"{query.output_folder}/dataset-{query.search_keyword}-from-{query.search_start_date}-to-{query.search_end_date}-{query.order}-{query.time_fragmentation}-{query.now.replace(" ","T")}.xlsx"
        if os.path.exists(final_file):
            print("DATASET YET EXISTS")
            pass
        else:
            informacion = []
            print("==> Pharsing Video Files from .json to Pandas DataFrame (1 loop = 1 video)")
            pbar = tqdm(total=len(video_files))
            for file in video_files:

                pbar.set_description(f"Pharsin: {file}")

                with open(file) as f:

                    parsed_json = json.load(f)
                    data = {}
                    try:
                        data["search_query"] = parsed_json["search_string"]
                        data["order_by"] = query.order
                        data["search_language"] = parsed_json["search_language"]
                        data["search_region"] = parsed_json["search_region"]
                        data["search_time"] = parsed_json["search_time"]
                        # Video INFO
                        data["video_id"] = parsed_json["video_Id"]
                        data["video_kind"] = parsed_json["video_info"]["kind"]
                        data["video_published_at"] = parsed_json["video_info"]["items"][0]["snippet"]["publishedAt"]
                        data["video_published_at"] = iso8601_to_datetime(data["video_published_at"])
                        data["video_title"] = parsed_json["video_info"]["items"][0]["snippet"]["title"]
                        data["video_description"] = parsed_json["video_info"]["items"][0]["snippet"]["description"]
                        data["video_category"] = parsed_json["video_info"]["items"][0]["snippet"]["categoryId"]
                        try:
                            data["video_language"] = parsed_json["video_info"]["items"][0]["snippet"]["defaultAudioLanguage"]
                        except KeyError:
                            data["video_language"] = "no data"
                        try:
                            data["video_duration"] = parsed_json["video_info"]["items"][0]["contentDetails"]["duration"]
                            duration = data["video_duration"]
                        except KeyError:
                            data["video_duration"] = "null"
                        seconds = iso8601_to_seconds(duration)
                        data["video_duration_seconds"] = int(seconds)
                        try:
                            data["video_view_count"] = parsed_json["video_info"]["items"][0]["statistics"]["viewCount"]
                        except KeyError:
                            data["video_view_count"] = "null"
                        try:
                            data["video_like_count"] = parsed_json["video_info"]["items"][0]["statistics"]["likeCount"]
                        except KeyError:
                            data["video_like_count"] = "hidden"
                        try:
                            data["video_comment_count"] = parsed_json["video_info"]["items"][0]["statistics"]["commentCount"]
                        except KeyError:
                            data["video_comment_count"] = "hidden"

                        # Channel INFO
                        data["channel_title"] = parsed_json["video_info"]["items"][0]["snippet"]["channelTitle"]
                        try:
                            data["channel_Id"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["id"]
                        except KeyError:
                            data["channel_Id"] = "no data"
                        try:
                            data["channel_custom_url"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["snippet"]["customUrl"]
                        except KeyError:
                            data["channel_custom_url"] = "no data"
                        try:
                            data["channel_created_at"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["snippet"]["publishedAt"]
                        except KeyError:
                            data["channel_created_at"] = "no data"
                        try:
                            data["channel_description"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["snippet"]["description"]
                        except KeyError:
                            data["channel_description"] = "no data"
                        try:
                            data["channel_location"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["snippet"]["country"]
                        except KeyError:
                            data["channel_location"] = "no data"
                        try:
                            data["channel_subscribers_count"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["statistics"]["subscriberCount"]
                        except KeyError:
                            data["channel_subscribers_count"] = "no data"
                        try:
                            data["channel_view_count"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["statistics"]["viewCount"]
                        except KeyError:
                            data["channel_view_count"] = "no data"
                        try:
                            data["channel_video_count"] = parsed_json["API_CHANNEL_INFO"]["items"][0]["statistics"]["videoCount"]
                        except KeyError:
                            data["channel_video_count"] = "no data"
                        informacion.append(data)
                    except IndexError:
                        print(f"Error on video {parsed_json["video_Id"]}")
                        pass
                pbar.update(1)
            pbar.close()
            df = pd.DataFrame.from_dict(informacion, orient="columns")
            print(df)
            print(f"===> EXECUTING DATASET CLEANER")
            cleaner(query, df, final_file)
