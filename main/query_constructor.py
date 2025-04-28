import os
output_base_folder = "outputs"
import datetime

class query_params:
    def __init__(self,search_keyword,region,language,search_start_date,search_end_date, search_folder, video_folder, order, max_results):
        self.search_keyword = search_keyword
        self.region = region
        self.language = language
        self.search_start_date = search_start_date
        self.search_end_date = search_end_date
        self.search_folder = search_folder
        self.video_folder = video_folder
        self.order = order
        self.max_results = max_results

def create_folders(query):
    now = datetime.datetime.now()
    search_folder = f"{output_base_folder}/{query.search_keyword}-{now}/search_results"
    video_folder = f"{output_base_folder}/{query.search_keyword}-{now}/video_data"
    output_folder = f"{output_base_folder}/{query.search_keyword}-{now}"

    # Create folders for SEARCH QUERY raw data (json files)
    if not os.path.exists(search_folder):
        os.makedirs(search_folder)
    else:
        print(f"{search_folder} yet exists")

    # Create folders for VIDEO raw data (json files)
    if not os.path.exists(video_folder):
        os.makedirs(video_folder)
    else:
        print(f"{video_folder} yet exists")
    query.now = str(now)
    query.search_folder = search_folder
    query.video_folder = video_folder
    query.output_folder = output_folder
    return query
