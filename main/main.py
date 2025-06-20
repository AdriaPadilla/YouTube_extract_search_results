import query_constructor as constructor
import search_module as search
import video_data_module as video
import export_module as export
from datetime import datetime, timedelta

# DUMMIE VARIABLES DON'T TOUCH THIS
after = None
before = None
days = None
start_date = None
end_date = None

search_keywords = ['Your Keywords here'] # Keywords or list of them. 
region = ""
language = ""
order = "relevance"
max_results = 50
search_start_date = datetime(2010, 1, 1) #### YYYY, month, day
search_end_date = datetime(2024, 1, 31) #### YYYY, month, day
time_fragmentation = ["week"] ### Search granularity within start and end period --> Accepted values: "week","day", "hour"

if __name__ == "__main__":

    for fragmentation in time_fragmentation:

        for search_keyword in search_keywords:
            # Building query_params class
            search_folder = None
            video_folder = None
            query = constructor.query_params(
                search_keyword,
                region,
                language,
                search_start_date,
                search_end_date,
                search_folder,
                video_folder,
                order,
                max_results,
                fragmentation
            )
            query = constructor.create_folders(query)

            # Debug: Print query parameters
            print(f"Query parameters: {vars(query)}")

            # Execution
            search.search_controller(query)
            video.video_data_controller(query)
            export.parser(query)

        print(f"Finalizada iteración {fragmentation}.")
