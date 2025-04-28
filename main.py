import query_constructor as constructor
import search_module as search
import video_data_module as video
import export_module as export
from datetime import date, timedelta


# DUMMIE VARIABLES DON'T TOUCH THIS
after = None
before = None
days = None
start_date = None
end_date = None
iterations = 1 # Cambia este valor según sea necesario

search_keywords = ['impuestos españa']
region = "ES"
language = "es-ES"
order = "relevance"
max_results = 50
search_start_date = date(2024, 1, 1) #### YYYY, month, day
search_end_date = date(2025, 1, 1)

if __name__ == "__main__":

    for i in range(iterations):
        print(f"Iniciando iteración {i + 1} de {iterations}...")

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
                max_results
            )
            query = constructor.create_folders(query)

            # Execution
            search.search_controller(query)
            video.video_data_controller(query)
            export.parser(query)

        print(f"Finalizada iteración {i + 1} de {iterations}.")

