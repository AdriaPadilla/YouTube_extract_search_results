import sys
import os
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi

def descargar_subtitulos(videoId, idioma='es', index=1, carpeta_salida='subtitulos'):
    try:
        # Obtenemos la transcripción en el idioma especificado (por defecto, español)
        transcripciones = YouTubeTranscriptApi.get_transcript(videoId, languages=[idioma])

        # Asegura que la carpeta de salida exista
        if not os.path.exists(carpeta_salida):
            os.makedirs(carpeta_salida)

        # Ruta completa para el archivo de salida
        nombre_archivo = os.path.join(carpeta_salida, f"video_{index}_{videoId}.txt")
        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            for linea in transcripciones:
                f.write(linea['text'] + '\n')

        print(f"Subtítulos descargados con éxito en {nombre_archivo}")

    except Exception as e:
        print(f"Error al descargar subtítulos para el video {videoId}: {e}")

if __name__ == "__main__":
    idioma = "es"
    archivo = ""  # ← aquí deberías indicar el path a tu CSV

    # Carga del dataset desde un archivo .csv
    df = pd.read_csv(archivo, sep=",")

    # Itera sobre las filas filtradas y descarga subtítulos para cada video
    for i, row in enumerate(df.itertuples(), start=1):
        videoId = getattr(row, 'videoId', None)
        if videoId:
            descargar_subtitulos(videoId, idioma, i)
        else:
            print("No se encontró la columna 'videoId' o no tiene valor en esta fila.")
