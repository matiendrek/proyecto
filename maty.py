from fastapi import FastAPI
import pandas as pd
import numpy as np 
import requests as rq 
from dataframe import df_final


# Descargo el archivo json desde guthub.

url = "https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/netflix_titles.json" 


f = rq.get(url)

data = f.json()

# Convierto el archivo json de netflix a un dataframe.

netflix = pd.DataFrame(data)



# Descargo los datasets en formato csv que faltaban.

url_hulu = pd.read_csv("https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/hulu_titles.csv")

url_amazon = pd.read_csv("https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/amazon_prime_titles.csv")

url_disney = pd.read_csv("https://raw.githubusercontent.com/HX-FAshur/PI01_DATA05/main/Datasets/disney_plus_titles.csv") 





# Convierto los archivos csv en dataframes.

hulu = pd.DataFrame(url_hulu)
amazon = pd.DataFrame(url_amazon)
disney = pd.DataFrame(url_disney)





# Renombro las columnas del dataframe de hulu.

hulu.rename(columns= {"type": "tipo", "title": "titulo", "cast": "emitir",
 "country": "pais", "date_added": "fecha_agregada",
"release_year": "año_de_lanzamiento", "duration": "duracion", "listed_in": "genero",
 "description": "descripcion"}, inplace=True)




# Renombro las columnas del dataframe de amazon.

amazon.rename(columns= {"type": "tipo", "title": "titulo", "cast": "emitir",
 "country": "pais", "date_added": "fecha_agregada",
"release_year": "año_de_lanzamiento", "duration": "duracion", "listed_in": "genero",
 "description": "descripcion"}, inplace=True)




# Renombro las columnas del dataframe de disney.

disney.rename(columns= {"type": "tipo", "title": "titulo", "cast": "emitir",
 "country": "pais", "date_added": "fecha_agregada",
"release_year": "año_de_lanzamiento", "duration": "duracion", "listed_in": "genero",
 "description": "descripcion"}, inplace=True)



hulu["plataforma"] = "hulu"
netflix["plataforma"] = "netflix"
amazon["plataforma"] = "amazon"
disney["plataforma"] = "disney"


df = pd.concat([netflix, hulu, disney,amazon])



data_all=pd.DataFrame([])
Lista_wrong=["min", "Seasons","Season"]
df_analysis= df[["show_id","rating","plataforma"]]



for elemento in Lista_wrong:
    
    # Filtrar el Dataframe por columna rating en el que se encuentre la palabra min, Seasons, Season
    df_analysis_filter= df_analysis.loc[df_analysis["rating"].str.contains(elemento, na=False)]
    data_all=pd.concat([data_all,df_analysis_filter])
    
data_all.rename(columns={'rating':"duracion"}, inplace=True)




#Sacamos las variables del dataframe principal
df_analysis_2= df[["show_id","duracion","plataforma"]]
df_analysis_2=df_analysis_2.dropna(subset="duracion")




#Juntamos las columnas
data_all=pd.concat([data_all, df_analysis_2])
data_all



#Limpieza de datos
data_all.rename(columns={'duracion':"duracion_2"}, inplace=True)



data_all["duracion_2"]=data_all["duracion_2"].str.replace(" min","")
data_all["duracion_2"]=data_all["duracion_2"].str.replace(" Seasons","")
data_all["duracion_2"]=data_all["duracion_2"].str.replace(" Season","")




#Juntamos todo en la tabla principal
df_final= pd.merge(df,data_all, on=["show_id","plataforma"],how="left")

df_final= df_final.drop_duplicates(subset=["titulo","tipo"])
df_final





#Separamos date added en día mes y año
df_final[["dia","mes","año_añadido"]]=df_final.fecha_agregada.str.split(expand =True)






#Separamos la columna de duration por su valor y su unidad de medida
df_final[["duration_3","um"]]=df_final.duracion.str.split(expand =True)

#Convertimos la columna de duration a integer
df_final["duration_3"]=pd.to_numeric(df_final["duration_3"], downcast="integer")

#Nos aseguramos que la columna um tenga los datos limpios
df_final["um"]=df_final["um"].str.replace("Seasons","Season")


#Quitamos los duplicados de la tabla
df_final=df_final.drop_duplicates(["titulo","tipo","plataforma"])
df_final


app = FastAPI()
@app.get("/")
async def index():
    return "Hola mundo!!"

@app.get("/get_max_duracion/")
async def get_max_duracion(anio:int,plataforma:str,um:str):
    
    df_consul_1= df_final[["plataform","release_year","um","duration_3"]]
    df_consul_1=df_consul_1[(df_consul_1["um"]==um)&(df_consul_1["release_year"]==anio)&(df_consul_1["plataform"]==plataforma)].groupby("release_year").max()
    return df_consul_1