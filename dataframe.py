#Import
import requests
import pandas as pd


#Read data from github

#Archivos json
url="https://raw.githubusercontent.com/mayteet/ProyectoIndividual/main/datasets/netflix_titles.json"
f=requests.get(url)
data=f.json()
# Convertir el archivo de json en un DataFrame
netflix=pd.DataFrame(data)

#Archivos csv
amazon= pd.read_csv("https://raw.githubusercontent.com/mayteet/ProyectoIndividual/main/datasets/amazon_prime_titles.csv")
disney= pd.read_csv("https://raw.githubusercontent.com/mayteet/ProyectoIndividual/main/datasets/disney_plus_titles.csv")
hulu= pd.read_csv("https://raw.githubusercontent.com/mayteet/ProyectoIndividual/main/datasets/hulu_titles.csv") 

#Agregamos la columna de plataform y unimos las tablas
netflix["plataform"]= "netflix"
amazon["plataform"]="amazon"
disney["plataform"]="disney"
hulu["plataform"]="hulu"


#Unimos los dataframes
df=pd.concat([netflix, amazon, disney, hulu]) 

data_all=pd.DataFrame([])
Lista_wrong=["min", "Seasons","Season"]
df_analysis= df[["show_id","rating","plataform"]]

for elemento in Lista_wrong:
    
    # Filtrar el Dataframe por columna rating en el que se encuentre la palabra min, Seasons, Season
    df_analysis_filter= df_analysis.loc[df_analysis["rating"].str.contains(elemento, na=False)]
    data_all=pd.concat([data_all,df_analysis_filter])
    
data_all.rename(columns={'rating':"duration"}, inplace=True)


#Sacamos las variables del dataframe principal
df_analysis_2= df[["show_id","duration","plataform"]]
df_analysis_2=df_analysis_2.dropna(subset="duration")

#Juntamos las columnas
data_all=pd.concat([data_all, df_analysis_2])
data_all

#Limpieza de datos
data_all.rename(columns={'duration':"duration_2"}, inplace=True)



#Juntamos todo en la tabla principal
df_final= pd.merge(df,data_all, on=["show_id","plataform"],how="left")

df_final= df_final.drop_duplicates(subset=["title","type"])

#Separamos date added en día mes y año
df_final[["day","month","year_added"]]=df_final.date_added.str.split(expand =True)


#Separamos la columna de duration por su valor y su unidad de medida
df_final[["duration_3","um"]]=df_final.duration.str.split(expand =True)

#Convertimos las columnas numéricas en tados numericos
df_final["duration_3"]=pd.to_numeric(df_final["duration_3"], downcast="integer")
df_final["year_added"]=pd.to_numeric(df_final["year_added"], downcast="integer")

#Nos aseguramos que la columna um tenga los datos limpios
df_final["um"]=df_final["um"].str.replace("Seasons","Season")


#Quitamos los duplicados de la tabla
df_final=df_final.drop_duplicates(["title","type","plataform"])

