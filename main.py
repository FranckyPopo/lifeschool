import os
import pandas as pd
import mysql.connector

# Paramètres de connexion à la base de données MySQL
db_config = {
  'user': 'root',
  'password': 'P@ssword2023',
  'host': 'localhost',
  'database': 'project',
  'raise_on_warnings': True
}

# Répertoire contenant les fichiers CSV
csv_dir = r"C:\Users\miche\OneDrive\SURFACE\DOCUMENT\02 - FORMATION\PYTHON\Projet\CSV"

# Connexion à la base de données MySQL
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Parcours des fichiers CSV dans le répertoire
for filename in os.listdir(csv_dir):
    if filename.endswith('.csv'):
        csv_path = os.path.join(csv_dir, filename)
        
        # Lecture du fichier CSV
        df = pd.read_csv(csv_path, comment='#')
        
        # Insertion des données dans la table MySQL
        for index, row in df.fillna(-1).iterrows():
            query = """
            INSERT INTO lifeschool (
              ts,
              te,
              td,
              sa,
              da,
              sp,
              dp,
              pr,
              flg,
              fwd,
              stos,
              ipkt,
              ibyt,
              opkt,
              obyt,
              _in,
              _out,
              sas,
              das,
              smk,
              dmk,
              dtos,
              dir,
              nh,
              nhb,
              svln,
              dvln,
              ismc,
              odmc,
              idmc,
              osmc,
              mpls1,
              mpls2,
              mpls3,
              mpls4,
              mpls5,
              mpls6,
              mpls7,
              mpls8,
              mpls9,
              mpls10,
              cl,
              sl,
              al,
              ra,
              eng,
              exid,
              tr) 
              VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
              );"""
            data = (
              row["ts"],
              row["te"],
              row["td"],
              row["sa"],
              row["da"],
              row["sp"],
              row["dp"],
              row["pr"],
              row["flg"],
              row["fwd"],
              row["stos"],
              row["ipkt"],
              row["ibyt"],
              row["opkt"],
              row["obyt"],
              row["_in"],
              row["_out"],
              row["sas"],
              row["das"],
              row["smk"],
              row["dmk"],
              row["dtos"],
              row["dir"],
              row["nh"],
              row["nhb"],
              row["svln"],
              row["dvln"],
              row["ismc"],
              row["odmc"],
              row["idmc"],
              row["osmc"],
              row["mpls1"],
              row["mpls2"],
              row["mpls3"],
              row["mpls4"],
              row["mpls5"],
              row["mpls6"],
              row["mpls7"],
              row["mpls8"],
              row["mpls9"],
              row["mpls10"],
              row["cl"],
              row["sl"],
              row["al"],
              row["ra"],
              row["eng"],
              row["exid"],
              row["tr"]
            )
            cursor.execute(query, data)
      
        # Validation des modifications
        cnx.commit()
        print("SUCCESS !")

# Fermeture de la connexion à la base de données MySQL
cursor.close()
cnx.close()


new fontionnality














