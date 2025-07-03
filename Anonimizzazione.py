import pandas as pd
import os
import json
import pathlib 
import hashlib
import creazione_df

def cifratura(stringa):
        return hashlib.sha256(str(stringa).encode()).hexdigest()[:12]

def anonimizza_fattura(df):
        df["piva_anon"] = df['IdFiscaleIVA'].apply(lambda x: cifratura(x))
        diz = dict(zip(df["piva_anon"], df['IdFiscaleIVA']))
        df['IdFiscaleIVA'] = df["piva_anon"]
        df = df.drop(columns=["piva_anon"])
        return df, diz


def xml_tocsvs(path_xml, output_folder):
    df = pd.DataFrame()
    diz={}
    for file in os.listdir(path_xml): 
        xml_path = os.path.join(path_xml, file)
        df_clean = creazione_df.dataframe_linee_da_xml(xml_path)
        encrypted_xml, dic = anonimizza_fattura(df_clean)
        df = pd.concat([df, encrypted_xml], ignore_index=True)
        diz.update(dic)  

    df_cifratura=pd.DataFrame(list(diz.items()), columns=['piva_anon', 'IdFiscaleIVA'])  
    output_df_crifratura= pathlib.Path(output_folder) / 'cifratura.csv'
    df_cifratura.to_csv(output_df_crifratura, index=False)

    for cessionario in df['CodiceFiscaleCessionario'].unique(): 
        df_data_azienda=df[df['CodiceFiscaleCessionario'] == cessionario]
        path=pathlib.Path(output_folder)/cessionario+'.csv'
        path.parent.mkdir(parents=True, exist_ok=True)
        df_data_azienda.drop(columns=['CodiceFiscaleCessionario']).to_csv(path, index=False)
        df_data_azienda = pd.DataFrame()
    

if __name__ == "__main__":
    path_xml = 'path_to_your_xml_files'  # Replace with your XML files path
    output_folder = 'output_csvs'  # Replace with your desired output folder
    xml_tocsvs(path_xml, output_folder)
    print(f"CSV files created in {output_folder} and anonymization mapping saved in cifratura.csv")
