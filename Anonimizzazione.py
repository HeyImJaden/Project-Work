import pandas as pd
import os
import json
import pathlib 
import hashlib
import creazione_df

input_dir = r"input/path"
output_dir = r"output/path"
lista2 = []
df = pd.DataFrame()

def cifratura(stringa):
    return hashlib.sha256(str(stringa).encode()).hexdigest()[:12]

def anonimizza_fattura(df):
    df["piva_anon"] = df['IdFiscaleIVA'].apply(lambda x: cifratura(x))
    diz = dict(zip(df["piva_anon"], df['IdFiscaleIVA']))
    df['IdFiscaleIVA'] = df["piva_anon"]
    df = df.drop(columns=["piva_anon"])
    return df, diz

def cleaning(directory, df):
    for file in os.listdir(directory):
        if file.endswith('.xml'):
            xml_path = os.path.join(directory, file)
            df_clean = creazione_df.dataframe_linee_da_xml(xml_path)
            encrypted_xml, dic = anonimizza_fattura(df_clean)
            lista2.append(dic)
            df = pd.concat([df, encrypted_xml], ignore_index=True)
    return df

df = cleaning(input_dir, df)

output_path_dict = pathlib.Path(output_dir) / 'output_group_1/group_1_bills_dictionary.json'
output_path_csv = pathlib.Path(output_dir) / 'output_group_1/group_1_bills_encrypted.csv'
output_path_dict.parent.mkdir(parents=True, exist_ok=True)

with open(output_path_dict, "w") as f:
    json.dump(lista2, f, indent=4)
df.to_csv(output_path_csv, index=False)
