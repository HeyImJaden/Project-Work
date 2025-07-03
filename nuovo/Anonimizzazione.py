import pandas as pd
import json
import pathlib 
import hashlib
import creazione_df

input_dir = r"C:\Users\JadeOliverGuevarra\Documents\prova\pjwork\dframe\xml_prova"  # directory contenente XML e P7M
output_dir = r"C:\Users\JadeOliverGuevarra\Documents\prova\pjwork\dframe\data"

def cifratura(stringa):
    return hashlib.sha256(str(stringa).encode()).hexdigest()[:12]

def anonimizza_fattura(df):
    df = df.copy()
    df["piva_anon"] = df['IdFiscaleIVA'].apply(lambda x: cifratura(x))
    diz = dict(zip(df["piva_anon"], df['IdFiscaleIVA']))
    df['IdFiscaleIVA'] = df["piva_anon"]
    df = df.drop(columns=["piva_anon"])
    return df, diz

# Estrai tutte le linee da XML e P7M (batch)
df = creazione_df.dataframe_linee_batch(input_dir)
print(f"Righe estratte dal batch: {len(df)}")

# Applica anonimizzazione su tutto il DataFrame
if not df.empty:
    df_anon, diz = anonimizza_fattura(df)
    print(f"Righe dopo anonimizzazione: {len(df_anon)}")
else:
    print("Nessun dato valido estratto. Verrà creato un CSV vuoto.")
    df_anon, diz = df, {}

output_path_dict = pathlib.Path(output_dir) / 'output_group_1/group_1_bills_dictionary.json'
output_path_csv = pathlib.Path(output_dir) / 'output_group_1/group_1_bills_encrypted.csv'
output_path_dict.parent.mkdir(parents=True, exist_ok=True)

with open(output_path_dict, "w") as f:
    json.dump(diz, f, indent=4)
df_anon.to_csv(output_path_csv, index=False)
print(f"CSV salvato in: {output_path_csv}")
