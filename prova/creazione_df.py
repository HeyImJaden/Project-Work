from lxml import objectify
import pandas as pd
import os
from estrai_p7m_python_v2 import estrai_xml_da_p7m_python_v2
import glob

def find_child_by_tag(parent, tag):
    for child in parent.iterchildren():
        if child.tag.endswith(tag):
            return child
    return None

def get_cedente_info(header):
    cedente = find_child_by_tag(header, 'CedentePrestatore')
    dati_anagrafici = find_child_by_tag(cedente, 'DatiAnagrafici')
    id_fiscale_iva = find_child_by_tag(dati_anagrafici, 'IdFiscaleIVA')
    id_paese = str(id_fiscale_iva.IdPaese) if id_fiscale_iva is not None else ''
    id_codice = str(id_fiscale_iva.IdCodice) if id_fiscale_iva is not None else ''
    sede = find_child_by_tag(cedente, 'Sede')
    cap = str(sede.CAP) if sede is not None and hasattr(sede, 'CAP') else ''
    comune = str(sede.Comune) if sede is not None and hasattr(sede, 'Comune') else ''
    provincia = str(sede.Provincia) if sede is not None and hasattr(sede, 'Provincia') else ''
    return id_paese, id_codice, cap, comune, provincia

def get_data_fattura(body):
    dati_generali = find_child_by_tag(body, 'DatiGenerali')
    dati_doc = find_child_by_tag(dati_generali, 'DatiGeneraliDocumento')
    return pd.to_datetime(str(dati_doc.Data))

def get_linee_dettaglio(body, cedente_info, data_fattura):
    beni_servizi = find_child_by_tag(body, 'DatiBeniServizi')
    id_paese, id_codice, cap, comune, provincia = cedente_info
    rows = []
    for linea in beni_servizi.iterchildren():
        if linea.tag.endswith('DettaglioLinee'):
            #  controllare se esiste il tag quantita
            if not hasattr(linea, 'Quantita') or linea.Quantita is None:
                linea.Quantita = -1
            row = {
                'Data': data_fattura,
                'IdPaese': id_paese,
                'IdFiscaleIVA': id_codice,
                'CAP': cap,
                'Comune': comune,
                'Provincia': provincia,
                'Descrizione': str(linea.Descrizione),
                'Quantita': int(float(linea.Quantita)) if int(float(linea.Quantita)) is not None else -1 ,
                'PrezzoUnitario': float(linea.PrezzoUnitario),
                'PrezzoTotale': float(linea.PrezzoTotale),
                # 'AliquotaIVA': int(linea.AliquotaIVA)
            }
            rows.append(row)
    return rows

def dataframe_linee_da_xml(path):
    xml = objectify.parse(open(path))
    root = xml.getroot()
    header = find_child_by_tag(root, 'FatturaElettronicaHeader')
    body = find_child_by_tag(root, 'FatturaElettronicaBody')
    cedente_info = get_cedente_info(header)
    data_fattura = get_data_fattura(body)
    rows = get_linee_dettaglio(body, cedente_info, data_fattura)
    return pd.DataFrame(rows)

def dataframe_linee_auto(path):
    """
    Se path è un .p7m estrae l'XML e crea il DataFrame, altrimenti usa direttamente l'XML.
    """
    if path.lower().endswith('.p7m'):
        xml_content = estrai_xml_da_p7m_python_v2(path)
        # Salva temporaneamente l'XML estratto
        temp_xml = path + '_estratto.xml'
        with open(temp_xml, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        path_xml = temp_xml
    else:
        path_xml = path
    return dataframe_linee_da_xml(path_xml)

def dataframe_linee_batch(cartella):
    """
    Processa tutti i file .xml (esclusi *_estratto.xml) e .p7m in una cartella.
    Restituisce un unico DataFrame concatenato. Salta i file malformati.
    """
    dfs = []
    # Tutti i .xml che NON finiscono con _estratto.xml
    for path in glob.glob(os.path.join(cartella, "*.xml")):
        if not path.endswith("_estratto.xml"):
            try:
                dfs.append(dataframe_linee_auto(path))
            except Exception as e:
                print(f"Errore su {path}: {e}")
    # Tutti i .p7m
    for path in glob.glob(os.path.join(cartella, "*.p7m")):
        try:
            dfs.append(dataframe_linee_auto(path))
        except Exception as e:
            print(f"Errore su {path}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()  # vuoto se nessun file trovato

if __name__ == "__main__":
    # Esempio: processa tutti i file validi in una cartella
    cartella = r'C:\Users\JadeOliverGuevarra\Documents\prova\pjwork\dframe\xml_prova'
    df_batch = dataframe_linee_batch(cartella)
    print(df_batch)
    df_batch.to_csv("linee_estratte_batch.csv", index=False)


# Esempio di utilizzo:
# from fattura_linee_utils import dataframe_linee_da_xml

# df = dataframe_linee_da_xml(r'C:\Users\JadeOliverGuevarra\Documents\prova\pjwork\IT01234567890_FPR02.xml')
# df