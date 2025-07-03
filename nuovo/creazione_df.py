from lxml import objectify
import pandas as pd

def find_child_by_tag(parent, tag):
    for child in parent.iterchildren():
        if child.tag.endswith(tag):
            return child
    return None

def find_all_children_by_tag(parent, tag):
    return [child for child in parent.iterchildren() if child.tag.endswith(tag)]

def get_cedente_info(header):
    cedente = find_child_by_tag(header, 'CedentePrestatore')
    dati_anagrafici = find_child_by_tag(cedente, 'DatiAnagrafici')
    id_fiscale_iva = find_child_by_tag(dati_anagrafici, 'IdFiscaleIVA')
    # Usa .text per preservare zeri iniziali
    id_paese = id_fiscale_iva.IdPaese.text if id_fiscale_iva is not None and hasattr(id_fiscale_iva, 'IdPaese') else ''
    id_codice = id_fiscale_iva.IdCodice.text if id_fiscale_iva is not None and hasattr(id_fiscale_iva, 'IdCodice') else ''
    sede = find_child_by_tag(cedente, 'Sede')
    cap = str(sede.CAP) if sede is not None and hasattr(sede, 'CAP') else ''
    comune = str(sede.Comune) if sede is not None and hasattr(sede, 'Comune') else ''
    provincia = str(sede.Provincia) if sede is not None and hasattr(sede, 'Provincia') else ''
    return id_paese, id_codice, cap, comune, provincia

def get_cessionario_codici_fiscali(header):
    codici = []
    for cessionario in find_all_children_by_tag(header, 'CessionarioCommittente'):
        dati_anagrafici = find_child_by_tag(cessionario, 'DatiAnagrafici')
        if dati_anagrafici is not None:
            # Prendi tutti i CodiceFiscale
            for cf in find_all_children_by_tag(dati_anagrafici, 'CodiceFiscale'):
                cf_str = cf.text if hasattr(cf, 'text') else str(cf)
                codici.append(cf_str)
            # Prendi tutti gli IdCodice dentro IdFiscaleIVA
            id_fiscale_iva = find_child_by_tag(dati_anagrafici, 'IdFiscaleIVA')
            if id_fiscale_iva is not None and hasattr(id_fiscale_iva, 'IdCodice'):
                idcodice = id_fiscale_iva.IdCodice.text if hasattr(id_fiscale_iva.IdCodice, 'text') else str(id_fiscale_iva.IdCodice)
                codici.append(idcodice)
    return codici

def get_data_fattura(body):
    dati_generali = find_child_by_tag(body, 'DatiGenerali')
    dati_doc = find_child_by_tag(dati_generali, 'DatiGeneraliDocumento')
    return pd.to_datetime(str(dati_doc.Data))

def get_linee_dettaglio(body, cedente_info, data_fattura, codici_cessionario):
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
                'CodiceFiscaleCessionario': ','.join(codici_cessionario) if codici_cessionario else ''
            }
            rows.append(row)
    return rows

def dataframe_linee_da_xml(path):
    xml = objectify.parse(open(path))
    root = xml.getroot()
    header = find_child_by_tag(root, 'FatturaElettronicaHeader')
    body = find_child_by_tag(root, 'FatturaElettronicaBody')
    cedente_info = get_cedente_info(header)
    codici_cessionario = get_cessionario_codici_fiscali(header)
    data_fattura = get_data_fattura(body)
    rows = get_linee_dettaglio(body, cedente_info, data_fattura, codici_cessionario)
    df = pd.DataFrame(rows)
    # Forza i campi a stringa per preservare zeri iniziali
    for col in ["IdFiscaleIVA", "CodiceFiscaleCessionario"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

# Esempio di utilizzo:
# from fattura_linee_utils import dataframe_linee_da_xml

# df = dataframe_linee_da_xml(r'C:\Users\JadeOliverGuevarra\Documents\prova\pjwork\IT01234567890_FPR02.xml')
# df