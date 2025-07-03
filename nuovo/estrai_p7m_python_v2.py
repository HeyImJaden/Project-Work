import io
import re
from lxml import objectify
import pandas as pd
import zlib
import binascii
import base64

def estrai_xml_da_p7m_python_v2(p7m_path):
    """
    Estrae il contenuto XML da un file .p7m usando solo librerie Python.
    Versione che prova diversi approcci di parsing.
    """
    with open(p7m_path, 'rb') as f:
        p7m_data = f.read()
    
    # Approccio 1: Cerca pattern XML direttamente nei dati binari
    xml_content = _cerca_xml_in_binario(p7m_data)
    if xml_content:
        return xml_content
    
    # Approccio 2: Prova decodifica con diversi encoding
    for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
        try:
            xml_content = _cerca_xml_in_testo(p7m_data, encoding)
            if xml_content:
                return xml_content
        except:
            continue
    
    # Approccio 3: Cerca pattern ASN.1/DER (più avanzato)
    xml_content = _cerca_xml_in_asn1(p7m_data)
    if xml_content:
        return xml_content
    
    # Approccio 4: Fallback - cerca la prima sequenza che sembra XML
    xml_content = _cerca_xml_generico(p7m_data)
    if xml_content:
        return xml_content
    
    # Approccio 5: Parsing ASN.1 avanzato con asn1crypto
    xml_content = _cerca_xml_in_asn1crypto(p7m_data, p7m_path)
    if xml_content:
        return xml_content
    
    # Approccio 6: Brute-force binario
    xml_content = _estrai_xml_bruteforce_binario(p7m_data)
    if xml_content:
        print("[Brute-force binario] XML estratto!")
        return xml_content
    
    # Approccio 7: Brute-force decompress zlib
    xml_content = _estrai_xml_bruteforce_compressed(p7m_data)
    if xml_content:
        print("[Brute-force decompress zlib] XML estratto!")
        return xml_content
    
    # Approccio 8: Brute-force base64
    xml_content = _estrai_xml_bruteforce_base64(p7m_data)
    if xml_content:
        print("[Brute-force base64] XML estratto!")
        return xml_content
    
    # Fallback: salva tutte le sequenze che iniziano con < e finiscono con >
    _salva_sequenze_xml_debug(p7m_data, p7m_path)
    
    raise Exception("Impossibile estrarre XML dal file p7m")

def _cerca_xml_in_binario(data):
    """Cerca pattern XML direttamente nei dati binari (robusto: primo end dopo start, strip)"""
    xml_start_patterns = [
        b'<?xml',
        b'\x3c\x3f\x78\6d\6c',  # <?xml in hex
        b'<n0:FatturaElettronica',
    ]
    xml_end_patterns = [
        b'</ns2:FatturaElettronica>',
        b'</FatturaElettronica>',
        b'</p:FatturaElettronica>',
        b'</n0:FatturaElettronica>',
    ]
    for start_pattern in xml_start_patterns:
        start_pos = data.find(start_pattern)
        if start_pos != -1:
            for end_pattern in xml_end_patterns:
                end_pos = data.find(end_pattern, start_pos)
                if end_pos != -1:
                    end_pos += len(end_pattern)
                    xml_bytes = data[start_pos:end_pos]
                    # Rimuovi caratteri nulli e whitespace ai bordi
                    xml_bytes = xml_bytes.strip(b'\x00 \r\n\t')
                    try:
                        return xml_bytes.decode('utf-8', errors='ignore').strip()
                    except:
                        try:
                            return xml_bytes.decode('latin-1', errors='ignore').strip()
                        except:
                            continue
    return None

def _cerca_xml_in_testo(data, encoding):
    """Cerca pattern XML dopo decodifica con un encoding specifico (robusto: primo end dopo start, strip)"""
    try:
        text = data.decode(encoding, errors='ignore')
        xml_start = text.find('<?xml')
        if xml_start == -1:
            xml_start = text.find('<n0:FatturaElettronica')
        if xml_start != -1:
            end_patterns = [
                '</ns2:FatturaElettronica>',
                '</FatturaElettronica>',
                '</p:FatturaElettronica>',
                '</n0:FatturaElettronica>',
            ]
            for end_pattern in end_patterns:
                xml_end = text.find(end_pattern, xml_start)
                if xml_end != -1:
                    xml_end += len(end_pattern)
                    xml_content = text[xml_start:xml_end].strip(' \r\n\t\x00')
                    if _is_valid_xml(xml_content):
                        return xml_content
    except:
        pass
    return None

def _cerca_xml_in_asn1(data):
    """Cerca XML in struttura ASN.1/DER (approccio semplificato)"""
    # Pattern comuni per contenuto XML in ASN.1
    # Cerca sequenze di byte che potrebbero contenere XML
    
    # Pattern per OCTET STRING che contiene XML
    xml_indicators = [
        b'FatturaElettronica',
        b'CedentePrestatore',
        b'DatiGenerali',
    ]
    
    for indicator in xml_indicators:
        pos = data.find(indicator)
        if pos != -1:
            # Cerca indietro per trovare l'inizio dell'XML
            search_start = max(0, pos - 1000)
            chunk = data[search_start:pos + 5000]
            
            xml_content = _cerca_xml_in_testo(chunk, 'utf-8')
            if xml_content:
                return xml_content
    
    return None

def _cerca_xml_in_asn1crypto(data, p7m_path=None):
    """Estrae tutte le OCTET STRING con asn1crypto e cerca l'XML"""
    try:
        from asn1crypto import cms
        content_info = cms.ContentInfo.load(data)
        # Ricorsivamente cerca tutte le octet string
        def extract_octets(obj):
            if hasattr(obj, 'native') and isinstance(obj.native, (bytes, str)):
                return [obj.native]
            elif hasattr(obj, 'children'):
                result = []
                for child in obj.children:
                    result.extend(extract_octets(child))
                return result
            return []
        octets = extract_octets(content_info)
        found = False
        candidates = []
        for octet in octets:
            if isinstance(octet, bytes):
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        text = octet.decode(encoding, errors='ignore')
                        if '<?xml' in text and '</FatturaElettronica' in text:
                            return text[text.find('<?xml'):text.rfind('>')+1]
                        if '<FatturaElettronica' in text:
                            candidates.append(text)
                    except:
                        continue
        # Salva i candidati su file per analisi manuale
        if p7m_path and candidates:
            with open(p7m_path + '_asn1_xml_candidates.txt', 'w', encoding='utf-8') as f:
                for c in candidates:
                    f.write(c + '\n\n---\n\n')
            print(f"Candidati XML ASN.1 salvati in: {p7m_path}_asn1_xml_candidates.txt")
    except ImportError:
        print("Devi installare la libreria asn1crypto: pip install asn1crypto")
    except Exception as e:
        print(f"Errore ASN.1 advanced: {e}")
    return None

def _cerca_xml_generico(data):
    """Cerca la prima sequenza che inizia con < e finisce con > lunga almeno 1000 caratteri"""
    try:
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            text = data.decode(encoding, errors='ignore')
            start = text.find('<')
            while start != -1:
                end = text.find('>', start)
                if end != -1 and (end - start) > 1000:
                    candidate = text[start:end+1]
                    if '<?xml' in candidate and '</FatturaElettronica' in candidate:
                        return candidate
                start = text.find('<', start+1)
    except:
        pass
    return None

def _salva_sequenze_xml_debug(data, p7m_path):
    """Salva su file tutte le sequenze che iniziano con < e finiscono con > per analisi manuale"""
    try:
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            text = data.decode(encoding, errors='ignore')
            found = False
            with open(p7m_path + '_xml_candidates.txt', 'w', encoding='utf-8') as f:
                start = text.find('<')
                while start != -1:
                    end = text.find('>', start)
                    if end != -1 and (end - start) > 1000:
                        candidate = text[start:end+1]
                        f.write(candidate + '\n\n---\n\n')
                        found = True
                    start = text.find('<', start+1)
            if found:
                print(f"Sequenze XML candidate salvate in: {p7m_path}_xml_candidates.txt")
                break
    except Exception as e:
        print(f"Errore salvataggio sequenze XML candidate: {e}")
    return None

def _is_valid_xml(xml_content):
    """Verifica se il contenuto è XML valido"""
    try:
        objectify.fromstring(xml_content.encode('utf-8'))
        return True
    except:
        return False

def find_child_by_tag(parent, tag):
    """Trova un elemento figlio per tag name (considera solo la parte finale del tag)"""
    for child in parent.iterchildren():
        if child.tag.endswith(tag):
            return child
    return None

def get_cedente_info(header):
    """Estrae informazioni del cedente/prestatore"""
    cedente = find_child_by_tag(header, 'CedentePrestatore')
    if cedente is None:
        return '', '', '', '', ''
    
    dati_anagrafici = find_child_by_tag(cedente, 'DatiAnagrafici')
    id_fiscale_iva = find_child_by_tag(dati_anagrafici, 'IdFiscaleIVA') if dati_anagrafici else None
    
    id_paese = str(id_fiscale_iva.IdPaese) if id_fiscale_iva is not None and hasattr(id_fiscale_iva, 'IdPaese') else ''
    id_codice = str(id_fiscale_iva.IdCodice) if id_fiscale_iva is not None and hasattr(id_fiscale_iva, 'IdCodice') else ''
    
    sede = find_child_by_tag(cedente, 'Sede')
    cap = str(sede.CAP) if sede is not None and hasattr(sede, 'CAP') else ''
    comune = str(sede.Comune) if sede is not None and hasattr(sede, 'Comune') else ''
    provincia = str(sede.Provincia) if sede is not None and hasattr(sede, 'Provincia') else ''
    
    return id_paese, id_codice, cap, comune, provincia

def get_data_fattura(body):
    """Estrae la data della fattura"""
    dati_generali = find_child_by_tag(body, 'DatiGenerali')
    if dati_generali is None:
        return pd.NaT
    
    dati_doc = find_child_by_tag(dati_generali, 'DatiGeneraliDocumento')
    if dati_doc is None or not hasattr(dati_doc, 'Data'):
        return pd.NaT
    
    return pd.to_datetime(str(dati_doc.Data))

def get_linee_dettaglio(body, cedente_info, data_fattura):
    """Estrae le linee di dettaglio della fattura"""
    beni_servizi = find_child_by_tag(body, 'DatiBeniServizi')
    if beni_servizi is None:
        return []
    
    id_paese, id_codice, cap, comune, provincia = cedente_info
    rows = []
    
    for linea in beni_servizi.iterchildren():
        if linea.tag.endswith('DettaglioLinee'):
            # Gestione robusta della quantità
            quantita = -1
            if hasattr(linea, 'Quantita') and linea.Quantita is not None:
                try:
                    quantita = int(float(str(linea.Quantita)))
                except (ValueError, TypeError):
                    quantita = -1
            
            # Gestione robusta di altri campi
            descrizione = str(linea.Descrizione) if hasattr(linea, 'Descrizione') else ''
            
            try:
                prezzo_unitario = float(str(linea.PrezzoUnitario)) if hasattr(linea, 'PrezzoUnitario') else 0.0
            except (ValueError, TypeError):
                prezzo_unitario = 0.0
            
            try:
                prezzo_totale = float(str(linea.PrezzoTotale)) if hasattr(linea, 'PrezzoTotale') else 0.0
            except (ValueError, TypeError):
                prezzo_totale = 0.0
            
            row = {
                'Data': data_fattura,
                'IdPaese': id_paese,
                'IdFiscaleIVA': id_codice,
                'CAP': cap,
                'Comune': comune,
                'Provincia': provincia,
                'Descrizione': descrizione,
                'Quantita': quantita,
                'PrezzoUnitario': prezzo_unitario,
                'PrezzoTotale': prezzo_totale,
            }
            rows.append(row)
    
    return rows

def sanitize_xml(xml_content):
    """Rimuove caratteri di controllo non validi per XML (eccetto tab, CR, LF)"""
    import re
    # Tieni solo caratteri validi per XML 1.0 (https://www.w3.org/TR/xml/#charsets)
    # Qui semplifichiamo: rimuoviamo tutto ciò che non è tab, CR, LF o stampabile
    return re.sub(r'[^\x09\x0A\x0D\x20-\x7E\xA0-\uFFFF]', '', xml_content)

def clean_xml_after_closing_tag(xml_content):
    """Taglia l'XML dopo il primo tag di chiusura FatturaElettronica valido"""
    end_tags = [
        '</ns2:FatturaElettronica>',
        '</FatturaElettronica>',
        '</p:FatturaElettronica>',
        '</n0:FatturaElettronica>',
    ]
    for tag in end_tags:
        idx = xml_content.find(tag)
        if idx != -1:
            return xml_content[:idx+len(tag)]
    return xml_content

def dataframe_linee_da_p7m_python(p7m_path):
    """
    Crea un DataFrame con le linee di dettaglio estratte da un file .p7m
    usando solo librerie Python (versione robusta).
    """
    try:
        # Estrai XML dal file p7m
        xml_content = estrai_xml_da_p7m_python_v2(p7m_path)
        # Sanifica l'XML estratto
        xml_content = sanitize_xml(xml_content)
        # Taglia eventuali dati extra dopo il tag di chiusura
        xml_content = clean_xml_after_closing_tag(xml_content)
        # Parsa l'XML
        xml = objectify.fromstring(xml_content.encode('utf-8'))
        
        # Trova le sezioni principali
        header = find_child_by_tag(xml, 'FatturaElettronicaHeader')
        body = find_child_by_tag(xml, 'FatturaElettronicaBody')
        
        if header is None or body is None:
            raise Exception("Struttura XML della fattura non valida")
        
        # Estrai informazioni
        cedente_info = get_cedente_info(header)
        data_fattura = get_data_fattura(body)
        rows = get_linee_dettaglio(body, cedente_info, data_fattura)
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        raise Exception(f"Errore nell'elaborazione del file p7m: {e}")

# Funzione di utilità per testare l'estrazione
def test_estrazione_p7m(p7m_path):
    """
    Testa l'estrazione XML da un file p7m e mostra informazioni di debug.
    Salva anche l'XML estratto su file per debug.
    """
    try:
        print(f"Tentativo di estrazione da: {p7m_path}")
        xml_content = estrai_xml_da_p7m_python_v2(p7m_path)
        print(f"XML estratto con successo. Lunghezza: {len(xml_content)} caratteri")
        print("Primi 200 caratteri:")
        print(xml_content[:200])
        # Salva l'XML estratto su file per debug
        xml_path = p7m_path + "_estratto.xml"
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        print(f"\nXML estratto salvato in: {xml_path}")
        return True
    except Exception as e:
        print(f"Errore nell'estrazione: {e}")
        return False

# Funzione per debug: analizza struttura del file p7m
def debug_p7m_structure(p7m_path):
    """
    Analizza la struttura di un file p7m per debug.
    """
    with open(p7m_path, 'rb') as f:
        data = f.read()
    
    print(f"File size: {len(data)} bytes")
    print(f"Primi 50 bytes (hex): {data[:50].hex()}")
    print(f"Primi 50 bytes (text): {data[:50]}")
    
    # Cerca pattern comuni
    patterns = [
        b'<?xml',
        b'FatturaElettronica',
        b'CedentePrestatore',
        b'-----BEGIN',
        b'-----END',
    ]
    
    print("\nPattern trovati:")
    for pattern in patterns:
        pos = data.find(pattern)
        if pos != -1:
            print(f"  {pattern.decode('utf-8', errors='ignore')}: posizione {pos}")
    
    # Prova diversi encoding per vedere se ci sono stringhe leggibili
    print("\nTentativo decodifica UTF-8 (primi 200 char):")
    try:
        print(data[:200].decode('utf-8', errors='ignore'))
    except:
        print("Decodifica UTF-8 fallita")
    
    print("\nTentativo decodifica Latin-1 (primi 200 char):")
    try:
        print(data[:200].decode('latin-1', errors='ignore'))
    except:
        print("Decodifica Latin-1 fallita")

def _estrai_xml_bruteforce_binario(data):
    """Cerca la sequenza binaria più lunga che inizia con < e finisce con > e contiene FatturaElettronica"""
    start_bytes = [b'<?xml', b'<FatturaElettronica', b'<n0:FatturaElettronica', b'<p:FatturaElettronica']
    end_bytes = [b'</FatturaElettronica>', b'</n0:FatturaElettronica>', b'</p:FatturaElettronica>', b'</ns2:FatturaElettronica>']
    best_candidate = b''
    for s in start_bytes:
        start = data.find(s)
        while start != -1:
            for e in end_bytes:
                end = data.find(e, start)
                if end != -1:
                    end += len(e)
                    candidate = data[start:end]
                    if b'FatturaElettronica' in candidate and len(candidate) > len(best_candidate):
                        best_candidate = candidate
            start = data.find(s, start+1)
    if best_candidate:
        # Decodifica robusta
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                return best_candidate.decode(encoding, errors='ignore').strip()
            except:
                continue
    return None

def _estrai_xml_bruteforce_compressed(data):
    """Cerca blocchi binari compressi (zlib/deflate) che contengono XML e li decomprime"""
    min_block_size = 500  # blocchi troppo piccoli non sono XML
    max_block_size = 500000  # blocchi troppo grandi sono improbabili
    found = False
    for i in range(len(data)):
        # Cerca possibili header zlib (0x78 0x9C o 0x78 0xDA)
        if data[i:i+2] in [b'\x78\x9c', b'\x78\xda']:
            for j in range(i+min_block_size, min(i+max_block_size, len(data))):
                try:
                    chunk = data[i:j]
                    decompressed = zlib.decompress(chunk)
                    if b'<?xml' in decompressed and b'FatturaElettronica' in decompressed:
                        try:
                            return decompressed.decode('utf-8', errors='ignore').strip()
                        except:
                            return decompressed.decode('latin-1', errors='ignore').strip()
                except Exception:
                    continue
    return None

def _estrai_xml_bruteforce_base64(data):
    """Cerca blocchi base64 lunghi, li decodifica e cerca XML"""
    # Cerca sequenze di almeno 500 caratteri base64
    import re
    b64_pattern = re.compile(b'([A-Za-z0-9+/=\r\n]{500,})')
    for match in b64_pattern.finditer(data):
        b64_block = match.group(1)
        # Rimuovi newline
        b64_block = b64_block.replace(b'\r', b'').replace(b'\n', b'')
        try:
            decoded = base64.b64decode(b64_block, validate=False)
            # Cerca XML nel decodificato
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    text = decoded.decode(encoding, errors='ignore')
                    if '<?xml' in text and 'FatturaElettronica' in text:
                        return text[text.find('<?xml'):text.rfind('>')+1]
                except:
                    continue
        except Exception:
            continue
    return None
