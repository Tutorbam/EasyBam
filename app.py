import asyncio
import logging
import re
import sys
import random
import os
import urllib.parse
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
import base64
import binascii
import json
import aiohttp
from aiohttp import web
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_proxy import ProxyConnector
from dotenv import load_dotenv
import zipfile
import io
import platform
import stat
from drm_decrypter import decrypt_segment

load_dotenv() # Carica le variabili dal file .env

# Configurazione logging
# ✅ CORREZIONE: Imposta un formato standard e assicurati che il logger 'aiohttp.access'
# non venga silenziato, permettendo la visualizzazione dei log di accesso.
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)

# Silenzia i log di accesso di aiohttp a meno che non siano errori
logging.getLogger('aiohttp.access').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# --- Configurazione Proxy ---
def parse_proxies(proxy_env_var: str) -> list:
    """Analizza una stringa di proxy separati da virgola da una variabile d'ambiente."""
    proxies_str = os.environ.get(proxy_env_var, "").strip()
    if proxies_str:
        return [p.strip() for p in proxies_str.split(',') if p.strip()]
    return []

GLOBAL_PROXIES = parse_proxies('GLOBAL_PROXY')
VAVOO_PROXIES = parse_proxies('VAVOO_PROXY')
DLHD_PROXIES = parse_proxies('DLHD_PROXY')

if GLOBAL_PROXIES: logging.info(f"🌍 Caricati {len(GLOBAL_PROXIES)} proxy globali.")
if VAVOO_PROXIES: logging.info(f"🎬 Caricati {len(VAVOO_PROXIES)} proxy Vavoo.")
if DLHD_PROXIES: logging.info(f"📺 Caricati {len(DLHD_PROXIES)} proxy DLHD.")

# Aggiungi path corrente per import moduli
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --- Moduli Esterni ---
# Vengono importati singolarmente per un feedback più granulare in caso di errore.
VavooExtractor, DLHDExtractor, VixSrcExtractor, PlaylistBuilder, SportsonlineExtractor = None, None, None, None, None

try:
    from extractors.vavoo import VavooExtractor
    logger.info("✅ Modulo VavooExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo VavooExtractor non trovato. Funzionalità Vavoo disabilitata.")

try:
    from extractors.dlhd import DLHDExtractor
    logger.info("✅ Modulo DLHDExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo DLHDExtractor non trovato. Funzionalità DLHD disabilitata.")

try:
    from routes.playlist_builder import PlaylistBuilder
    logger.info("✅ Modulo PlaylistBuilder caricato.")
except ImportError:
    logger.warning("⚠️ Modulo PlaylistBuilder non trovato. Funzionalità PlaylistBuilder disabilitata.")
    
try:
    from extractors.vixsrc import VixSrcExtractor
    logger.info("✅ Modulo VixSrcExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo VixSrcExtractor non trovato. Funzionalità VixSrc disabilitata.")

try:
    from extractors.sportsonline import SportsonlineExtractor
    logger.info("✅ Modulo SportsonlineExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo SportsonlineExtractor non trovato. Funzionalità Sportsonline disabilitata.")

# --- Classi Unite ---
class ExtractorError(Exception):
    """Eccezione personalizzata per errori di estrazione"""
    pass

class GenericHLSExtractor:
    def __init__(self, request_headers, proxies=None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = None
        self.proxies = proxies or []

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            proxy = self._get_random_proxy()
            if proxy:
                logging.info(f"Utilizzo del proxy {proxy} per la sessione generica.")
                connector = ProxyConnector.from_url(proxy)
            else:
                connector = TCPConnector(
                    limit=20, limit_per_host=10, 
                    keepalive_timeout=60, enable_cleanup_closed=True, 
                    force_close=False, use_dns_cache=True
                )

            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            self.session = ClientSession(
                timeout=timeout, connector=connector, 
                headers={'user-agent': self.base_headers['user-agent']}
            )
        return self.session

    async def extract(self, url, **kwargs):
        # ✅ AGGIORNATO: Rimossa validazione estensioni su richiesta utente.
        # Accetta qualsiasi URL per evitare errori con segmenti mascherati.
        # if not any(pattern in url.lower() for pattern in ['.m3u8', '.mpd', '.ts', '.js', '.css', '.html', '.txt', 'vixsrc.to/playlist', 'newkso.ru']):
        #     raise ExtractorError("URL non supportato (richiesto .m3u8, .mpd, .ts, .js, .css, .html, .txt, URL VixSrc o URL newkso.ru valido)")

        parsed_url = urlparse(url)
        origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = self.base_headers.copy()
        headers.update({"referer": origin, "origin": origin})

        for h, v in self.request_headers.items():
            if h.lower() in ["authorization", "x-api-key", "x-auth-token"]:
                headers[h] = v

        return {
            "destination_url": url, 
            "request_headers": headers, 
            "mediaflow_endpoint": "hls_proxy"
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

class MPDToHLSConverter:
    """Converte manifest MPD (DASH) in playlist HLS (m3u8) on-the-fly."""
    
    def __init__(self):
        self.ns = {
            'mpd': 'urn:mpeg:dash:schema:mpd:2011',
            'cenc': 'urn:mpeg:cenc:2013'
        }

    def convert_master_playlist(self, manifest_content: str, proxy_base: str, original_url: str, params: str) -> str:
        """Genera la Master Playlist HLS dagli AdaptationSet del MPD."""
        try:
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)
            
            root = ET.fromstring(manifest_content)
            lines = ['#EXTM3U', '#EXT-X-VERSION:3']
            
            # Trova AdaptationSet Video e Audio
            video_sets = []
            audio_sets = []
            
            for adaptation_set in root.findall('.//mpd:AdaptationSet', self.ns):
                mime_type = adaptation_set.get('mimeType', '')
                content_type = adaptation_set.get('contentType', '')
                
                if 'video' in mime_type or 'video' in content_type:
                    video_sets.append(adaptation_set)
                elif 'audio' in mime_type or 'audio' in content_type:
                    audio_sets.append(adaptation_set)
            
            # Fallback per detection
            if not video_sets and not audio_sets:
                for adaptation_set in root.findall('.//mpd:AdaptationSet', self.ns):
                    if adaptation_set.find('mpd:Representation[@mimeType="video/mp4"]', self.ns) is not None:
                        video_sets.append(adaptation_set)
                    elif adaptation_set.find('mpd:Representation[@mimeType="audio/mp4"]', self.ns) is not None:
                        audio_sets.append(adaptation_set)

            # --- GESTIONE AUDIO (EXT-X-MEDIA) ---
            audio_group_id = 'audio'
            has_audio = False
            
            for adaptation_set in audio_sets:
                for representation in adaptation_set.findall('mpd:Representation', self.ns):
                    rep_id = representation.get('id')
                    bandwidth = representation.get('bandwidth', '128000') # Default fallback
                    
                    # Costruisci URL Media Playlist Audio
                    encoded_url = urllib.parse.quote(original_url, safe='')
                    media_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}&format=hls&rep_id={rep_id}{params}"
                    
                    # Usa GROUP-ID 'audio' e NAME basato su ID o lingua
                    lang = adaptation_set.get('lang', 'und')
                    name = f"Audio {lang} ({bandwidth})"
                    
                    # EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="...",DEFAULT=YES,AUTOSELECT=YES,URI="..."
                    # Impostiamo DEFAULT=YES solo per il primo
                    default_attr = "YES" if not has_audio else "NO"
                    
                    lines.append(f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="{audio_group_id}",NAME="{name}",LANGUAGE="{lang}",DEFAULT={default_attr},AUTOSELECT=YES,URI="{media_url}"')
                    has_audio = True

            # --- GESTIONE VIDEO (EXT-X-STREAM-INF) ---
            for adaptation_set in video_sets:
                for representation in adaptation_set.findall('mpd:Representation', self.ns):
                    rep_id = representation.get('id')
                    bandwidth = representation.get('bandwidth')
                    width = representation.get('width')
                    height = representation.get('height')
                    frame_rate = representation.get('frameRate')
                    codecs = representation.get('codecs')
                    
                    encoded_url = urllib.parse.quote(original_url, safe='')
                    media_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}&format=hls&rep_id={rep_id}{params}"
                    
                    inf = f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth}'
                    if width and height:
                        inf += f',RESOLUTION={width}x{height}'
                    if frame_rate:
                        inf += f',FRAME-RATE={frame_rate}'
                    if codecs:
                        inf += f',CODECS="{codecs}"'
                    
                    # Collega il gruppo audio se presente
                    if has_audio:
                        inf += f',AUDIO="{audio_group_id}"'
                    
                    lines.append(inf)
                    lines.append(media_url)
            
            return '\n'.join(lines)
        except Exception as e:
            logging.error(f"Errore conversione Master Playlist: {e}")
            return "#EXTM3U\n#EXT-X-ERROR: " + str(e)

    def convert_media_playlist(self, manifest_content: str, rep_id: str, proxy_base: str, original_url: str, params: str, clearkey_param: str = None) -> str:
        """Genera la Media Playlist HLS per una specifica Representation."""
        try:
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)
                
            root = ET.fromstring(manifest_content)
            
            # Trova la Representation specifica
            representation = None
            adaptation_set = None
            
            # Cerca in tutti gli AdaptationSet
            for aset in root.findall('.//mpd:AdaptationSet', self.ns):
                rep = aset.find(f'mpd:Representation[@id="{rep_id}"]', self.ns)
                if rep is not None:
                    representation = rep
                    adaptation_set = aset
                    break
            
            if representation is None:
                logger.error(f"❌ Representation {rep_id} non trovata nel manifest.")
                return "#EXTM3U\n#EXT-X-ERROR: Representation not found"

            # fMP4 richiede HLS versione 6 o 7
            lines = ['#EXTM3U', '#EXT-X-VERSION:7', '#EXT-X-TARGETDURATION:10', '#EXT-X-PLAYLIST-TYPE:VOD']
            
            # --- GESTIONE DRM (ClearKey) ---
            # Decrittazione lato server con mp4decrypt
            server_side_decryption = False
            decryption_params = ""
            
            if clearkey_param:
                try:
                    kid_hex, key_hex = clearkey_param.split(':')
                    server_side_decryption = True
                    decryption_params = f"&key={key_hex}&key_id={kid_hex}"
                    # Server-side decryption enabled
                except Exception as e:
                    logger.error(f"Errore parsing clearkey_param: {e}")

            # --- GESTIONE SEGMENTI ---
            # SegmentTemplate è il caso più comune per lo streaming live/vod moderno
            segment_template = representation.find('mpd:SegmentTemplate', self.ns)
            if segment_template is None:
                # Fallback: cerca nell'AdaptationSet
                segment_template = adaptation_set.find('mpd:SegmentTemplate', self.ns)
            
            if segment_template is not None:
                timescale = int(segment_template.get('timescale', '1'))
                initialization = segment_template.get('initialization')
                media = segment_template.get('media')
                start_number = int(segment_template.get('startNumber', '1'))
                
                # Risolvi URL base
                base_url_tag = root.find('mpd:BaseURL', self.ns)
                base_url = base_url_tag.text if base_url_tag is not None else os.path.dirname(original_url)
                if not base_url.endswith('/'): base_url += '/'

                # --- INITIALIZATION SEGMENT (EXT-X-MAP) ---
                encoded_init_url = ""
                if initialization:
                    # Processing initialization segment
                    init_url = initialization.replace('$RepresentationID$', str(rep_id))
                    full_init_url = urljoin(base_url, init_url)
                    encoded_init_url = urllib.parse.quote(full_init_url, safe='')
                    
                    # Aggiungiamo EXT-X-MAP solo se NON usiamo decrittazione server
                    # Quando usiamo ffmpeg per decrittare, ogni segmento include già il moov
                    if not server_side_decryption:
                        proxy_init_url = f"{proxy_base}/segment/init.mp4?base_url={encoded_init_url}{params}"
                        lines.append(f'#EXT-X-MAP:URI="{proxy_init_url}"')

                # --- SEGMENT TIMELINE ---
                segment_timeline = segment_template.find('mpd:SegmentTimeline', self.ns)
                if segment_timeline is not None:
                    current_time = 0
                    segment_number = start_number
                    
                    for s in segment_timeline.findall('mpd:S', self.ns):
                        t = s.get('t')
                        if t: current_time = int(t)
                        d = int(s.get('d'))
                        r = int(s.get('r', '0'))
                        
                        duration_sec = d / timescale
                        
                        # Ripeti per r + 1 volte
                        for _ in range(r + 1):
                            # Costruisci URL segmento
                            seg_name = media.replace('$RepresentationID$', str(rep_id))
                            seg_name = seg_name.replace('$Number$', str(segment_number))
                            seg_name = seg_name.replace('$Time$', str(current_time))
                            
                            full_seg_url = urljoin(base_url, seg_name)
                            encoded_seg_url = urllib.parse.quote(full_seg_url, safe='')
                            
                            lines.append(f'#EXTINF:{duration_sec:.3f},')
                            
                            if server_side_decryption:
                                # Usa endpoint di decrittazione
                                # Passiamo init_url perché serve per la concatenazione
                                decrypt_url = f"{proxy_base}/decrypt/segment.mp4?url={encoded_seg_url}&init_url={encoded_init_url}{decryption_params}{params}"
                                lines.append(decrypt_url)
                            else:
                                # Proxy standard
                                proxy_seg_url = f"{proxy_base}/segment/{seg_name}?base_url={encoded_seg_url}{params}"
                                lines.append(proxy_seg_url)
                            
                            current_time += d
                            segment_number += 1
                
                # --- SEGMENT TEMPLATE (DURATION) ---
                else:
                    duration = int(segment_template.get('duration', '0'))
                    if duration > 0:
                        # Stima o limite segmenti (per VOD/Live senza timeline è complicato sapere quanti sono)
                        # Per ora generiamo un numero fisso o basato sulla durata periodo se disponibile
                        period = root.find('mpd:Period', self.ns)
                        period_duration_str = period.get('duration')
                        # Parsing durata ISO8601 (semplificato)
                        # TODO: Implementare parsing durata reale
                        total_segments = 100 # Placeholder
                        
                        duration_sec = duration / timescale
                        
                        for i in range(total_segments):
                            seg_num = start_number + i
                            seg_name = media.replace('$RepresentationID$', str(rep_id))
                            seg_name = seg_name.replace('$Number$', str(seg_num))
                            
                            full_seg_url = urljoin(base_url, seg_name)
                            encoded_seg_url = urllib.parse.quote(full_seg_url, safe='')
                            proxy_seg_url = f"{proxy_base}/segment/seg_{seg_num}.m4s?base_url={encoded_seg_url}{params}"
                            
                            lines.append(f'#EXTINF:{duration_sec:.6f},')
                            lines.append(proxy_seg_url)

            lines.append('#EXT-X-ENDLIST')
            return '\n'.join(lines)

        except Exception as e:
            logging.error(f"Errore conversione Media Playlist: {e}")
            return "#EXTM3U\n#EXT-X-ERROR: " + str(e)

class HLSProxy:
    """Proxy HLS per gestire stream Vavoo, DLHD, HLS generici e playlist builder con supporto AES-128"""
    
    def __init__(self):
        self.extractors = {}
        
        # Inizializza il playlist_builder se il modulo è disponibile
        if PlaylistBuilder:
            self.playlist_builder = PlaylistBuilder()
            logger.info("✅ PlaylistBuilder inizializzato")
        else:
            self.playlist_builder = None
            
        # Inizializza il convertitore MPD -> HLS
        self.mpd_converter = MPDToHLSConverter()
        
        # Cache per segmenti di inizializzazione (URL -> content)
        self.init_cache = {}
        
        # Sessione condivisa per il proxy
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            # Importa aiohttp e ClientTimeout qui se non sono già importati globalmente
            import aiohttp
            from aiohttp import ClientTimeout
            self.session = aiohttp.ClientSession(timeout=ClientTimeout(total=30))
        return self.session

    async def get_extractor(self, url: str, request_headers: dict):
        """Ottiene l'estrattore appropriato per l'URL"""
        try:
            if "vavoo.to" in url:
                key = "vavoo"
                proxies = VAVOO_PROXIES or GLOBAL_PROXIES
                if key not in self.extractors:
                    self.extractors[key] = VavooExtractor(request_headers, proxies=proxies)
                return self.extractors[key]
            elif any(domain in url for domain in ["daddylive", "dlhd"]) or re.search(r'stream-\d+\.php', url):
                key = "dlhd"
                proxies = DLHD_PROXIES or GLOBAL_PROXIES
                if key not in self.extractors:
                    self.extractors[key] = DLHDExtractor(request_headers, proxies=proxies)
                return self.extractors[key]
            elif 'vixsrc.to/' in url.lower() and any(x in url for x in ['/movie/', '/tv/', '/iframe/']):
                key = "vixsrc"
                if key not in self.extractors:
                    self.extractors[key] = VixSrcExtractor(request_headers, proxies=GLOBAL_PROXIES)
                return self.extractors[key]
            elif any(domain in url for domain in ["sportzonline", "sportsonline"]):
                key = "sportsonline"
                proxies = GLOBAL_PROXIES
                if key not in self.extractors:
                    self.extractors[key] = SportsonlineExtractor(request_headers, proxies=proxies)
                return self.extractors[key]
            else:
                # ✅ MODIFICATO: Fallback al GenericHLSExtractor per qualsiasi altro URL.
                # Questo permette di gestire estensioni sconosciute o URL senza estensione.
                key = "hls_generic"
                if key not in self.extractors:
                    self.extractors[key] = GenericHLSExtractor(request_headers, proxies=GLOBAL_PROXIES)
                return self.extractors[key]
        except (NameError, TypeError) as e:
            raise ExtractorError(f"Estrattore non disponibile - modulo mancante: {e}")

    async def handle_proxy_request(self, request):
        """Gestisce le richieste proxy principali"""
        extractor = None
        try:
            target_url = request.query.get('url')
            force_refresh = request.query.get('force', 'false').lower() == 'true'
            if not target_url:
                return web.Response(text="Parametro 'url' mancante", status=400)
            
            try:
                target_url = urllib.parse.unquote(target_url)
            except:
                pass
                
            # Log removed for cleaner output
            
            extractor = await self.get_extractor(target_url, dict(request.headers))
            
            try:
                # Passa il flag force_refresh all'estrattore
                result = await extractor.extract(target_url, force_refresh=force_refresh)
                stream_url = result["destination_url"]
                stream_headers = result.get("request_headers", {})
                
                # Aggiungi headers personalizzati da query params
                for param_name, param_value in request.query.items():
                    if param_name.startswith('h_'):
                        header_name = param_name[2:]
                        stream_headers[header_name] = param_value
                
                # Stream URL resolved
                return await self._proxy_stream(request, stream_url, stream_headers)
            except ExtractorError as e:
                logger.warning(f"Estrazione fallita, tento di nuovo forzando l'aggiornamento: {e}")
                result = await extractor.extract(target_url, force_refresh=True) # Forza sempre il refresh al secondo tentativo
                stream_url = result["destination_url"]
                stream_headers = result.get("request_headers", {})
                # Stream URL resolved after refresh
                return await self._proxy_stream(request, stream_url, stream_headers)
            
        except Exception as e:
            # ✅ AGGIORNATO: Se un estrattore specifico (DLHD, Vavoo) fallisce, riavvia il server per forzare un aggiornamento.
            # Questo è utile se il sito ha cambiato qualcosa e l'estrattore è obsoleto.
            restarting = False
            extractor_name = "sconosciuto"
            if DLHDExtractor and isinstance(extractor, DLHDExtractor):
                restarting = True
                extractor_name = "DLHDExtractor"
            elif VavooExtractor and isinstance(extractor, VavooExtractor):
                restarting = True
                extractor_name = "VavooExtractor"

            if restarting:
                logger.critical(f"❌ Errore critico con {extractor_name}: {e}. Riavvio disabilitato per stabilità.")
                # await asyncio.sleep(1)  # Attesa per il flush dei log
                # os._exit(1)  # Uscita forzata per innescare il riavvio dal process manager (Docker, Gunicorn)

            logger.exception(f"Errore nella richiesta proxy: {str(e)}")
            return web.Response(text=f"Errore proxy: {str(e)}", status=500)

    async def handle_license_request(self, request):
        """✅ NUOVO: Gestisce le richieste di licenza DRM (ClearKey e Proxy)"""
        try:
            # 1. Modalità ClearKey Statica
            clearkey_param = request.query.get('clearkey')
            if clearkey_param:
                logger.info(f"🔑 Richiesta licenza ClearKey statica: {clearkey_param}")
                try:
                    kid_hex, key_hex = clearkey_param.split(':')
                    
                    # Converte hex in base64url (senza padding) come richiesto da JWK
                    def hex_to_b64url(hex_str):
                        return base64.urlsafe_b64encode(binascii.unhexlify(hex_str)).decode('utf-8').rstrip('=')

                    jwk_response = {
                        "keys": [{
                            "kty": "oct",
                            "k": hex_to_b64url(key_hex),
                            "kid": hex_to_b64url(kid_hex),
                            "type": "temporary"
                        }],
                        "type": "temporary"
                    }
                    
                    logger.info(f"🔑 Serving static ClearKey license for KID: {kid_hex}")
                    return web.json_response(jwk_response)
                except Exception as e:
                    logger.error(f"❌ Errore nella generazione della licenza ClearKey statica: {e}")
                    return web.Response(text="Invalid ClearKey format", status=400)

            # 2. Modalità Proxy Licenza
            license_url = request.query.get('url')
            if not license_url:
                return web.Response(text="Missing url parameter", status=400)

            license_url = urllib.parse.unquote(license_url)
            
            # Ricostruisce gli headers
            headers = {}
            for param_name, param_value in request.query.items():
                if param_name.startswith('h_'):
                    header_name = param_name[2:].replace('_', '-')
                    headers[header_name] = param_value

            # Aggiunge headers specifici della richiesta originale (es. content-type per il body)
            if request.headers.get('Content-Type'):
                headers['Content-Type'] = request.headers.get('Content-Type')

            # Legge il body della richiesta (challenge DRM)
            body = await request.read()
            
            logger.info(f"🔐 Proxying License Request to: {license_url}")
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
            
            async with ClientSession() as session:
                async with session.request(
                    request.method, 
                    license_url, 
                    headers=headers, 
                    data=body, 
                    **connector_kwargs
                ) as resp:
                    response_body = await resp.read()
                    logger.info(f"✅ License response: {resp.status} ({len(response_body)} bytes)")
                    
                    response_headers = {
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
                    }
                    # Copia alcuni headers utili dalla risposta originale
                    if 'Content-Type' in resp.headers:
                        response_headers['Content-Type'] = resp.headers['Content-Type']

                    return web.Response(
                        body=response_body,
                        status=resp.status,
                        headers=response_headers
                    )

        except Exception as e:
            logger.error(f"❌ License proxy error: {str(e)}")
            return web.Response(text=f"License error: {str(e)}", status=500)

    async def handle_key_request(self, request):
        """✅ NUOVO: Gestisce richieste per chiavi AES-128"""
        # 1. Gestione chiave statica (da MPD converter)
        static_key = request.query.get('static_key')
        if static_key:
            try:
                key_bytes = binascii.unhexlify(static_key)
                return web.Response(
                    body=key_bytes,
                    content_type='application/octet-stream',
                    headers={'Access-Control-Allow-Origin': '*'}
                )
            except Exception as e:
                logger.error(f"❌ Errore decodifica chiave statica: {e}")
                return web.Response(text="Invalid static key", status=400)

        # 2. Gestione proxy chiave remota
        key_url = request.query.get('key_url')
        
        if not key_url:
            return web.Response(text="Missing key_url or static_key parameter", status=400)
        
        try:
            # Decodifica l'URL se necessario
            try:
                key_url = urllib.parse.unquote(key_url)
            except:
                pass
                
            # Inizializza gli header esclusivamente da quelli passati dinamicamente
            # tramite l'URL. Se l'estrattore non li passa, la richiesta
            # verrà fatta senza header specifici, affidandosi alla correttezza
            # del flusso di estrazione.
            headers = {}
            for param_name, param_value in request.query.items():
                if param_name.startswith('h_'):
                    header_name = param_name[2:].replace('_', '-')
                    headers[header_name] = param_value

            logger.info(f"🔑 Fetching AES key from: {key_url}")
            logger.debug(f"   -> with headers: {headers}")
            
            # ✅ CORREZIONE: Seleziona il proxy corretto (DLHD, Vavoo, etc.) in base all'URL originale.
            # Se non c'è un proxy specifico, usa quello globale come fallback.
            proxy_list = GLOBAL_PROXIES
            original_channel_url = request.query.get('original_channel_url')

            # Se l'URL della chiave è un dominio newkso.ru o l'URL originale è di DLHD, usa il proxy DLHD.
            if "newkso.ru" in key_url or (original_channel_url and any(domain in original_channel_url for domain in ["daddylive", "dlhd"])):
                proxy_list = DLHD_PROXIES or GLOBAL_PROXIES
            elif original_channel_url and "vavoo.to" in original_channel_url:
                proxy_list = VAVOO_PROXIES or GLOBAL_PROXIES
            
            proxy = random.choice(proxy_list) if proxy_list else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.info(f"Utilizzo del proxy {proxy} per la richiesta della chiave.")
            
            timeout = ClientTimeout(total=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(key_url, headers=headers, **connector_kwargs) as resp:
                    if resp.status == 200:
                        key_data = await resp.read()
                        logger.info(f"✅ AES key fetched successfully: {len(key_data)} bytes")
                        
                        return web.Response(
                            body=key_data,
                            content_type="application/octet-stream",
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Headers": "*",
                                "Cache-Control": "no-cache, no-store, must-revalidate"
                            }
                        )
                    else:
                        logger.error(f"❌ Key fetch failed with status: {resp.status}")
                        # --- LOGICA DI INVALIDAZIONE AUTOMATICA ---
                        # Se il recupero della chiave fallisce, è probabile che la cache
                        # dell'estrattore sia obsoleta. Invalidiamola.
                        try:
                            url_param = request.query.get('original_channel_url') # ✅ CORREZIONE: Usa il parametro corretto
                            if url_param:
                                extractor = await self.get_extractor(url_param, {})
                                if hasattr(extractor, 'invalidate_cache_for_url'):
                                    await extractor.invalidate_cache_for_url(url_param)
                        except Exception as cache_e:
                            logger.error(f"⚠️ Errore durante l'invalidazione automatica della cache: {cache_e}")
                        # --- FINE LOGICA ---
                        return web.Response(text=f"Key fetch failed: {resp.status}", status=resp.status)
                        
        except Exception as e:
            logger.error(f"❌ Error fetching AES key: {str(e)}")
            return web.Response(text=f"Key error: {str(e)}", status=500)

    async def handle_ts_segment(self, request):
        """Gestisce richieste per segmenti .ts"""
        try:
            segment_name = request.match_info.get('segment')
            base_url = request.query.get('base_url')
            
            if not base_url:
                return web.Response(text="Base URL mancante per segmento", status=400)
            
            base_url = urllib.parse.unquote(base_url)
            
            if base_url.endswith('/'):
                segment_url = f"{base_url}{segment_name}"
            else:
                # ✅ CORREZIONE: Se base_url è un URL completo (es. generato dal converter), usalo direttamente.
                # Altrimenti, assumi che sia una directory e accoda il nome del segmento.
                # Aggiunto supporto per .m4i (init segments), .m4a (audio), .m4v (video)
                if any(ext in base_url for ext in ['.mp4', '.m4s', '.ts', '.m4i', '.m4a', '.m4v']):
                    segment_url = base_url
                else:
                    segment_url = f"{base_url.rsplit('/', 1)[0]}/{segment_name}"
            
            logger.info(f"📦 Proxy Segment: {segment_name} -> {segment_url}")
            
            # Gestisce la risposta del proxy per il segmento
            return await self._proxy_segment(request, segment_url, {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "referer": base_url
            }, segment_name)
            
        except Exception as e:
            logger.error(f"Errore nel proxy segmento .ts: {str(e)}")
            return web.Response(text=f"Errore segmento: {str(e)}", status=500)

    async def _proxy_segment(self, request, segment_url, stream_headers, segment_name):
        """✅ NUOVO: Proxy dedicato per segmenti .ts con Content-Disposition"""
        try:
            headers = dict(stream_headers)
            
            # Passa attraverso alcuni headers del client
            for header in ['range', 'if-none-match', 'if-modified-since']:
                if header in request.headers:
                    headers[header] = request.headers[header]
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.info(f"Utilizzo del proxy {proxy} per il segmento .ts.")

            timeout = ClientTimeout(total=60, connect=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(segment_url, headers=headers, **connector_kwargs) as resp:
                    response_headers = {}
                    
                    for header in ['content-type', 'content-length', 'content-range', 
                                 'accept-ranges', 'last-modified', 'etag']:
                        if header in resp.headers:
                            response_headers[header] = resp.headers[header]
                    
                    # Forza il content-type e aggiunge Content-Disposition per .ts
                    response_headers['Content-Type'] = 'video/MP2T'
                    response_headers['Content-Disposition'] = f'attachment; filename="{segment_name}"'
                    response_headers['Access-Control-Allow-Origin'] = '*'
                    response_headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
                    response_headers['Access-Control-Allow-Headers'] = 'Range, Content-Type'
                    
                    response = web.StreamResponse(
                        status=resp.status,
                        headers=response_headers
                    )
                    
                    await response.prepare(request)
                    
                    async for chunk in resp.content.iter_chunked(8192):
                        await response.write(chunk)
                    
                    await response.write_eof()
                    return response
                    
        except Exception as e:
            logger.error(f"Errore nel proxy del segmento: {str(e)}")
            return web.Response(text=f"Errore segmento: {str(e)}", status=500)

    async def _proxy_stream(self, request, stream_url, stream_headers):
        """Effettua il proxy dello stream con gestione manifest e AES-128"""
        try:
            headers = dict(stream_headers)
            
            # Passa attraverso alcuni headers del client
            for header in ['range', 'if-none-match', 'if-modified-since']:
                if header in request.headers:
                    headers[header] = request.headers[header]
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.info(f"Utilizzo del proxy {proxy} per lo stream.")

            timeout = ClientTimeout(total=60, connect=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(stream_url, headers=headers, **connector_kwargs) as resp:
                    content_type = resp.headers.get('content-type', '')
                    
                    # Gestione special per manifest HLS
                    # ✅ CORREZIONE: Gestisce anche i manifest mascherati da .css (usati da DLHD)
                    if 'mpegurl' in content_type or stream_url.endswith('.m3u8') or (stream_url.endswith('.css') and 'newkso.ru' in stream_url):
                        manifest_content = await resp.text()
                        
                        # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        proxy_base = f"{scheme}://{host}"
                        original_channel_url = request.query.get('url', '')
                        # Proxy base constructed
                        
                        rewritten_manifest = await self._rewrite_manifest_urls(
                            manifest_content, stream_url, proxy_base, headers, original_channel_url
                        )
                        
                        return web.Response(
                            text=rewritten_manifest,
                            headers={
                                'Content-Type': 'application/vnd.apple.mpegurl',
                                'Content-Disposition': 'attachment; filename="stream.m3u8"',
                                'Access-Control-Allow-Origin': '*',
                                'Cache-Control': 'no-cache'
                            }
                        )
                    
                    # ✅ AGGIORNATO: Gestione per manifest MPD (DASH)
                    elif 'dash+xml' in content_type or stream_url.endswith('.mpd'):
                        manifest_content = await resp.text()
                        
                        # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        proxy_base = f"{scheme}://{host}"
                        # MPD proxy base constructed
                        
                        # Recupera parametri
                        clearkey_param = request.query.get('clearkey')
                        req_format = request.query.get('format')
                        rep_id = request.query.get('rep_id')
                        
                        # --- CONVERSIONE MPD -> HLS ---
                        # Se richiesto formato HLS o se l'URL del proxy termina con .m3u8 (default)
                        # e non stiamo chiedendo esplicitamente il manifest originale riscritto
                        if req_format == 'hls' or (request.path.endswith('.m3u8') and req_format != 'mpd'):
                            
                            # Costruiamo i parametri da passare ai sottolink
                            params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items() if key.lower() in ['user-agent', 'referer', 'origin', 'authorization']])
                            if clearkey_param:
                                params += f"&clearkey={clearkey_param}"
                            
                            if rep_id:
                                # Genera Media Playlist per la variante specifica
                                hls_content = self.mpd_converter.convert_media_playlist(
                                    manifest_content, rep_id, proxy_base, stream_url, params, clearkey_param
                                )
                                return web.Response(
                                    text=hls_content,
                                    headers={
                                        'Content-Type': 'application/vnd.apple.mpegurl',
                                        'Content-Disposition': 'attachment; filename="playlist.m3u8"',
                                        'Access-Control-Allow-Origin': '*',
                                        'Cache-Control': 'no-cache'
                                    }
                                )
                            else:
                                # Genera Master Playlist
                                hls_content = self.mpd_converter.convert_master_playlist(
                                    manifest_content, proxy_base, stream_url, params
                                )
                                return web.Response(
                                    text=hls_content,
                                    headers={
                                        'Content-Type': 'application/vnd.apple.mpegurl',
                                        'Content-Disposition': 'attachment; filename="master.m3u8"',
                                        'Access-Control-Allow-Origin': '*',
                                        'Cache-Control': 'no-cache'
                                    }
                                )

                        # --- MPD REWRITING (DASH NATIVO) ---
                        rewritten_manifest = self._rewrite_mpd_manifest(manifest_content, stream_url, proxy_base, headers, clearkey_param)
                        
                        return web.Response(
                            text=rewritten_manifest,
                            headers={
                                'Content-Type': 'application/dash+xml',
                                'Content-Disposition': 'attachment; filename="stream.mpd"',
                                'Access-Control-Allow-Origin': '*',
                                'Cache-Control': 'no-cache'
                            })
                    
                    # Streaming normale per altri tipi di contenuto
                    response_headers = {}
                    
                    for header in ['content-type', 'content-length', 'content-range', 
                                 'accept-ranges', 'last-modified', 'etag']:
                        if header in resp.headers:
                            response_headers[header] = resp.headers[header]
                    
                    response_headers['Access-Control-Allow-Origin'] = '*'
                    response_headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
                    response_headers['Access-Control-Allow-Headers'] = 'Range, Content-Type'
                    
                    response = web.StreamResponse(
                        status=resp.status,
                        headers=response_headers
                    )
                    
                    await response.prepare(request)
                    
                    async for chunk in resp.content.iter_chunked(8192):
                        await response.write(chunk)
                    
                    await response.write_eof()
                    return response
                    
        except Exception as e:
            logger.error(f"Errore nel proxy dello stream: {str(e)}")
            return web.Response(text=f"Errore stream: {str(e)}", status=500)

    def _rewrite_mpd_manifest(self, manifest_content: str, base_url: str, proxy_base: str, stream_headers: dict, clearkey_param: str = None) -> str:
        """Riscrive i manifest MPD (DASH) per passare attraverso il proxy."""
        try:
            # Aggiungiamo il namespace di default se non presente, per ET
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)

            root = ET.fromstring(manifest_content)
            ns = {'mpd': 'urn:mpeg:dash:schema:mpd:2011', 'cenc': 'urn:mpeg:cenc:2013', 'dashif': 'http://dashif.org/guidelines/clearKey'}
            
            # Registra i namespace per evitare prefissi ns0
            ET.register_namespace('', ns['mpd'])
            ET.register_namespace('cenc', ns['cenc'])
            ET.register_namespace('dashif', ns['dashif'])

            # Includiamo solo gli header rilevanti per evitare URL troppo lunghi
            header_params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items() if key.lower() in ['user-agent', 'referer', 'origin', 'authorization']])

            def create_proxy_url(relative_url):
                absolute_url = urljoin(base_url, relative_url)
                encoded_url = urllib.parse.quote(absolute_url, safe='')
                return f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}{header_params}"

            # --- GESTIONE CLEARKEY STATICA ---
            if clearkey_param:
                # Se è presente il parametro clearkey, iniettiamo il ContentProtection
                # clearkey_param formato: id:key (hex)
                try:
                    kid_hex, key_hex = clearkey_param.split(':')
                    
                    # Crea l'elemento ContentProtection per ClearKey
                    cp_element = ET.Element('ContentProtection')
                    cp_element.set('schemeIdUri', 'urn:uuid:e2719d58-a985-b3c9-781a-007147f192ec')
                    cp_element.set('value', 'ClearKey')
                    
                    # Aggiungi l'elemento Laurl (License Acquisition URL)
                    # Puntiamo al nostro endpoint /license con i parametri necessari
                    license_url = f"{proxy_base}/license?clearkey={clearkey_param}"
                    
                    # 1. Laurl standard (namespace MPD) - alcuni player lo usano
                    laurl_element = ET.SubElement(cp_element, '{urn:mpeg:dash:schema:mpd:2011}Laurl')
                    laurl_element.text = license_url
                    
                    # 2. dashif:Laurl (namespace DashIF) - standard de facto per ClearKey
                    laurl_dashif = ET.SubElement(cp_element, '{http://dashif.org/guidelines/clearKey}Laurl')
                    laurl_dashif.text = license_url
                    
                    # 3. Aggiungi cenc:default_KID per aiutare il player a identificare la chiave
                    # Formatta il KID con i trattini: 8-4-4-4-12
                    if len(kid_hex) == 32:
                        kid_guid = f"{kid_hex[:8]}-{kid_hex[8:12]}-{kid_hex[12:16]}-{kid_hex[16:20]}-{kid_hex[20:]}"
                        cp_element.set('{urn:mpeg:cenc:2013}default_KID', kid_guid)

                    # Inietta ContentProtection nel primo AdaptationSet trovato (o dove appropriato)
                    # Per semplicità, lo aggiungiamo a tutti gli AdaptationSet se non presente
                    adaptation_sets = root.findall('.//mpd:AdaptationSet', ns)
                    logger.info(f"🔎 Trovati {len(adaptation_sets)} AdaptationSet nel manifest.")
                    
                    for adaptation_set in adaptation_sets:
                        # RIMUOVI altri ContentProtection (es. Widevine, PlayReady) per forzare ClearKey
                        # Questo è fondamentale perché i browser preferiscono Widevine se presente
                        for cp in adaptation_set.findall('mpd:ContentProtection', ns):
                            scheme = cp.get('schemeIdUri', '').lower()
                            # ClearKey UUID: e2719d58-a985-b3c9-781a-007147f192ec
                            if 'e2719d58-a985-b3c9-781a-007147f192ec' not in scheme:
                                adaptation_set.remove(cp)
                                logger.info(f"🗑️ Rimosso ContentProtection conflittuale: {scheme}")

                        # Verifica se esiste già un ContentProtection ClearKey
                        existing_cp = False
                        for cp in adaptation_set.findall('mpd:ContentProtection', ns):
                            if cp.get('schemeIdUri') == 'urn:uuid:e2719d58-a985-b3c9-781a-007147f192ec':
                                existing_cp = True
                                break
                        
                        if not existing_cp:
                            adaptation_set.insert(0, cp_element)
                            logger.info(f"💉 Iniettato ContentProtection ClearKey statico in AdaptationSet")
                        else:
                            logger.info(f"⚠️ ContentProtection ClearKey già presente in AdaptationSet, salto iniezione.")

                except Exception as e:
                    logger.error(f"❌ Errore nel parsing del parametro clearkey: {e}")

            # --- GESTIONE PROXY LICENZE ESISTENTI ---
            # Cerca ContentProtection esistenti e riscrive le URL di licenza
            for cp in root.findall('.//mpd:ContentProtection', ns):
                # Cerca elementi che contengono URL di licenza (es. dashif:Laurl, laurl, ecc.)
                # Nota: Questo è un tentativo generico, potrebbe richiedere adattamenti per specifici schemi
                for child in cp:
                    if 'Laurl' in child.tag and child.text:
                        original_license_url = child.text
                        encoded_license_url = urllib.parse.quote(original_license_url, safe='')
                        proxy_license_url = f"{proxy_base}/license?url={encoded_license_url}{header_params}"
                        child.text = proxy_license_url
                        logger.info(f"🔄 Redirected License URL: {original_license_url} -> {proxy_license_url}")

            # Riscrive gli attributi 'media' e 'initialization' in <SegmentTemplate>
            for template_tag in root.findall('.//mpd:SegmentTemplate', ns):
                for attr in ['media', 'initialization']:
                    if template_tag.get(attr):
                        template_tag.set(attr, create_proxy_url(template_tag.get(attr)))
            
            # Riscrive l'attributo 'media' in <SegmentURL>
            for seg_url_tag in root.findall('.//mpd:SegmentURL', ns):
                if seg_url_tag.get('media'):
                    seg_url_tag.set('media', create_proxy_url(seg_url_tag.get('media')))

            # Riscrive BaseURL se presente
            for base_url_tag in root.findall('.//mpd:BaseURL', ns):
                if base_url_tag.text:
                    base_url_tag.text = create_proxy_url(base_url_tag.text)

            return ET.tostring(root, encoding='unicode', method='xml')

        except Exception as e:
            logger.error(f"❌ Errore durante la riscrittura del manifest MPD: {e}")
            return manifest_content # Restituisce il contenuto originale in caso di errore

    async def _rewrite_manifest_urls(self, manifest_content: str, base_url: str, proxy_base: str, stream_headers: dict, original_channel_url: str = '') -> str:
        """✅ AGGIORNATA: Riscrive gli URL nei manifest HLS per passare attraverso il proxy (incluse chiavi AES)"""
        lines = manifest_content.split('\n')
        rewritten_lines = []
        
        # ✅ NUOVO: Logica speciale per VixSrc
        # Determina se l'URL base è di VixSrc per applicare la logica personalizzata.
        is_vixsrc_stream = False
        try:
            # Usiamo l'URL originale della richiesta per determinare l'estrattore
            # Questo è più affidabile di `base_url` che potrebbe essere già un URL di playlist.
            original_request_url = stream_headers.get('referer', base_url)
            extractor = await self.get_extractor(original_request_url, {})
            if hasattr(extractor, 'is_vixsrc') and extractor.is_vixsrc:
                is_vixsrc_stream = True
                logger.info("Rilevato stream VixSrc. Applicherò la logica di filtraggio qualità e non-proxy.")
        except Exception:
            # Se l'estrattore non viene trovato, procedi normalmente.
            pass

        if is_vixsrc_stream:
            streams = []
            for i, line in enumerate(lines):
                if line.startswith('#EXT-X-STREAM-INF:'):
                    bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
                    if bandwidth_match:
                        bandwidth = int(bandwidth_match.group(1))
                        streams.append({'bandwidth': bandwidth, 'inf': line, 'url': lines[i+1]})
            
            if streams:
                # Filtra per la qualità più alta
                highest_quality_stream = max(streams, key=lambda x: x['bandwidth'])
                logger.info(f"VixSrc: Trovata qualità massima con bandwidth {highest_quality_stream['bandwidth']}.")
                
                # Ricostruisci il manifest solo con la qualità più alta e gli URL originali
                rewritten_lines.append('#EXTM3U')
                for line in lines:
                    if line.startswith('#EXT-X-MEDIA:') or line.startswith('#EXT-X-STREAM-INF:') or (line and not line.startswith('#')):
                        continue # Salta i vecchi tag di stream e media
                
                # Aggiungi i tag media e lo stream di qualità più alta
                rewritten_lines.extend([line for line in lines if line.startswith('#EXT-X-MEDIA:')])
                rewritten_lines.append(highest_quality_stream['inf'])
                rewritten_lines.append(highest_quality_stream['url'])
                return '\n'.join(rewritten_lines)

        # Logica standard per tutti gli altri stream
        header_params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items() if key.lower() in ['user-agent', 'referer', 'origin', 'authorization']])

        for line in lines:
            line = line.strip()
            
            # ✅ NUOVO: Gestione chiavi AES-128
            if line.startswith('#EXT-X-KEY:') and 'URI=' in line:
                # Trova e sostituisci l'URI della chiave AES
                uri_start = line.find('URI="') + 5
                uri_end = line.find('"', uri_start)
                
                if uri_start > 4 and uri_end > uri_start:
                    original_key_url = line[uri_start:uri_end]
                    
                    # ✅ CORREZIONE: Usa urljoin per costruire l'URL assoluto della chiave in modo sicuro.
                    absolute_key_url = urljoin(base_url, original_key_url)
                    
                    # Crea URL proxy per la chiave
                    encoded_key_url = urllib.parse.quote(absolute_key_url, safe='')
                    # ✅ AGGIUNTO: Passa l'URL originale del canale per l'invalidazione della cache
                    encoded_original_channel_url = urllib.parse.quote(original_channel_url, safe='')
                    proxy_key_url = f"{proxy_base}/key?key_url={encoded_key_url}&original_channel_url={encoded_original_channel_url}"

                    # Aggiungi gli header necessari come parametri h_
                    # Questo permette al gestore della chiave di usare il contesto corretto
                    # ✅ CORREZIONE: Passa tutti gli header rilevanti alla richiesta della chiave
                    # per garantire l'autenticazione corretta.
                    key_header_params = "".join(
                        [f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" 
                         for key, value in stream_headers.items() if key.lower() in ['user-agent', 'referer', 'origin', 'authorization']]
                    )
                    proxy_key_url += key_header_params
                    
                    # Sostituisci l'URI nel tag EXT-X-KEY
                    new_line = line[:uri_start] + proxy_key_url + line[uri_end:]
                    rewritten_lines.append(new_line)
                    logger.info(f"🔄 Redirected AES key: {absolute_key_url} -> {proxy_key_url}")
                else:
                    rewritten_lines.append(line)
            
            # ✅ NUOVO: Gestione per i sottotitoli e altri media nel tag #EXT-X-MEDIA
            elif line.startswith('#EXT-X-MEDIA:') and 'URI=' in line:
                uri_start = line.find('URI="') + 5
                uri_end = line.find('"', uri_start)
                
                if uri_start > 4 and uri_end > uri_start:
                    original_media_url = line[uri_start:uri_end]
                    
                    # Costruisci l'URL assoluto e poi il proxy URL
                    absolute_media_url = urljoin(base_url, original_media_url)
                    encoded_media_url = urllib.parse.quote(absolute_media_url, safe='')
                    
                    # I sottotitoli sono manifest, quindi usano l'endpoint del proxy principale
                    proxy_media_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_media_url}{header_params}"
                    
                    # Sostituisci l'URI nel tag
                    new_line = line[:uri_start] + proxy_media_url + line[uri_end:]
                    rewritten_lines.append(new_line)
                    logger.info(f"🔄 Redirected Media URL: {absolute_media_url} -> {proxy_media_url}")
                else:
                    rewritten_lines.append(line)

            # Gestione segmenti video e sub-manifest, sia relativi che assoluti
            elif line and not line.startswith('#'):
                # ✅ CORREZIONE: Riscrive qualsiasi URL relativo o assoluto che non sia un tag.
                # Distingue tra manifest (.m3u8, .css) e segmenti (.ts, .html, etc.).
                absolute_url = urljoin(base_url, line) if not line.startswith('http') else line
                encoded_url = urllib.parse.quote(absolute_url, safe='')
                
                # I sub-manifest o URL che potrebbero contenere altri manifest vengono inviati all'endpoint proxy.
                if any(ext in line for ext in ['.m3u8', '.css']):
                    proxy_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}{header_params}"
                    rewritten_lines.append(proxy_url)
                # Tutti gli altri (segmenti .ts, .html, etc.) vengono inviati allo stesso endpoint,
                # ma la logica in _proxy_stream li tratterà come stream diretti e non come manifest da processare.
                else:
                    proxy_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}{header_params}"
                    rewritten_lines.append(proxy_url)

            else:
                # Aggiunge tutti gli altri tag (es. #EXTINF, #EXT-X-ENDLIST)
                rewritten_lines.append(line)
        
        return '\n'.join(rewritten_lines)

    async def handle_playlist_request(self, request):
        """Gestisce le richieste per il playlist builder"""
        if not self.playlist_builder:
            return web.Response(text="❌ Playlist Builder non disponibile - modulo mancante", status=503)
            
        try:
            url_param = request.query.get('url')
            
            if not url_param:
                return web.Response(text="Parametro 'url' mancante", status=400)
            
            if not url_param.strip():
                return web.Response(text="Parametro 'url' non può essere vuoto", status=400)
            
            playlist_definitions = [def_.strip() for def_ in url_param.split(';') if def_.strip()]
            if not playlist_definitions:
                return web.Response(text="Nessuna definizione playlist valida trovata", status=400)
            
            # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
            scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
            host = request.headers.get('X-Forwarded-Host', request.host)
            base_url = f"{scheme}://{host}"
            
            async def generate_response():
                async for line in self.playlist_builder.async_generate_combined_playlist(
                    playlist_definitions, base_url
                ):
                    yield line.encode('utf-8')
            
            response = web.StreamResponse(
                status=200,
                headers={
                    'Content-Type': 'application/vnd.apple.mpegurl',
                    'Content-Disposition': 'attachment; filename="playlist.m3u"',
                    'Access-Control-Allow-Origin': '*'
                }
            )
            
            await response.prepare(request)
            
            async for chunk in generate_response():
                await response.write(chunk)
            
            await response.write_eof()
            return response
            
        except Exception as e:
            logger.error(f"Errore generale nel playlist handler: {str(e)}")
            return web.Response(text=f"Errore: {str(e)}", status=500)

    def _read_template(self, filename: str) -> str:
        """Funzione helper per leggere un file di template."""
        template_path = os.path.join(os.path.dirname(__file__), 'templates', filename)
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()

    async def handle_root(self, request):
        """Serve la pagina principale index.html."""
        try:
            html_content = self._read_template('index.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'index.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Pagina non trovata.</p>", status=500, content_type='text/html')

    async def handle_builder(self, request):
        """Gestisce l'interfaccia web del playlist builder."""
        try:
            html_content = self._read_template('builder.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'builder.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Impossibile caricare l'interfaccia builder.</p>", status=500, content_type='text/html')

    async def handle_info_page(self, request):
        """Serve la pagina HTML delle informazioni."""
        try:
            html_content = self._read_template('info.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'info.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Impossibile caricare la pagina info.</p>", status=500, content_type='text/html')

    async def handle_options(self, request):
        """Gestisce richieste OPTIONS per CORS"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
            'Access-Control-Allow-Headers': 'Range, Content-Type',
            'Access-Control-Max-Age': '86400'
        }
        return web.Response(headers=headers)

    async def handle_api_info(self, request):
        """Endpoint API che restituisce le informazioni sul server in formato JSON."""
        info = {
            "proxy": "HLS Proxy Server",
            "version": "2.5.0",  # Aggiornata per supporto AES-128
            "status": "✅ Funzionante",
            "features": [
                "✅ Proxy HLS streams",
                "✅ AES-128 key proxying",  # ✅ NUOVO
                "✅ Playlist building",
                "✅ Supporto Proxy (SOCKS5, HTTP/S)",
                "✅ Multi-extractor support",
                "✅ CORS enabled"
            ],
            "extractors_loaded": list(self.extractors.keys()),
            "modules": {
                "playlist_builder": PlaylistBuilder is not None,
                "vavoo_extractor": VavooExtractor is not None,
                "dlhd_extractor": DLHDExtractor is not None,
                "vixsrc_extractor": VixSrcExtractor is not None,
                "sportsonline_extractor": SportsonlineExtractor is not None,
            },
            "proxy_config": {
                "global": f"{len(GLOBAL_PROXIES)} proxies caricati",
                "vavoo": f"{len(VAVOO_PROXIES)} proxies caricati",
                "dlhd": f"{len(DLHD_PROXIES)} proxies caricati",
            },
            "endpoints": {
                "/proxy/manifest.m3u8": "Proxy principale - ?url=<URL>",
                "/key": "Proxy chiavi AES-128 - ?key_url=<URL>",  # ✅ NUOVO
                "/playlist": "Playlist builder - ?url=<definizioni>",
                "/builder": "Interfaccia web per playlist builder",
                "/segment/{segment}": "Proxy per segmenti .ts - ?base_url=<URL>",
                "/license": "Proxy licenze DRM (ClearKey/Widevine) - ?url=<URL> o ?clearkey=<id:key>",
                "/info": "Pagina HTML con informazioni sul server",
                "/api/info": "Endpoint JSON con informazioni sul server"
            },
            "usage_examples": {
                "proxy": "/proxy/manifest.m3u8?url=https://example.com/stream.m3u8",
                "aes_key": "/key?key_url=https://server.com/key.bin",  # ✅ NUOVO
                "playlist": "/playlist?url=http://example.com/playlist1.m3u8;http://example.com/playlist2.m3u8",
                "custom_headers": "/proxy/manifest.m3u8?url=<URL>&h_Authorization=Bearer%20token"
            }
        }
        return web.json_response(info)

    async def handle_decrypt_segment(self, request):
        """✅ Decritta segmenti fMP4 lato server usando Python (PyCryptodome)."""  
        url = request.query.get('url')
        init_url = request.query.get('init_url')
        key = request.query.get('key')
        key_id = request.query.get('key_id')
        
        if not url or not key or not key_id:
            return web.Response(text="Missing url, key, or key_id", status=400)

        try:
            session = await self._get_session()

            # --- 1. Scarica Initialization Segment (con cache) ---
            init_content = b""
            if init_url:
                if init_url in self.init_cache:
                    init_content = self.init_cache[init_url]
                else:
                    async with session.get(init_url) as resp:
                        if resp.status == 200:
                            init_content = await resp.read()
                            self.init_cache[init_url] = init_content
                        else:
                            logger.error(f"❌ Failed to fetch init segment: {resp.status}")
                            return web.Response(status=502)

            # --- 2. Scarica Media Segment ---
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Failed to fetch segment: {resp.status}")
                    return web.Response(status=502)
                
                segment_content = await resp.read()

            # --- 3. Decritta con Python (PyCryptodome) ---
            decrypted_content = decrypt_segment(init_content, segment_content, key_id, key)

            # --- 4. Invia Risposta ---
            return web.Response(
                body=decrypted_content,
                status=200,
                headers={'Content-Type': 'video/mp4', 'Access-Control-Allow-Origin': '*'}
            )

        except Exception as e:
            logger.error(f"❌ Decryption error: {e}")
            import traceback
            traceback.print_exc()
            return web.Response(status=500, text=f"Decryption failed: {str(e)}")

    async def cleanup(self):
        """Pulizia delle risorse"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                
            for extractor in self.extractors.values():
                if hasattr(extractor, 'close'):
                    await extractor.close()
        except Exception as e:
            logger.error(f"Errore durante cleanup: {e}")

# --- Logica di Avvio ---
def create_app():
    """Crea e configura l'applicazione aiohttp."""
    proxy = HLSProxy()
    
    app = web.Application()
    
    # Registra le route
    app.router.add_get('/', proxy.handle_root)
    app.router.add_get('/builder', proxy.handle_builder)
    app.router.add_get('/info', proxy.handle_info_page)
    app.router.add_get('/api/info', proxy.handle_api_info)
    app.router.add_get('/key', proxy.handle_key_request)
    app.router.add_get('/proxy/manifest.m3u8', proxy.handle_proxy_request)
    app.router.add_get('/playlist', proxy.handle_playlist_request)
    app.router.add_get('/segment/{segment}', proxy.handle_ts_segment)
    app.router.add_get('/decrypt/segment.mp4', proxy.handle_decrypt_segment) # ✅ NUOVO ROUTE
    
    # Route per licenze DRM (GET e POST)
    app.router.add_get('/license', proxy.handle_license_request)
    app.router.add_post('/license', proxy.handle_license_request)
    
    # Gestore OPTIONS generico per CORS
    app.router.add_route('OPTIONS', '/{tail:.*}', proxy.handle_options)
    
    async def cleanup_handler(app):
        await proxy.cleanup()
    app.on_cleanup.append(cleanup_handler)
    
    return app

# Crea l'istanza "privata" dell'applicazione aiohttp.
app = create_app()

def main():
    """Funzione principale per avviare il server."""
    # Workaround per il bug di asyncio su Windows con ConnectionResetError
    if sys.platform == 'win32':
        # Silenzia il logger di asyncio per evitare spam di ConnectionResetError
        logging.getLogger('asyncio').setLevel(logging.CRITICAL)

    print("🚀 Avvio HLS Proxy Server...")
    print("📡 Server disponibile su: http://localhost:7860")
    print("📡 Oppure: http://server-ip:7860")
    print("🔗 Endpoints:")
    print("   • / - Pagina principale")
    print("   • /builder - Interfaccia web per il builder di playlist")
    print("   • /info - Pagina con informazioni sul server")
    print("   • /proxy/manifest.m3u8?url=<URL> - Proxy principale per stream")
    print("   • /playlist?url=<definizioni> - Generatore di playlist")
    print("=" * 50)
    
    web.run_app(
        app, # Usa l'istanza aiohttp originale per il runner integrato
        host='0.0.0.0',
        port=7860
    )

if __name__ == '__main__':
    main()
