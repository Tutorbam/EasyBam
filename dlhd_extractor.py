import asyncio
import logging
import re
import base64
import json
import os
import gzip
import zlib
import zstandard
import random
from urllib.parse import urlparse, quote_plus
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_proxy import ProxyConnector
from typing import Dict, Any, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DLHDExtractor:
    """DLHD Extractor con sessione persistente e gestione anti-bot avanzata"""

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            # ✅ User-Agent più recente per bypassare protezioni anti-bot
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._cached_base_url = None
        self._iframe_context = None
        self._session_lock = asyncio.Lock()
        self.proxies = proxies or []
        self._extraction_locks: Dict[str, asyncio.Lock] = {} # ✅ NUOVO: Lock per evitare estrazioni multiple
        self.cache_file = os.path.join(os.path.dirname(__file__), '.dlhd_cache')
        self._stream_data_cache: Dict[str, Dict[str, Any]] = self._load_cache()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Carica la cache da un file codificato in Base64 all'avvio."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    logger.info(f"💾 Caricamento cache dal file: {self.cache_file}")
                    encoded_data = f.read()
                    if not encoded_data:
                        return {}
                    decoded_data = base64.b64decode(encoded_data).decode('utf-8')
                    return json.loads(decoded_data)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"❌ Errore durante il caricamento della cache: {e}. Inizio con una cache vuota.")
        return {}

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        """✅ Sessione persistente con cookie jar automatico"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"🔗 Utilizzo del proxy {proxy} per la sessione DLHD.")
                connector = ProxyConnector.from_url(proxy, ssl=False)
            else:
                connector = TCPConnector(
                    limit=10,
                    limit_per_host=3,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True,
                    force_close=False,
                    use_dns_cache=True
                )
                logger.info("ℹ️ Nessun proxy specifico per DLHD, uso connessione diretta.")
            # ✅ FONDAMENTALE: Cookie jar per mantenere sessione come browser reale
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    def _save_cache(self):
        """Salva lo stato corrente della cache su un file, codificando il contenuto in Base64."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json_data = json.dumps(self._stream_data_cache)
                encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
                f.write(encoded_data)
                logger.info(f"💾 Cache codificata e salvata con successo nel file: {self.cache_file}")
        except IOError as e:
            logger.error(f"❌ Errore durante il salvataggio della cache: {e}")

    def _get_headers_for_url(self, url: str, base_headers: dict) -> dict:
        """Applica headers specifici per newkso.ru automaticamente"""
        headers = base_headers.copy()
        parsed_url = urlparse(url)
        
        if "newkso.ru" in parsed_url.netloc:
            if self._iframe_context:
                iframe_origin = f"https://{urlparse(self._iframe_context).netloc}"
                newkso_headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                    'Referer': self._iframe_context,
                    'Origin': iframe_origin
                }
                logger.info(f"Applied newkso.ru headers with iframe context for: {url}")
            else:
                newkso_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
                newkso_headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                    'Referer': newkso_origin,
                    'Origin': newkso_origin
                }
            headers.update(newkso_headers)
        
        return headers

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione manuale del corpo della risposta (zstd, gzip, etc.)."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd':
                logger.info(f"Rilevata compressione zstd per {response.url}. Decompressione in corso...")
                try:
                    dctx = zstandard.ZstdDecompressor()
                    # ✅ MODIFICA: Utilizza stream_reader per gestire frame senza dimensione del contenuto.
                    # Questo risolve l'errore "could not determine content size in frame header".
                    with dctx.stream_reader(raw_body) as reader:
                        decompressed_body = reader.read()
                    return decompressed_body.decode(response.charset or 'utf-8')
                except zstandard.ZstdError as e:
                    logger.error(f"Errore di decompressione Zstd: {e}. Il contenuto potrebbe essere incompleto o corrotto.")
                    raise ExtractorError(f"Fallimento decompressione zstd: {e}")
            elif content_encoding == 'gzip':
                logger.info(f"Rilevata compressione gzip per {response.url}. Decompressione in corso...")
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                logger.info(f"Rilevata compressione deflate per {response.url}. Decompressione in corso...")
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                # Nessuna compressione o compressione non gestita, prova a decodificare direttamente
                return raw_body.decode(response.charset or 'utf-8')
        except Exception as e:
            logger.error(f"Errore durante la decompressione/decodifica del contenuto da {response.url}: {e}")
            raise ExtractorError(f"Fallimento decompressione per {response.url}: {e}")

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """✅ Richieste con sessione persistente per evitare anti-bot"""
        final_headers = self._get_headers_for_url(url, headers or {})
        # Aggiungiamo zstd agli header accettati per segnalare al server che lo supportiamo
        final_headers['Accept-Encoding'] = 'gzip, deflate, br, zstd'
        
        for attempt in range(retries):
            try:
                # ✅ IMPORTANTE: Riusa sempre la stessa sessione
                session = await self._get_session()
                
                logger.info(f"Tentativo {attempt + 1}/{retries} per URL: {url}")
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    class MockResponse:
                        def __init__(self, text_content, status, headers_dict):
                            self._text = text_content
                            self.status = status
                            self.headers = headers_dict
                            self.url = url
                        
                        async def text(self):
                            return self._text
                            
                        def raise_for_status(self):
                            if self.status >= 400:
                                raise aiohttp.ClientResponseError(
                                    request_info=None, 
                                    history=None,
                                    status=self.status
                                )
                        
                        async def json(self):
                            return json.loads(self._text)
                    
                    logger.info(f"✅ Richiesta riuscita per {url} al tentativo {attempt + 1}")
                    return MockResponse(content, response.status, response.headers)
                    
            except (
                aiohttp.ClientConnectionError, 
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientPayloadError,
                asyncio.TimeoutError,
                OSError,
                ConnectionResetError,
            ) as e:
                logger.warning(f"⚠️ Errore connessione tentativo {attempt + 1} per {url}: {str(e)}")
                
                # ✅ Solo in caso di errore critico, chiudi la sessione
                if attempt == retries - 1:
                    if self.session and not self.session.closed:
                        try:
                            await self.session.close()
                        except:
                            pass
                    self.session = None
                
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.info(f"⏳ Aspetto {delay} secondi prima del prossimo tentativo...")
                    await asyncio.sleep(delay)
                else:
                    raise ExtractorError(f"Tutti i {retries} tentativi falliti per {url}: {str(e)}")
                    
            except Exception as e:
                # Controlla se l'errore è dovuto a zstd e logga un messaggio specifico
                if 'zstd' in str(e).lower():
                    logger.critical(f"❌ Errore critico con la decompressione zstd. Assicurati che la libreria 'zstandard' sia installata (`pip install zstandard`). Errore: {e}")
                else:
                    logger.error(f"❌ Errore non di rete tentativo {attempt + 1} per {url}: {str(e)}")
                if attempt == retries - 1:
                    raise ExtractorError(f"Errore finale per {url}: {str(e)}")
        await asyncio.sleep(initial_delay)

    async def extract(self, url: str, force_refresh: bool = False, **kwargs) -> Dict[str, Any]:
        """Flusso di estrazione principale: risolve il dominio base, trova i player, estrae l'iframe, i parametri di autenticazione e l'URL m3u8 finale."""
        async def resolve_base_url(preferred_host: Optional[str] = None) -> str:
            """Risolve l'URL di base attivo provando una lista di domini noti."""
            if self._cached_base_url and not force_refresh:
                return self._cached_base_url
            
            DOMAINS = ['https://daddylive.sx/', 'https://dlhd.dad/']
            for base in DOMAINS:
                try:
                    resp = await self._make_robust_request(base, retries=1)
                    final_url = str(resp.url)
                    if not final_url.endswith('/'): final_url += '/' # Assicura lo slash finale
                    self._cached_base_url = final_url
                    logger.info(f"✅ Dominio base risolto: {final_url}")
                    return final_url
                except Exception as e:
                    logger.warning(f"⚠️ Tentativo fallito per il dominio base {base}: {e}")
            
            fallback = DOMAINS[0]
            logger.warning(f"Tutti i tentativi di risoluzione del dominio sono falliti, uso il fallback: {fallback}")
            self._cached_base_url = fallback
            return fallback

        def extract_channel_id(u: str) -> Optional[str]:
            patterns = [
                r'/premium(\d+)/mono\.m3u8$',
                r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
                r'watch\.php\?id=(\d+)',
                r'(?:%2F|/)stream-(\d+)\.php',
                r'stream-(\d+)\.php'
            ]
            for pattern in patterns:
                match = re.search(pattern, u, re.IGNORECASE)
                if match:
                    return match.group(1)
            return None

        async def get_stream_data(baseurl: str, initial_url: str, channel_id: str):
            def _extract_auth_params_dynamic(js: str) -> Dict[str, Any]:
                """
                Estrae dinamicamente i parametri di autenticazione da JavaScript offuscato.
                Cerca una stringa Base64 che contiene un oggetto JSON con i parametri.
                """
                # Pattern per trovare una variabile che contiene una lunga stringa Base64
                pattern = r'(?:const|var|let)\s+[A-Z0-9_]+\s*=\s*["\']([a-zA-Z0-9+/=]{50,})["\']'
                matches = re.finditer(pattern, js)
                
                for match in matches:
                    b64_data = match.group(1)
                    try:
                        json_data = base64.b64decode(b64_data).decode('utf-8')
                        obj_data = json.loads(json_data)

                        # Mappa nomi di chiavi alternativi a quelli standard
                        key_mappings = {
                            'auth_host': ['host', 'b_host', 'server', 'domain'],
                            'auth_php': ['script', 'b_script', 'php', 'path'],
                            'auth_ts': ['ts', 'b_ts', 'timestamp', 'time'],
                            'auth_rnd': ['rnd', 'b_rnd', 'random', 'nonce'],
                            'auth_sig': ['sig', 'b_sig', 'signature', 'sign']
                        }
                        
                        result = {}
                        is_complete = True
                        for target_key, possible_names in key_mappings.items():
                            found_key = False
                            for name in possible_names:
                                if name in obj_data:
                                    try:
                                        # Prova a decodificare se è a sua volta in base64
                                        decoded_value = base64.b64decode(obj_data[name]).decode('utf-8')
                                        result[target_key] = decoded_value
                                    except Exception:
                                        # Altrimenti usa il valore così com'è
                                        result[target_key] = obj_data[name]
                                    found_key = True
                                    break
                            if not found_key:
                                is_complete = False
                                break
                        
                        if is_complete:
                            logger.info(f"✅ Parametri di autenticazione trovati dinamicamente con chiavi: {list(obj_data.keys())}")
                            return result
                            
                    except Exception:
                        continue
                
                logger.warning("Nessun parametro di autenticazione valido trovato con la ricerca dinamica.")
                return {}

            daddy_origin = urlparse(baseurl).scheme + "://" + urlparse(baseurl).netloc
            daddylive_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                'Referer': baseurl,
                'Origin': daddy_origin
            }

            # 1. Richiesta pagina iniziale per trovare i link dei player
            resp1 = await self._make_robust_request(initial_url, headers=daddylive_headers)
            content1 = await resp1.text()
            player_links = re.findall(r'<button[^>]*data-url="([^"]+)"[^>]*>Player\s*\d+</button>', content1)
            if not player_links:
                raise ExtractorError("Nessun link player trovato nella pagina.")

            last_player_error = None
            iframe_url = None
            for player_url in player_links:
                try:
                    if not player_url.startswith('http'):
                        player_url = urljoin(baseurl, player_url)

                    daddylive_headers['Referer'] = player_url
                    resp2 = await self._make_robust_request(player_url, headers=daddylive_headers)
                    content2 = await resp2.text()
                    iframes2 = re.findall(r'iframe src="([^"]*)', content2)
                    if iframes2:
                        iframe_url = iframes2[0]
                        if not iframe_url.startswith('http'):
                            iframe_url = urljoin(player_url, iframe_url)
                        break
                except Exception as e:
                    last_player_error = e
                    logger.warning(f"Fallito il processamento del link player {player_url}: {e}")
                    continue

            if not iframe_url:
                if last_player_error:
                    raise ExtractorError(f"Tutti i link dei player sono falliti. Ultimo errore: {last_player_error}")
                raise ExtractorError("Nessun iframe valido trovato in nessuna pagina player")

            # Salva il contesto dell'iframe per gli header di newkso.ru
            self._iframe_context = iframe_url
            resp3 = await self._make_robust_request(iframe_url, headers=daddylive_headers)
            iframe_content = await resp3.text()

            try:
                # Estrai channel key
                channel_key = None
                channel_key_patterns = [
                    r'const\s+CHANNEL_KEY\s*=\s*["\']([^"\']+)["\']',
                    r'channelKey\s*=\s*["\']([^"\']+)["\']',
                    r'(?:let|const)\s+channelKey\s*=\s*["\']([^"\']+)["\']',
                    r'var\s+channelKey\s*=\s*["\']([^"\']+)["\']',
                    r'channel_id\s*:\s*["\']([^"\']+)["\']' # Aggiunto per nuovi formati
                ]
                for pattern in channel_key_patterns:
                    match = re.search(pattern, iframe_content)
                    if match:
                        channel_key = match.group(1)
                        break

                # Estrai parametri di autenticazione con la nuova funzione dinamica
                params = _extract_auth_params_dynamic(iframe_content)
                auth_host = params.get("auth_host")
                auth_php = params.get("auth_php")
                auth_ts = params.get("auth_ts")
                auth_rnd = params.get("auth_rnd")
                auth_sig = params.get("auth_sig")

                # Verifica che tutti i parametri siano presenti
                missing_params = []
                if not channel_key:
                    missing_params.append('channel_key')
                if not auth_ts:
                    missing_params.append('auth_ts (timestamp)')
                if not auth_rnd:
                    missing_params.append('auth_rnd (random)')
                if not auth_sig:
                    missing_params.append('auth_sig (signature)')
                if not auth_host:
                    missing_params.append('auth_host (host)')
                if not auth_php:
                    missing_params.append('auth_php (script)')

                if missing_params:
                    raise ExtractorError(f"Parametri mancanti: {', '.join(missing_params)}")

                # Procedi con l'autenticazione
                auth_sig_quoted = quote_plus(auth_sig)
                if auth_php:
                    normalized_auth_php = auth_php.strip().lstrip('/')
                    if normalized_auth_php == 'a.php':
                        auth_php = 'auth.php' # urljoin gestirà lo slash
                
                # Costruisci l'URL di autenticazione
                base_auth_url = urljoin(auth_host, auth_php)
                auth_url = f'{base_auth_url}?channel_id={channel_key}&ts={auth_ts}&rnd={auth_rnd}&sig={auth_sig_quoted}'
                
                # Fase 4: Auth request con header del contesto iframe
                iframe_origin = f"https://{urlparse(iframe_url).netloc}"
                auth_headers = daddylive_headers.copy()
                auth_headers['Referer'] = iframe_url
                auth_headers['Origin'] = iframe_origin
                try:
                    await self._make_robust_request(auth_url, headers=auth_headers, retries=1)
                except Exception as auth_error:
                    error_msg = str(auth_error).lower()
                    # ✅ GESTIONE SPECIFICA ERRORE 418: Timestamp scaduto
                    if "418" in error_msg or "i'm a teapot" in error_msg:
                        logger.warning(f"⚠️ Errore 418 rilevato (timestamp scaduto) per canale {channel_id}. Forza refresh completo...")
                        # Invalida sempre la cache per errore 418 e ritenta con dati freschi
                        if channel_id in self._stream_data_cache:
                            del self._stream_data_cache[channel_id]
                            self._save_cache()
                            logger.info(f"🗑️ Cache invalidata per errore 418 su canale {channel_id}")
                        # Forza un nuovo tentativo con estrazione completa (non dalla cache)
                        logger.info(f"🔄 Retry automatico per canale {channel_id} dopo errore 418")
                        return await get_stream_data(baseurl, initial_url, channel_id)
                    else:
                        # Per altri errori, comportamento esistente
                        logger.warning(f"Richiesta di autenticazione fallita: {auth_error}.")
                        if channel_id in self._stream_data_cache:
                            del self._stream_data_cache[channel_id]
                            logger.info(f"Cache per il canale {channel_id} invalidata; nuovo tentativo in corso.")
                            return await get_stream_data(baseurl, initial_url, channel_id)
                        raise ExtractorError(f"Autenticazione fallita: {auth_error}")
                
                # Fase 5: Server lookup
                server_lookup_path = None # Riscritto per essere più robusto
                # Cerca dinamicamente il path per il server lookup
                lookup_match = re.search(r"fetchWithRetry\(['\"](/server_lookup\.(?:js|php)\?channel_id=)['\"]", iframe_content)
                if lookup_match:
                    server_lookup_path = lookup_match.group(1)
                else:
                    # Fallback a un pattern più generico se il primo fallisce
                    lookup_match_generic = re.search(r"['\"](/server_lookup\.(?:js|php)\?channel_id=)['\"]", iframe_content)
                    if lookup_match_generic:
                        server_lookup_path = lookup_match_generic.group(1)

                if not server_lookup_path:
                    logger.error(f"❌ Impossibile estrarre l'URL per il server lookup. Contenuto iframe: {iframe_content[:1000]}")
                    raise ExtractorError("Impossibile estrarre l'URL per il server lookup")
                
                server_lookup_url = f"https://{urlparse(iframe_url).netloc}{server_lookup_path}{channel_key}"
                try:
                    lookup_resp = await self._make_robust_request(server_lookup_url, headers=daddylive_headers)
                    server_data = await lookup_resp.json()
                    server_key = server_data.get('server_key')
                    if not server_key:
                        logger.error(f"Nessun server_key nella risposta: {server_data}")
                        raise ExtractorError("Fallito l'ottenimento del server key dalla risposta di lookup")
                except Exception as lookup_error:
                    logger.error(f"Richiesta di server lookup fallita: {lookup_error}")
                    raise ExtractorError(f"Server lookup fallito: {str(lookup_error)}")

                logger.info(f"Server key ottenuto: {server_key}")
                
                referer_raw = f'https://{urlparse(iframe_url).netloc}'
                
                # Costruisci URL finale del stream
                if server_key == 'top1/cdn':
                    clean_m3u8_url = f'https://top1.newkso.ru/top1/cdn/{channel_key}/mono.m3u8' # Dominio noto e funzionante
                elif '/' in server_key:
                    parts = server_key.split('/')
                    domain = parts[0]
                    clean_m3u8_url = f'https://{domain}.newkso.ru/{server_key}/{channel_key}/mono.m3u8'
                else:
                    # ✅ CORREZIONE: Usa un dominio di fallback più affidabile se la costruzione dinamica fallisce.
                    # 'top1' è più recente e stabile di 'top2'.
                    clean_m3u8_url = f'https://{server_key}new.newkso.ru/{server_key}/{channel_key}/mono.m3u8'.replace('top2new', 'top1new')
                
                # Configura headers finali
                if "newkso.ru" in clean_m3u8_url:
                    stream_headers = {
                        'User-Agent': daddylive_headers['User-Agent'],
                        'Referer': iframe_url,
                        'Origin': referer_raw
                    }
                else:
                    stream_headers = {
                        'User-Agent': daddylive_headers['User-Agent'],
                        'Referer': referer_raw,
                        'Origin': referer_raw
                    }
                
                logger.info(f"🔧 Headers finali per stream: {stream_headers}")
                logger.info(f"✅ Stream URL finale: {clean_m3u8_url}")
                
                result_data = {
                    "destination_url": clean_m3u8_url,
                    "request_headers": stream_headers,
                    "mediaflow_endpoint": self.mediaflow_endpoint,
                }
                # Salva in cache
                self._stream_data_cache[channel_id] = result_data
                self._save_cache()
                logger.info(f"💾 Dati per il canale ID {channel_id} salvati in cache.")
                return result_data
                
            except Exception as param_error:
                logger.error(f"Errore nell'estrazione parametri: {str(param_error)}")
                raise ExtractorError(f"Fallimento estrazione parametri: {str(param_error)}")

        try:
            channel_id = extract_channel_id(url)
            if not channel_id:
                raise ExtractorError(f"Impossibile estrarre channel ID da {url}")

            # Controlla la cache prima di procedere
            if not force_refresh and channel_id in self._stream_data_cache:
                logger.info(f"✅ Trovati dati in cache per il canale ID: {channel_id}. Verifico la validità...")
                cached_data = self._stream_data_cache[channel_id]
                stream_url = cached_data.get("destination_url")
                stream_headers = cached_data.get("request_headers", {})

                is_valid = False
                if stream_url:
                    try:
                        # Usa una sessione separata per la validazione per non interferire
                        # con la sessione principale e i suoi cookie.
                        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as validation_session:
                            async with validation_session.head(stream_url, headers=stream_headers, ssl=False) as response:
                                # Uso una richiesta HEAD per efficienza, con un timeout breve
                                if response.status == 200:
                                    is_valid = True
                                    logger.info(f"✅ Cache per il canale ID {channel_id} è valida.")
                                else:
                                    logger.warning(f"⚠️ Cache per il canale ID {channel_id} non valida. Status: {response.status}. Procedo con estrazione.")
                    except Exception as e:
                        logger.warning(f"⚠️ Errore durante la validazione della cache per {channel_id}: {e}. Procedo con estrazione.")
                
                if not is_valid:
                    # Rimuovi i dati invalidi dalla cache
                    if channel_id in self._stream_data_cache:
                        del self._stream_data_cache[channel_id]
                    self._save_cache()
                    logger.info(f"🗑️ Cache invalidata per il canale ID {channel_id}.")
                else:
                    # ✅ NUOVO: Esegui una richiesta di "keep-alive" per mantenere la sessione attiva
                    # Questo utilizza il proxy se configurato, come richiesto.
                    try:
                        logger.info(f"🔄 Eseguo una richiesta di keep-alive per il canale {channel_id} per mantenere la sessione attiva tramite proxy.")
                        baseurl = await resolve_base_url()
                        # Eseguiamo una richiesta leggera alla pagina del canale per aggiornare i cookie di sessione.
                        # Questo assicura che il proxy venga utilizzato.
                        await self._make_robust_request(url, retries=1)
                        logger.info(f"✅ Sessione per il canale {channel_id} rinfrescata con successo.")
                    except Exception as e:
                        logger.warning(f"⚠️ Fallita la richiesta di keep-alive per il canale {channel_id}: {e}. Lo stream potrebbe non funzionare.")
                    
                    return cached_data

            # ✅ NUOVO: Usa un lock per prevenire estrazioni simultanee per lo stesso canale
            if channel_id not in self._extraction_locks:
                self._extraction_locks[channel_id] = asyncio.Lock()
            
            lock = self._extraction_locks[channel_id]
            async with lock:
                # Ricontrolla la cache dopo aver acquisito il lock, un altro processo potrebbe averla già popolata
                if channel_id in self._stream_data_cache:
                    logger.info(f"✅ Dati per il canale {channel_id} trovati in cache dopo aver atteso il lock.")
                    return self._stream_data_cache[channel_id]

                # Procedi con l'estrazione
                logger.info(f"⚙️ Nessuna cache valida per {channel_id}, avvio estrazione completa...")
                baseurl = await resolve_base_url()
                return await get_stream_data(baseurl, url, channel_id)
            
        except Exception as e:
            logger.exception(f"Estrazione DLHD completamente fallita per URL {url}")
            raise ExtractorError(f"Estrazione DLHD completamente fallita: {str(e)}")

    async def invalidate_cache_for_url(self, url: str):
        """
        Invalida la cache per un URL specifico.
        Questa funzione viene chiamata da app.py quando rileva un errore (es. fallimento chiave AES).
        """
        def extract_channel_id_internal(u: str) -> Optional[str]:
            patterns = [
                r'/premium(\d+)/mono\.m3u8$',
                r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
                r'watch\.php\?id=(\d+)',
                r'(?:%2F|/)stream-(\d+)\.php',
                r'stream-(\d+)\.php'
            ]
            for pattern in patterns:
                match = re.search(pattern, u, re.IGNORECASE)
                if match: return match.group(1)
            return None

        channel_id = extract_channel_id_internal(url)
        if channel_id and channel_id in self._stream_data_cache:
            del self._stream_data_cache[channel_id]
            self._save_cache()
            logger.info(f"🗑️ Cache per il canale ID {channel_id} invalidata a causa di un errore esterno (es. chiave AES).")

    async def close(self):
        """Chiude definitivamente la sessione"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except:
                pass
        self.session = None
