import re
from urllib.parse import urlparse, urljoin, urldefrag, urlsplit, parse_qsl, urlencode
import configparser
import logging
from bs4 import BeautifulSoup
import sys
import os
import hashlib
import csv

#parse user agents from config.ini
def load_user_agents(config_path: str):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config.get("IDENTIFICATION", "USERAGENT").strip()

CONFIG_PATH = "config.ini"
USER_AGENT = load_user_agents("config.ini")
MIN_WORDS = 100
SIMHASH_BITS = 64         
NEAR_DUP_TAU = 0.95     
SEEN_SIMHASHES = []  

# configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='crawler.log', encoding='utf-8', level=logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)




def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # 200: success; 204: no content; 301/302: redirect; 403: forbidden; 404: not found; 500: server error
    # resp.error: when status is not 200, you can check the error here, if needed.
    # If the status codes are between 600 and 606, the reason for the error is provided in resp.error
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    logger.info(f"START crawling URL: {url}")

    # If the downloader gave no response object
    if resp is None:
        logger.error(f"DROP no response, url: {url}")
        return []
    
    # If the server did not return 200 (OK), skip parsing links from it since it may be unreliable for extraction
    if resp.status != 200: #Comment (Quang): This one can miss the redirect links with code 301/302 which may lead to other valid pages. The code in other files already handle the redirect links correctly for us.
        logger.warning(f"DROP status={resp.status} error={resp.error} url={url}")
        return []
    
    # If raw response is None, cant access content attribute, so check this before
    if resp.raw_response is None:
        logger.error(f"DROP no raw_response, url: {url}")
        return []
    
    # If the response has no content, no links can be extracted 
    content = resp.raw_response.content
    if not content:
        logger.error(f"DROP no content, url: {url}")
        return []

    # grab links in resp.raw_response.content
    try:
        logger.info(f"Begin analyzing content url={url}")
        soup = BeautifulSoup(content, 'html.parser')

        # Detect and avoid pages with low information
        # Sources : https://stackoverflow.com/questions/30565404/remove-all-style-scripts-and-html-tags-from-an-html-page
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
        if has_low_info(text, resp.url):
            return []
        else:
            save_page_content(resp.url, text) # save the text content

        # extract links
        extracted_links = set()
        for tag in soup.find_all('a', href=True):
            link = urljoin(resp.url, tag['href']) # duplicate handling  : ensure absolute path
            clean_link = urldefrag(link)[0] # duplicate handling : avoid fragments
            clean_link = avoid_duplicate_urls(clean_link) # additional duplicate handling

            extracted_links.add(clean_link)
            
        return list(extracted_links)

    except Exception as e:
        logger.warning(f"Error parsing {url}: {e}")
        return []


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # host must be in allowed domain set
        host = parsed.hostname 
        if host is None:
            host = ""
        else:
            host = host.lower() # hostnames are case-insensitive
        if not (
            host.endswith(".ics.uci.edu")
            or host.endswith(".cs.uci.edu")
            or host.endswith(".informatics.uci.edu")
            or host.endswith(".stat.uci.edu")
        ):
            return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
    
        # --- TRAP DETECTIONS ---
        #Reference: https://developers.google.com/search/docs/crawling-indexing/url-structure, https://support.archive-it.org/hc/en-us/articles/208332943-How-to-identify-and-avoid-crawler-traps, https://en.wikipedia.org/wiki/Spider_trap
        #1. Avoid infinite calendar trap: for example, URLs with /calendar/2024/01/01, /calendar/2024/01/01, etc.
        calendar_pattern = re.compile(r"/calendar/\d{4}/\d{1,2}/\d{1,2}")
        if calendar_pattern.search(parsed.path.lower()):
            logger.info(f"DROPPED infinite calendar URL: {url}")
            return False
        
        #2. Avoid session IDs in query session=, sid=, jseesionid=
        if parsed.query:
            query_params = dict(parse_qsl(parsed.query))
            session_keys = {"session", "sid", "jsessionid"}
            if any(key.lower() in session_keys for key in query_params):
                logger.info(f"DROPPED session ID URL: {url}")
                return False
            
        #3. Avoid spider traps / manual patterns
        #Example: very long numeric path segments (common traps)
        segments = [seg for seg in parsed.path.split("/") if seg]
        if any(len(seg) > 50 for seg in segments):
            logger.info(f"DROPPED suspicious long segment URL: {url}")
            return False
        
        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise


def tokenize_text(text: str):
    # allow non-English characters for this assignment
    current = []
    for ch in text:
        if ch.isalnum():        
            current.append(ch.lower())
        else:
            if current:
                yield "".join(current)
                current.clear()

    if current:
        yield "".join(current)
        current.clear()

#Comment Quang: we may want to normalize the domain too, e.g., lowercase hostnames such as Example.COM and example.com are the same.
def avoid_duplicate_urls(url: str) -> str:
    """
    Source : Google AI Overview was used to understand how to utilize urlsplit(),
    parse_qsl

    Handle different URL string that points to the same page.
    This includes:
    1. Different fragments -> resolved using urldefrag()
    2. Relative cs Absolute path -> resolved using join()
    3. Query parameter order
    TODO continue thinking of other edge cases
    
    """
    url_components = urlsplit(url)
    scheme = url_components.scheme
    network_location = url_components.netloc
    path = url_components.path if url_components.path else "/" # if path is empty, path is a /
    query = url_components.query

    # 3. Query parameter order
    if query:
        logger.debug(f"raw query: {query}")
        params = parse_qsl(query, keep_blank_values=True) # returns a list of key, value pairs
        params.sort() # sort by key, then value -- ensures unique ordering for same query in different order
        query = urlencode(params)
        logger.debug(f"cleaned query: {query}")
    
    return urllib.parse.urlunsplit((scheme, network_location, path, query, ""))


def has_low_info(text: str, url: str) -> bool:
    if not has_min_words(text):
        logger.info(f"LOWINFO reason=min_words, url={url}")
        return True

    if has_few_unique_tokens(text):
        logger.info(f"LOWINFO reason=few_unique_tokens, url={url}")
        return True

    if has_repeated_sentences(text, min_len=30, repeat_threshold=10):
        logger.info(f"LOWINFO reason=repeated_sentences, url={url}")
        return True

    return False


def has_min_words(text: str) -> bool:
    count = 0
    for token in tokenize_text(text):
        count += 1
        if count >= MIN_WORDS:
            return True
    return False


def has_few_unique_tokens(text: str) -> bool:
    total = 0
    unique_tokens = set()

    for tok in tokenize_text(text):
        total += 1
        unique_tokens.add(tok)
        if total >= 500:  # Check the first 500 tokens
            break

    if total == 0:
        return True
    unique_ratio = len(unique_tokens) / total
    return unique_ratio < 0.05


def has_repeated_sentences(text: str, min_len: int = 30, repeat_threshold: int = 10) -> bool:
    sentences = re.split(r"[.!?]\s+|\n+", text)  # basic heuristic to detect a sentence

    counts = {}
    total = 0
    for sentence in sentences:
        sentence = sentence.strip().lower()
        sentence = re.sub(r"\s+", " ", sentence)

        if len(sentence) < min_len:
            continue

        total += 1 
        counts[sentence] = counts.get(sentence, 0) + 1
        if counts[sentence] >= repeat_threshold:
            return True
        
        if total >= 300:
            break

    return False



# TODO Shizuka -- EC Simhash