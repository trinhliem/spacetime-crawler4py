import re
from urllib.parse import urlparse, urljoin, urldefrag, urlsplit, parse_qsl, urlencode, urlunsplit
import configparser
import logging
from bs4 import BeautifulSoup
import sys
import os
import hashlib
import csv

# #parse user agents from config.ini
# def load_user_agents(config_path: str):
#     config = configparser.ConfigParser()
#     config.read(config_path)
#     return config.get("IDENTIFICATION", "USERAGENT").strip()

# CONFIG_PATH = "config.ini"
# USER_AGENT = load_user_agents("config.ini")


MIN_WORDS = 100
REPORT_DIR = "report"
UNIQUE_PAGES = set()
LONGEST_PAGE_URL = ""
LONGEST_PAGE_WORDS = 0
WORD_FREQ: dict[str, int] = {}
STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any","are",
    "aren't","as","at","be","because","been","before","being","below","between","both",
    "but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't",
    "doing","don't","down","during","each","few","for","from","further","had","hadn't",
    "has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here",
    "here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll",
    "i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me",
    "more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only",
    "or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she",
    "she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's",
    "the","their","theirs","them","themselves","then","there","there's","these","they",
    "they'd","they'll","they're","they've","this","those","through","to","too","under",
    "until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't",
    "what","what's","when","when's","where","where's","which","while","who","who's","whom",
    "why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've",
    "your","yours","yourself","yourselves"
}

SUBDOMAIN_COUNTS = 0

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

    # Handle server status codes and redirects
    CACHE_SERVER_ERRORS = {600, 601, 602, 603, 604, 605, 606, 607, 608}

    # If the downloader gave no response object
    if resp is None:
        logger.error(f"DROP no response, url: {url}")
        return []
    
    # If it's a cache server error, log and drop completely since these are likely transient and not useful for extraction
    if resp.status in CACHE_SERVER_ERRORS:
        logger.info(f"DROPPED {url} due to cache server error={resp.status}")
        return None
    
    # If it's a redirect (301/302), log and return the redirect URL for crawling since these can lead to valid pages. The crawler will handle the redirect URL as a new crawl.
    if resp.status in {301, 302}:
        redirect_url = resp.headers.get("Location")
        if redirect_url:
            logger.info(f"REDIRECT {url} TO {redirect_url}")
            return [redirect_url] # return the redirect URL for crawler to handle as a new crawl
        else:
            logger.warning(f"DROPPED {url}: redirect without location header")
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

    if is_large_file(resp):
        logger.info(f"DROP large_file url={url}")
        return []
    
    # grab links in resp.raw_response.content
    try:
        logger.info(f"Begin analyzing content url={url}")
        soup = BeautifulSoup(content, 'html.parser')

        # Sources : https://stackoverflow.com/questions/30565404/remove-all-style-scripts-and-html-tags-from-an-html-page
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ").strip()

        if no_data_wrapper(resp, text):
            return []
        #
        if low_info_wrapper(text, url):
            return []

        # #save_page_content(resp.url, text) # save the text content
        update_word_frequencies(text)
        page_url = unique_url_key(resp.url)
        if is_valid(page_url):
            UNIQUE_PAGES.add(page_url)
        update_subdomain_counts(url, SUBDOMAIN_COUNTS)

        wc = count_words(text)
        if wc > LONGEST_PAGE_WORDS:
            LONGEST_PAGE_WORDS = wc
            LONGEST_PAGE_URL = page_url

        # extract links
        extracted_links = set()
        for tag in soup.find_all('a', href=True):
            link = urljoin(resp.url, tag['href']) 
            clean_link = urldefrag(link)[0]
            clean_link = similar_no_info(clean_link) 

            extracted_links.add(clean_link)

        write_unique_pages_report()
        write_longest_page_report()
        write_top_50_words("common_words.txt")
        write_subdomains_report(SUBDOMAIN_COUNTS, "subdomains.txt")
            
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
        
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
    
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


# --- Handling similar pages with no information --- 
def similar_no_info(url: str) -> str:
    """
    Source : Google AI Overview was used to understand how to utilize urlsplit(),
    parse_qsl

    Handle different URL string that points to the same page.
    This includes:
    1. Different fragments -> resolved using urldefrag()
    2. Relative cs Absolute path -> resolved using join()
    3. Query parameter order
    """
    url_components = urlsplit(url)
    scheme = url_components.scheme
    network_location = url_components.netloc.lower()
    path = url_components.path if url_components.path else "/" # if path is empty, path is a /
    query = url_components.query

    # 3. Query parameter order
    if query:
        logger.debug(f"raw query: {query}")
        params = parse_qsl(query, keep_blank_values=True) # returns a list of key, value pairs
        params.sort() # sort by key, then value -- ensures unique ordering for same query in different order
        query = urlencode(params)
        logger.debug(f"cleaned query: {query}")
    
    return urlunsplit((scheme, network_location, path, query, ""))


# --- Handling pages with thin content/junk --- 
def low_info_wrapper(text: str, url: str) -> bool:
    if not has_min_words(text):
        logger.info(f"LOWINFO reason=min_words, url={url}")
        return True

    if has_repeated_tokens(text):
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


def has_repeated_tokens(text: str) -> bool:
    """
    Handle pages that lacks diveristy in words, which may be junk
    """
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


def has_repeated_sentences(text: str) -> bool:
    """
    Handling pages with many repetitions, which may be junk
    """
    sentences = re.split(r"[.!?]\s+|\n+", text)  # basic heuristic to detect a sentence

    counts = {}
    total = 0
    for sentence in sentences:
        sentence = sentence.strip().lower()
        sentence = re.sub(r"\s+", " ", sentence)

        if len(sentence) < 30:
            continue

        total += 1 
        counts[sentence] = counts.get(sentence, 0) + 1
        if counts[sentence] >= 10:
            return True
        
        if total >= 300:
            break

    return False


# --- Handling 200, but no data ---
def no_data_wrapper(response, text: str) -> bool:
    '''
    Wrapper for 200, but no data.
    '''

    if not is_html_content_type(response):
        logger.info(f"DROP 200_no_data reason=non_html url={response.url}")
        return True

    return False


def is_html_content_type(response) -> bool:
    """ 
    Return true if the content is HTML (or text)
    i.e. If it is not text data, but instead is pdf or image, then don't crawl
    """
    try:
        headers = response.raw_response.headers 
        content_type = (headers.get("Content-Type") or "").lower()  
    except (AttributeError, TypeError):
        return True

    if content_type and ("text/html" not in content_type) and ("application/xhtml+xml" not in content_type): # content can be html even if the content_type is empty
        return False 

    return True


# --- Handling Large files, Low info --- 
def is_large_file(resp) -> bool:
    try:
        headers = resp.raw_response.headers
        content_length = headers.get("Content-Length")
        if content_length and content_length.isdigit():
            size_bytes = int(content_length)

            if size_bytes > 5_000_000: # 5MB
                return True
    except Exception:
        pass
    return False



# TODO Shizuka -- EC Simhas


# --- Report ---
def unique_url_key(u: str) -> str:
    return urldefrag(u)[0]


def write_unique_pages_report() -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    out_path = os.path.join(REPORT_DIR, "unique_pages.txt")
    with open(out_path, "w") as f:
        f.write(f"Unique pages: {len(UNIQUE_PAGES)}\n")
        f.write("\n")
        for u in sorted(UNIQUE_PAGES):
            f.write(u + "\n")


def count_words(text: str) -> int:
    num_words = 0
    for _ in tokenize_text(text):
        num_words += 1
    return num_words


def write_longest_page_report() -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    out_path = os.path.join(REPORT_DIR, "longest_page.txt")
    with open(out_path, "w") as f:
        f.write(f"Longest page (num of words): {LONGEST_PAGE_WORDS}\n")
        f.write(f"URL: {LONGEST_PAGE_URL}\n")


def load_stopwords_file(stopwords_path: str) -> set[str]:
    words = set()
    with open(stopwords_path, "r", encoding="utf-8") as f:
        for line in f:
            w = line.strip().lower()
            if w:
                words.add(w)
    return words


def update_word_frequencies(text: str) -> None:
    for token in tokenize_text(text):
        if token in STOPWORDS:
            continue
        WORD_FREQ[token] = WORD_FREQ.get(token, 0) + 1


def write_top_50_words(out_path: str) -> None:
    items = sorted(WORD_FREQ.items(), key=lambda kv: (-kv[1], kv[0]))[:50]
    with open(out_path, "w", encoding="utf-8") as f:
        for word, count in items:
            f.write(f"{word}, {count}\n")


def update_subdomain_counts(url: str, subdomain_counts: dict[str, int]) -> None:
    clean_url = urldefrag(url)[0]
    host = (urlparse(clean_url).hostname or "").lower()
    if not host:
        return
    subdomain_counts[host] = subdomain_counts.get(host, 0) + 1


def write_subdomains_report(subdomain_counts: dict[str, int], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        for host in sorted(subdomain_counts.keys()):
            f.write(f"{host}, {subdomain_counts[host]}\n")