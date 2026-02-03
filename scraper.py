import re
from urllib.parse import urlparse, urljoin, urldefrag
import configparser
import urllib.robotparser
# NOTE(E): duplicate urllib.parse?
import urllib.parse
import logging
# NOTE(E): install BeautifulSoup4
from bs4 import BeautifulSoup


CONFIG_PATH = "config.ini"
USER_AGENT = load_user_agent("Config.ini")


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

    # NOTE: already check is_valid() in scraper?
    if not is_valid(url):
        return []

    logger.info(f"START crawling URL: {url}")

    if resp is None:
        logger.error(f"DROP no response, url: {url}")

    if resp.status != 200:
        logger.warning(f"DROP status={resp.status} error={resp.error} url={url}")

    robots_url = get_robots_url(url)
    robot_parser = setup_robots(robots_url)
    agents = load_user_agents(CONFIG_PATH)

    if not robot_parser.can_fetch(USER_AGENT, url):
        logger.warning(f"DROP no permission for url={url}, robots_txt={robots_url}")
        return []

    # TODO(TAN): grab links in resp.raw_response.content
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        # TODO(TAN): save contents

        # extract links
        extracted_links = set() # no duplicates
        for tag in soup.find_all('a', href=True):
            link = urljoin(resp.url, tag['href'])
            clean_link = urldefrag(link)[0]

            # TODO(TAN): check for robots.txt Agents/Disallow here:
            if robot_parser.can_fetch(USER_AGENT, clean_link):
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
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def get_robots_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

def setup_robots(url):
    robot_parser = urllib.robotparser.RobotFileParser()
    robot_parser.set_url(url)
    robot_parser.read()
    return robot_parser


#parse user agents from config.ini
def load_user_agents(config_path: str) -> set[str]:
    config = configparser.ConfigParser()
    config.read(config_path)
    return config.get("DEFAULT", "USERAGENT").strip()

