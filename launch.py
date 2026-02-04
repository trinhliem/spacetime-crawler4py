from configparser import ConfigParser # ConfigParser is a Python standard library module 
from argparse import ArgumentParser # Build-in library module

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    cparser = ConfigParser() # creates an INI parser object
    cparser.read(config_file) # read settings from config.ini into cparser
    config = Config(cparser) # build the crawler's Config object from the parsed data 
    config.cache_server = get_cache_server(config, restart) # determines which cache server to use for this run and stores into config.cache_server
    crawler = Crawler(config, restart) # crawler object is created, which immediately creates a frontier
    crawler.start() # create workers and start threads


if __name__ == "__main__":
    parser = ArgumentParser() # build the CLI
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args() # read what you typer in the terminal
    main(args.config_file, args.restart)
