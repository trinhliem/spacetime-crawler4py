import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list() # initialize the todo list of URLs
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url) # if restarting, add each seed to the frontier
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save: # If the save is empty, fall back to seeding
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save) # how many URLs hav been discovered 
        tbd_count = 0 # how many are still pending 
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url) # add to todolist
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop() # frontier uses stack; if empty, return None; Workers stop when None is returned
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url) # normalize so same page doesn't appear in multiple forms
        urlhash = get_urlhash(url) # compute hash key for storage
        if urlhash not in self.save:
            self.save[urlhash] = (url, False) # store hash key as not completed yet
            self.save.sync() # flush to disk immediately
            self.to_be_downloaded.append(url) # add to todo list for Workers
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True) # update status to completed
        self.save.sync() # flush to disk
