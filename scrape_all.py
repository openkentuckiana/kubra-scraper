import pathlib
import importlib
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from base_scraper import Scraper, DeltaScraper


def discover_scrapers(token):
    scrapers = []
    for filepath in pathlib.Path(".").glob("*.py"):
        mod = importlib.import_module(filepath.stem)
        # if there's a load_scrapers() function, call that
        if hasattr(mod, "load_scrapers"):
            scrapers.extend(mod.load_scrapers(token))
        # Otherwise instantiate a scraper for each class
        else:
            for klass in mod.__dict__.values():
                try:
                    if (
                        issubclass(klass, DeltaScraper)
                        and klass.__module__ != "kubra_scraper"
                        and klass.__module__ != "base_scraper"
                        and klass.__module__ != "lgeku_scraper"
                    ):
                        scrapers.append(klass(token))
                except TypeError:
                    pass
    return scrapers


if __name__ == "__main__":
    github_token = os.getenv("GITHUB_TOKEN")
    for scraper in discover_scrapers(github_token):
        if github_token is None:
            scraper.test_mode = True
        scraper.scrape_and_store()
