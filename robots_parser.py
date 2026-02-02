import configparser

#parse user agents from config.ini
def load_user_agents(config_path: str) -> set[str]:
    config = configparser.ConfigParser()
    config.read(config_path)

    raw_agents = config.get("robots", "user_agents", fallback="")

    agents = {
        line.strip().lower()
        for line in raw_agents.splitlines()
        if line.strip()
    }

    return agents

class robots_parser(Object):
    """
    text: robots.txt tokens
    """
    def __init__(self, text):
        pass

    def is_allowed(self) -> True:
        agents = load_user_agents()
        if '*' or agents not in restricted_bots:
            return True

    def parse_txt(self) -> dict:
        pass


