import os
from dotenv import load_dotenv


load_dotenv()


DEBUG_API_PORT = int(os.getenv('DEBUG_API_PORT', '8080'))

DB_DSN = os.path.expandvars(os.getenv('DB_DSN', 'postgres://$DB_HOST:$DB_PORT/$DB_NAME'))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

GRAPH_CACHE_PATH = os.getenv('GRAPH_CACHE_PATH')
