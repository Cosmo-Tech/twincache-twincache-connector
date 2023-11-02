# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import os
import sys
import redis

from twincache_connector import TwinCacheConnector

log_level_name = os.getenv("LOG_LEVEL") if "LOG_LEVEL" in os.environ else "INFO"
log_level = logging.getLevelName(log_level_name)
logging.basicConfig(stream=sys.stdout,
                    level=log_level,
                    format='%(levelname)s(%(name)s) - %(asctime)s - %(message)s',
                    datefmt='%d-%m-%y %H:%M:%S')
logger = logging.getLogger(__name__)


def check_env_var(required_env_var):
    if missing_env_vars := [r for r in required_env_var if r not in os.environ]:
        raise Exception(f"Missing environment variables named {missing_env_vars}")


if __name__ == '__main__':
    REQUIRED_VARS = ['TWIN_CACHE_HOST', 'TWIN_CACHE_PORT', 'TWIN_CACHE_PASSWORD', 'SOURCE', 'TARGET']
    check_env_var(REQUIRED_VARS)

    twin_cache_host = os.getenv("TWIN_CACHE_HOST")
    twin_cache_port = os.getenv("TWIN_CACHE_PORT")
    twin_cache_password = os.getenv("TWIN_CACHE_PASSWORD")

    source_redis_graph = os.getenv("SOURCE")
    target_redis_graph = os.getenv("TARGET")

    if sub_query := os.getenv('QUERIES'):
        logger.info(f"Running sub query: {sub_query}")
        sub_queries = sub_query.split(';')
        # run extract
        source_tc = TwinCacheConnector(host=twin_cache_host, port=twin_cache_port, password=twin_cache_password, name=source_redis_graph)
        source_tc.extract_to('extract_dir/', sub_queries)
        # import
        target_tc = TwinCacheConnector(host=twin_cache_host, port=twin_cache_port, password=twin_cache_password, name=target_redis_graph)
        target_tc.import_from('extract_dir')
    else:
        # simple copy
        r = redis.Redis(twin_cache_host, twin_cache_port, password=twin_cache_password)
        r.eval(
            """local o = redis.call('DUMP', KEYS[1]);\
               redis.call('RENAME', KEYS[1], KEYS[2]);\
               redis.call('RESTORE', KEYS[1], 0, o)""", 2, source_redis_graph, target_redis_graph)
