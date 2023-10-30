# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import os
import sys
import redis

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
    r = redis.Redis(twin_cache_host, twin_cache_port, password=twin_cache_password)

    source_redis_graph = os.getenv("SOURCE")
    target_redis_graph = os.getenv("TARGET")

    if sub_query := os.getenv('QUERIES'):
        pass
        # run extract
    else:
        # simple copy
        r.eval(
            """local o = redis.call('DUMP', KEYS[1]);\
               redis.call('RENAME', KEYS[1], KEYS[2]);\
               redis.call('RESTORE', KEYS[1], 0, o)""", 2, source_redis_graph, target_redis_graph)
