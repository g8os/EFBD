from flask import current_app
import redis


def get_configuration():
    return current_app['config']


def get_redis_connection():
    config = get_configuration()
    return redis.Redis(config.redisHost, config.redisPort, config.redisDB)
