#!/usr/bin/env python3
from flask import Flask, send_from_directory, send_file
import wtforms_json
from volumes import volumes_api


app = Flask(__name__)

app.config["WTF_CSRF_ENABLED"] = False
wtforms_json.init()

app.register_blueprint(volumes_api)


@app.route('/apidocs/<path:path>')
def send_js(path):
    return send_from_directory('apidocs', path)


@app.route('/', methods=['GET'])
def home():
    return send_file('index.html')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='GIG Blockstor Volume Controller')
    parser.add_argument('metadatadir', type=str,
                        help='Configuration directory')
    parser.add_argument('redisHost', type=str,
                        help='Redis host')
    parser.add_argument('redisPort', type=int,
                        help='Redis port')
    parser.add_argument('--redisDB', type=int, default=0, required=False,
                        help='Redis database index')
    parser.add_argument('--bindIp', type=str, default='127.0.0.1', required=False,
                        help='Redis database index')
    parser.add_argument('--bindPort', type=int, default=5000, required=False,
                        help='Redis database index')
    config = parser.parse_args()
    app.config['config'] = config
    app.run(debug=True, host=config.bindIp, port=config.bindPort)
