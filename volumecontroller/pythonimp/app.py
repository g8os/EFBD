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
    parser.add_argument('metadatadir', metavar='N', type=str, nargs='+',
                        help='Configuration directory')
    parser.add_argument('redisHost', metavar='N', type=str, nargs='+',
                        help='Redis host')
    parser.add_argument('redisPort', metavar='N', type=int, nargs='+',
                        help='Redis port')
    parser.add_argument('redisDB', metavar='N', type=int, nargs='+', default=0,
                        help='Redis database index')
    config = parser.parse()
    app.config['config'] = config
    app.run(debug=True)
