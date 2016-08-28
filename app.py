from flask import Flask
from flask import request
import json

import column_infos
import version_infos

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello world!"

@app.route("/get_column_infos", methods=['GET'])
def get_colmn_infos():
    if 'zk_hosts' in request.args and 'root_path' in request.args:
        infos = column_infos.get_column_infos(request.args.get('zk_hosts'), request.args.get('root_path')) 
        return json.dumps(infos)

    return "You must set zk_hosts and root_path"

@app.route("/get_version_infos", methods=['GET'])
def get_version_infos():
    if 'zk_hosts' in request.args and 'root_path' in request.args:
        infos = version_infos.get_version_infos(request.args.get('zk_hosts'), request.args.get('root_path')) 
        return json.dumps(infos)

    return "You must set zk_hosts and root_path"

if __name__ == "__main__":
    app.run(host = "0.0.0.0", port = 5000, debug = True)
