import os
import time
import json
from flask import Blueprint, jsonify, request
from config import get_configuration, get_redis_connection


from CreateNewVolumeReqBody import CreateNewVolumeReqBody
from CreateNewVolumeRespBody import CreateNewVolumeRespBody
from VolumeInformation import VolumeInformation
from ResizeVolumeReqBody import ResizeVolumeReqBody

volumes_api = Blueprint('volumes_api', __name__)


def get_volume_config_filename(volume_id):
    config = get_configuration()
    return os.path.join(config.metadatadir, '{}.volume'.format(volume_id))


@volumes_api.route('/api/0.1/volumes', methods=['POST'])
def CreateNewVolume():
    '''
    Create a new volume, can be a copy from an existing volume
    It is handler for POST /volumes

    {"blocksize": 4096, "deduped": false, "size": 1000000000, "storagecluster": "default"}
    '''

    inputs = CreateNewVolumeReqBody.from_json(request.get_json())
    if not inputs.validate():
        return jsonify(errors=inputs.errors), 400

    volume = inputs.data

    # Generate new id
    volume_id = hex(int(time.time() * 10000000))
    volume['id'] = volume_id
    templatevolume = volume['templatevolume']
    # Clone the template volume
    if templatevolume:
        with open(os.path.join(os.path.dirname(__file__), 'copyvolume.lua'), 'r') as f:
            lua_script = f.read()
        c = get_redis_connection()
        clone = c.register_script(lua_script)
        clone(keys=[templatevolume, volume_id], args=[templatevolume, volume_id])
    del volume['templatevolume']

    # Write file
    with open(get_volume_config_filename(volume_id), 'w') as f:
        json.dump(volume, f)

    response = CreateNewVolumeRespBody()
    response.volumeid = volume_id
    return jsonify(dict(volumeid=volume_id))


@volumes_api.route('/api/0.1/volumes/<volumeid>', methods=['GET'])
def GetVolumeInfo(volumeid):
    '''
    Get volume information
    It is handler for GET /volumes/<volumeid>
    '''

    with open(get_volume_config_filename(volumeid), 'r') as f:
        return jsonify(json.load(f))


@volumes_api.route('/api/0.1/volumes/<volumeid>', methods=['DELETE'])
def DeleteVolume(volumeid):
    '''
    Delete Volume
    It is handler for DELETE /volumes/<volumeid>
    '''

    return jsonify()


@volumes_api.route('/api/0.1/volumes/<volumeid>/resize', methods=['POST'])
def ResizeVolume(volumeid):
    '''
    Resize Volume
    It is handler for POST /volumes/<volumeid>/resize
    '''

    inputs = ResizeVolumeReqBody.from_json(request.get_json())
    if not inputs.validate():
        return jsonify(errors=inputs.errors), 400

    return jsonify()
