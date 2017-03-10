import os
import time
from flask import Blueprint, jsonify, request
from config import get_configuration, get_redis_connection


from CreateNewVolumeReqBody import CreateNewVolumeReqBody
from CreateNewVolumeRespBody import CreateNewVolumeRespBody
from VolumesVolumeidResizePostReqBody import VolumesVolumeidResizePostReqBody

volumes_api = Blueprint('volumes_api', __name__)


def get_volume_config_filename(volume_id):
    config = get_configuration()
    return os.path.join(config.metadatadir, '{}.volume'.format(volume_id))


@volumes_api.route('/volumes', methods=['POST'])
def CreateNewVolume():
    '''
    Create a new volume, can be a copy from an existing volume
    It is handler for POST /volumes
    '''

    inputs = CreateNewVolumeReqBody.from_json(request.get_json())
    if not inputs.validate():
        return jsonify(errors=inputs.errors), 400

    # Generate new id
    volume_id = hex(int(time.time() * 10000000))

    # Clone the template volume
    if inputs.templatevolume:
        with open(os.path.join(os.path.dirname(__file__), 'copyvolume.lua'), 'r') as f:
            lua_script = f.read()
        with get_redis_connection() as c:
            clone = c.register_script(lua_script)
            clone(keys=[inputs.volume_id, volume_id], args=[inputs.volume_id, volume_id])

    # Write file
    with open(get_volume_config_filename(volume_id), 'w') as f:
        f.write(jsonify(inputs))

    response = CreateNewVolumeRespBody()
    response.volumeid = volume_id
    return jsonify(response)


@volumes_api.route('/volumes/<volumeid>', methods=['GET'])
def GetVolumeInfo(volumeid):
    '''
    Get volume information
    It is handler for GET /volumes/<volumeid>
    '''

    return jsonify()


@volumes_api.route('/volumes/<volumeid>', methods=['DELETE'])
def DeleteVolume(volumeid):
    '''
    Delete Volume
    It is handler for DELETE /volumes/<volumeid>
    '''

    return jsonify()


@volumes_api.route('/volumes/<volumeid>/resize', methods=['POST'])
def ResizeVolume(volumeid):
    '''
    Resize Volume
    It is handler for POST /volumes/<volumeid>/resize
    '''

    inputs = VolumesVolumeidResizePostReqBody.from_json(request.get_json())
    if not inputs.validate():
        return jsonify(errors=inputs.errors), 400

    return jsonify()
