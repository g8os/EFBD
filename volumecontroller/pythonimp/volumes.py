from flask import Blueprint, jsonify, request


from VolumesPostReqBody import VolumesPostReqBody
from VolumesVolumeidResizePostReqBody import VolumesVolumeidResizePostReqBody

volumes_api = Blueprint('volumes_api', __name__)


@volumes_api.route('/volumes', methods=['POST'])
def CreateNewVolume():
    '''
    Create a new volume, can be a copy from an existing volume
    It is handler for POST /volumes
    '''
    
    inputs = VolumesPostReqBody.from_json(request.get_json())
    if not inputs.validate():
        return jsonify(errors=inputs.errors), 400
    
    return jsonify()


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
