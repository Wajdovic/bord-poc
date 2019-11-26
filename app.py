import uuid
import services.fileManager as fm
import os
from flask import Flask, request, Response
from werkzeug.utils import secure_filename
from flask_cors import CORS
import constants as const
import services.azureManager as adf
import json
import services.utils as utils

ALLOWED_EXTENSIONS = set(['xlsx', 'xls'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
app.config.from_pyfile('config.py')
CORS(app)


@app.route("/detect", methods=['GET'])
def detectFileStruct():
    filename = request.args.get("filename")
    if filename is None:
        os.abort(404)
    return fm.readExcel(filename)


# @app.route("/refresh", methods=['POST'])
# def refresh():
#     filename = request.args.get("filename")
#     if filename is None:
#         os.abort(404)
#     if request.method != 'POST':
#         os.abort(404)
#     data = fm.readExcelWithMetadata(filename, request.get_json())
#     data["global_metadata"] = {"filename": filename}
#     return "Done"


@app.route("/<poc>/status", methods=['GET'])
def getStatus(poc):
    if poc == app.config["POC1"]:
        id = request.args.get("id")
        pipeline_execution = vars(adf.getPiplineExecutionDetails(id))
        return Response(json.dumps(pipeline_execution,default=utils.serializer, indent=2), mimetype='application/json')
    else:
        os.abort(404)


@app.route("/<poc>/transform", methods=['POST'])
def transformFile(poc):
    filename = request.args.get("filename")
    if filename is None:
        os.abort(404)
    if request.method != 'POST':
        os.abort(404)
    # data = fm.readExcel(filename)
    # data["global_metadata"] = {"filename": filename}
    if poc == app.config["POC1"]:
        return fm.transformAndSaveAndExecute(filename, request.get_json())
    else:
        os.abort(404)


@app.route('/<poc>/upload', methods=['POST'])
def upload_file(poc):
    filename = request.args.get("fileName")
    if filename is None:
        os.abort(404)
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            return {"message": 'No file part'}, 400
        file = request.files['file']
        if file.filename == '':
            return {"message": "No file selected for uploading"}, 400
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filename, file_extension = os.path.splitext(filename)
            id = str(uuid.uuid4())
            filename = filename + '-' + id + file_extension
            if not os.path.isdir(app.config['UPLOAD_FOLDER']):
                os.mkdir(app.config['UPLOAD_FOLDER'])
            print(" ---- UPLOADING " + filename + " To TEMP FOLDER")
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            data = fm.readExcel(filename)
            data["metadata"] = {"filename": filename}
            print(" ---- SAVING RAW FILE  " + filename + " IN BLOB STORAGE [raw_data]")
            fm.saveRawFile(filename, app.config["RAWDATA"])
            return data, 200
        else:
            return {"message": 'Allowed file types are xlsx,xls'}, 400


# @app.route("/<poc>/targetfields", methods=['GET'])
# def getTarget(poc):
#     print("GETTING TARGET FIELDS FOR POC: "+poc)
#     if poc == "poc1":
#         return Response(json.dumps(const.targetFilds1), mimetype='application/json')
#     elif poc == "poc2":
#         return Response(json.dumps(const.targetFilds2), mimetype='application/json')
#     else:
#         os.abort(400)
#     print("END GETTING TARGET FIELDS FOR POC: "+poc)

@app.route("/<poc>/targetfields", methods=['GET'])
def getTarget(poc):
    filename = request.args.get("filename")
    if filename is None:
        os.abort(404)
    return fm.getTargetFieldsByPoc(poc,filename)


@app.route("/<poc>/sheettypes", methods=['GET'])
def getTargetSheets(poc):
    print(" ---- GETTING SHEET TYPES FOR POC: "+poc)
    if poc == "poc1":
        print(" ---- END GETTING SHEET TYPES FOR POC: " + poc)
        return Response(json.dumps(const.targetSheetsPoc1), mimetype='application/json')
    elif poc == "poc2":
        print(" ---- END GETTING SHEET TYPES FOR POC: " + poc)
        return Response(json.dumps(const.targetSheetsPoc2), mimetype='application/json')
    else:
        print(" ---- ERROR GETTING SHEET TYPES FOR POC: " + poc)
        os.abort(400)


if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', port=80)
