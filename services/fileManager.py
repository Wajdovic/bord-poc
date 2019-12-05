import os
import uuid

from azure.storage.blob import BlockBlobService
import pandas as pd
import json
from flask import current_app as app
import services.azureManager as adf
import numpy as np
import constants as const
import Levenshtein as lev
import copy


# def getFileFromBlob(filename):
#     blob_service = getBlobService()
#     blob_service.get_blob_to_path(app.config["CONTAINERNAME"], filename, './temp/' + filename)
#
#
# def getBlobService():
#     STORAGEACCOUNTNAME = app.config["STORAGEACCOUNTNAME"]
#     STORAGEACCOUNTKEY = app.config["STORAGEACCOUNTKEY"]
#     blob_service = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)
#     return blob_service


def getSheetNames(filename, uploadfolder):
    df = pd.read_excel(os.path.join(uploadfolder, filename), None)
    return list(df.keys())


def getDataFrameBySheet(FileName, Sheetname, uploadfolder, header_index=-1, data_end_index=None):
    df = None
    if header_index == -1:
        i = -1
        is_header = True
        while is_header and i <= 5:
            df = pd.read_excel(os.path.join(uploadfolder, FileName), sheet_name=Sheetname, skiprows=[i])
            if all(isinstance(item, str) for item in df.columns.values):
                is_header = False
            i += 1
        # df.columns = [col.strip().lower() for col in df.columns]
        if i == 6:
            df = pd.read_excel(os.path.join(uploadfolder), FileName, sheet_name=Sheetname,
                               skiprows=[header_index])
    else:
        df = pd.read_excel(os.path.join(uploadfolder), FileName, sheet_name=Sheetname,
                           skiprows=[header_index])

    if data_end_index is not None:
        df.drop(df.index[data_end_index])
    else:
        try:
            df = df[np.isfinite(df['treaty_number'])]
        except:
            pass
    seperator = '_'
    for duplicate_column in df.columns:
        try:
            new_name = duplicate_column.split(".")
            new_name = seperator.join(new_name)
            df = df.rename(
                columns={duplicate_column: new_name})
        except:
            pass
    return df


def convertToJSON(df):
    resultJSON = df.to_json(orient='records')
    return (resultJSON)


def readExcel(fileName, uploadfolder):
    res = {}
    sheets = getSheetNames(fileName, uploadfolder)
    for sheet in sheets:
        df = getDataFrameBySheet(fileName, sheet, uploadfolder)
        res[sheet] = {"header": list(df.columns), "data": json.loads(convertToJSON(df))}
    return res


def readExcelWithMetadata(fileName, metadata):
    res = {}
    sheets = getSheetNames(fileName)
    for sheet in sheets:
        df = getDataFrameBySheet(fileName, sheet, metadata[sheet]["header_index"], metadata[sheet]["data_end_index"])
        res[sheet] = {"header": list(df.columns), "data": json.loads(convertToJSON(df)), "metadata": metadata[sheet]}
    return res


def transformAndSaveAndExecute(filename, request):
    print(" ---- BEGIN TRANSFORMING " + filename)
    res = {}
    for sheet in request.keys():
        if sheet != "input":
            transformData(filename, request, res, sheet, app.config["UPLOAD_FOLDER"])
    print(" ---- END TRANSFORMING " + filename)
    originalFilename = filename
    filename, file_extension = os.path.splitext(filename)
    filename = filename + '.json'
    res["filename"] = filename
    res["currency"] = request["input"]["currency"]
    res["cedant_name"] = request["input"]["cedant_name"]
    res["writtenEarned"] = request["input"]["writtenEarned"]
    print(" ---- BEGIN SAVING " + filename + " TO [mapped_data]")
    # doc = json.dumps(res)
    # saveToBlob(filename, doc, app.config["MAPPEDDATA"])
    with open(os.path.join(app.config['MAPPED_FOLDER'], filename), 'w') as outfile:
        json.dump(res, outfile)
    print(" ---- END SAVING " + filename + " TO [mapped_data]")
    print(" ---- BEGIN EXECUTING ADF PIPELINE")
    run_exec = executePipeLine(filename)
    # deleteFileByPath(app.config["UPLOAD_FOLDER"] + '/' + originalFilename)
    print(" ---- Pipeline Run ID: " + str(run_exec.run_id), " Filename: " + filename)
    print(" ---- END EXECUTING ADF PIPELINE")
    return {"status": "Launched", "runid": str(run_exec.run_id)}


def deleteFileByPath(filePath):
    if os.path.exists(filePath):
        os.remove(filePath)


def transformData(filename, request, res, sheet, uploadfolder):
    df = getDataFrameBySheet(filename, sheet, uploadfolder)
    mapping = request[sheet]["mapping"]
    df = generateMappedColumn(df, mapping)
    res[request[sheet]["type"]] = json.loads(convertToJSON(df))


def generateMappedColumn(df, mapping):
    for key in mapping.keys():
        df[key] = df[mapping[key]]
    new_headers = list(mapping.keys())
    df = df[new_headers]
    return df


# def saveToBlob(filename, data, folder=""):
#     blob_service = getBlobService()
#     blob_service.create_blob_from_text(app.config["CONTAINERNAME"], folder + filename, data)
#
#
# def saveRawFile(filename, folder=""):
#     blob_service = getBlobService()
#     blob_service.create_blob_from_path(app.config["CONTAINERNAME"] + folder, filename, './temp/' + filename)


def executePipeLine(filname):
    params = {
        "fileName": filname
    }
    return adf.executePipline(params,app.config["P_NAME"])

def executePipeLine4(filenames):
    params = {
        "filenames": filenames
    }
    return adf.executePipline(params,app.config["P_NAME4"])


def automaticHeaderMapper(mappedField, headers):
    for target in mappedField:
        for col in headers:
            copy = col
            try:
                if str.strip(copy.lower()) == " ".join(target["name"].split("_")).lower():
                    target["value"] = col
                    break
                elif checkIfHeaderIsInPossibleMappedValues(copy, target):
                    target["value"] = col
                    break
                elif lev.ratio(str.strip(copy.lower()), " ".join(target["name"].split("_")).lower()) >= app.config[
                    "MINLEV"]:
                    target["value"] = col
                    break
            except:
                pass
    return mappedField


def getTargetFieldsByPoc(poc, filename, uploadfolder):
    print("GETTING TARGET FIELDS FOR POC: " + poc)
    dfjson = readExcel(filename, uploadfolder)
    res = {}
    if poc == app.config["POC1"]:
        for sheet in getSheetNames(filename, uploadfolder):
            targetFields = copy.deepcopy(const.targetFilds1)
            res[sheet] = automaticHeaderMapper(targetFields, dfjson[sheet]["header"])
        print("END GETTING TARGET FIELDS FOR POC: " + poc)
        return res
    elif poc == app.config["POC2"]:
        for sheet in getSheetNames(filename, uploadfolder):
            targetFields = const.targetFilds2
            res[sheet] = targetFields
        print("END GETTING TARGET FIELDS FOR POC: " + poc)
        return res
    else:
        print("ERROR GETTING TARGET FIELDS FOR POC: " + poc)
        os.abort(404)


def checkIfHeaderIsInPossibleMappedValues(col, target):
    pssibleValues = const.possibleMappingPoc1[target["name"]]
    return str.strip(col) in pssibleValues


def transformAndSaveAndExecute4(filename, request):
    print(" ---- BEGIN TRANSFORMING " + filename)
    res = {}
    dfinput = []
    for sheet in request.keys():
        if sheet != "input":
            transformData4(filename, request, res, sheet, app.config["UPLOAD_FOLDER_4"])
    print(" ---- END TRANSFORMING " + filename)
    print(" ---- BEGIN SAVING " + filename + " TO [mapped_data]")
    # doc = json.dumps(res)
    # saveToBlob(filename, doc, app.config["MAPPEDDATA"])
    for item in list(res.keys()):
        with open(os.path.join(app.config['MAPPED_FOLDER_4'], res[item]["sheetname"]), 'w') as outfile:
            json.dump(res[item], outfile)
        dfinput.append(str(res[item]["sheetname"]))
    print(" ---- END SAVING " + filename + " TO [mapped_data]")
    print(" ---- BEGIN EXECUTING ADF PIPELINE")
    # print(dfinput)
    # return "Done"
    run_exec = executePipeLine4(dfinput)
    # deleteFileByPath(app.config["UPLOAD_FOLDER"] + '/' + originalFilename)
    print(" ---- Pipeline Run ID: " + str(run_exec.run_id), " Filename: " + filename)
    print(" ---- END EXECUTING ADF PIPELINE")
    return {"status": "Launched", "runid": str(run_exec.run_id)}


def transformData4(filename, request, res, sheet, uploadfolder):
    doc = {}
    df = getDataFrameBySheet(filename, sheet, uploadfolder)
    mapping = request[sheet]["mapping"]
    df = generateMappedColumn(df, mapping)
    doc["Data"] = json.loads(convertToJSON(df))
    doc["UWY"] = request[sheet]["input"]["UWY"]
    doc["Scor Treaty"] = request[sheet]["input"]["Scor Treaty"]
    doc["As of Date"] = request[sheet]["input"]["As of Date"]
    import time;
    doc["Uploaded On"] = time.time()*1000
    doc["File Type"] = request[sheet]["File Type"]
    doc["Data Type"] = request[sheet]["Data Type"]
    doc["originalFilename"] = filename
    id = str(uuid.uuid4())
    name = sheet + '-' + id + '.json'
    doc["sheetname"] = name
    res[sheet] = doc
