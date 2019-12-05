from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from flask import current_app as app


def getCredentials():
    return ServicePrincipalCredentials(client_id=app.config["CLIENT_ID"],
                                       secret=app.config["SECRET"],
                                       tenant=app.config["TENANT"])


def getAdfClient():
    subscription_id = app.config["SUBSCRIPTION_ID"]
    credentials = getCredentials()
    return DataFactoryManagementClient(credentials, subscription_id)


def executePipline(params,p_name):
    rg_name = app.config["RG_NAME"]
    df_name = app.config["DF_NAME"]
    adf_client = getAdfClient()
    return adf_client.pipelines.create_run(rg_name, df_name, p_name, params)

def getPiplineExecutionDetails(id):
    rg_name = app.config["RG_NAME"]
    df_name = app.config["DF_NAME"]
    adf_client = getAdfClient()
    return adf_client.pipeline_runs.get(rg_name,df_name,id)