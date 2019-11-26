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


def executePipline(params):
    rg_name = app.config["RG_NAME"]
    df_name = app.config["DF_NAME"]
    p_name = app.config["P_NAME"]
    adf_client = getAdfClient()
    adf_client.pipelines.create_run(rg_name, df_name, p_name, params)