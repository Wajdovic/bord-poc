targetSheetsPoc1 = [{"label":"Loss","value":"Loss"},{"label":"Premium","value":"Premium"},{"label":"Reserve","value":"Reserve"}]
targetSheetsPoc2 = [{"label":"Loss Listing","value":"Loss Listing"},{"label":"Loss Bdx","value":"Loss Bdx"},
                    {"label":"Policy Listing","value":"Policy Listing"},{"label":"Premium Bdx","value":"Premium Bdx"}]
targetFilds1 = [#{"name": "Cedant_Name", "value": "", "mandatory": False},
               {"name": "Cedant_Legal_Entity", "value": "", "mandatory": False},
               {"name": "Cedant_Program", "value": "", "mandatory": False},
               {"name": "Policy_Number", "value": "", "mandatory": True},
               {"name": "Effective_Date", "value": "", "mandatory": True},
               {"name": "Expiration_Date", "value": "", "mandatory": True},
               {"name": "Policy_Limit", "value": "", "mandatory": False},
               {"name": "Policy_Attachment", "value": "", "mandatory": False},
               {"name": "Line_of_Business", "value": "", "mandatory": False},
               {"name": "Subline_of_Business", "value": "", "mandatory": False},
               {"name": "Insured", "value": "", "mandatory": False},
               {"name": "Location", "value": "", "mandatory": False},
               {"name": "Period_Begin_Date", "value": "", "mandatory": True},
               {"name": "Period_End_Date", "value": "", "mandatory": True},
               #{"name": "Currency", "value": "", "mandatory": False},
               #{"name": "Written/Earned", "value": "", "mandatory": False},
               {"name": "Gross_Premium_Change", "value": "", "mandatory": False},
               #{"name": "Gross_UPR_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Comm_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Premium_Change", "value": "", "mandatory": False},
               {"name": "Ceded_UPR_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Comm_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Brok_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Premium_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_UPR_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Comm_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Brok_Change", "value": "", "mandatory": False},
               {"name": "Claimant", "value": "", "mandatory": False},
               {"name": "Claim_Number", "value": "", "mandatory": False},
               {"name": "Date_of_Loss", "value": "", "mandatory": False},
               {"name": "Cause_of_Loss", "value": "", "mandatory": False},
               {"name": "Loss_Description", "value": "", "mandatory": False},
               {"name": "Cat_Code", "value": "", "mandatory": False},
               {"name": "Claim_Status", "value": "", "mandatory": False},
               #{"name": "Gross_Pd_Loss_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Pd_LAE_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Sal_Sub_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Pd_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "Gross_OS_Loss_Change", "value": "", "mandatory": False},
               #{"name": "Gross_OS_LAE_Change", "value": "", "mandatory": False},
               #{"name": "Gross_OS_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Inc_Loss_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Inc_LAE_Change", "value": "", "mandatory": False},
               #{"name": "Gross_Inc_L&LAE_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Pd_Loss_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Pd_LAE_Change", "value": "", "mandatory": False},
               {"name": "Ceded_Sal_Sub_Change", "value": "", "mandatory": False},
               #{"name": "Ceded_Pd_L&LAE_Change", "value": "", "mandatory": False},
               {"name": "Ceded_OS_Loss_Change", "value": "", "mandatory": False},
               {"name": "Ceded_OS_LAE_Change", "value": "", "mandatory": False},
               #{"name": "Ceded_OS_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "Ceded_Inc_Loss_Change", "value": "", "mandatory": False},
               #{"name": "Ceded_Inc_LAE_Change", "value": "", "mandatory": False},
               #{"name": "Ceded_Inc_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Pd_Loss_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Pd_LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Sal_Sub_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Pd_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_OS_Loss_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_OS_LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_OS_L&LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Inc_Loss_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Inc_LAE_Change", "value": "", "mandatory": False},
               #{"name": "SCOR_Inc_L&LAE_Chang", "value": "", "mandatory": False}
               ]
targetFilds2 = [{"name":"Policy ID","value":"","mandatory":True},
{"name":"Policy Effective Date","value":"","mandatory":True},
{"name":"Policy Expiration Date","value":"","mandatory":True},
{"name":"Insured Name","value":"","mandatory":True},
{"name":"Coverage Code","value":"","mandatory":False},
{"name":"Coverage Name","value":"","mandatory":False},
{"name":"Subcoverage Code","value":"","mandatory":False},
{"name":"Subcoverage Name","value":"","mandatory":False},
{"name":"Scor Coverage","value":"","mandatory":False},
{"name":"100Pct Occurrence Limit Orig CCY","value":"","mandatory":False},
{"name":"100Pct Aggregate Limit  Orig CCY","value":"","mandatory":False},
{"name":"Attachment Orig CCY","value":"","mandatory":False},
{"name":"Deductible SIR Orig CCY","value":"","mandatory":False},
{"name":"Limit Currency","value":"","mandatory":False},
{"name":"Cedent Share","value":"","mandatory":False},
{"name":"Gross Written Premium Orig CCY","value":"","mandatory":False},
{"name":"Subject Premium  Orig CCY","value":"","mandatory":False},
{"name":"Gross Written Premium USD","value":"","mandatory":False},
{"name":"Subject Premium USD","value":"","mandatory":False},
{"name":"Premium Currency","value":"","mandatory":False},
{"name":"Prem FX Rate","value":"","mandatory":False},
{"name":"Fac Detail","value":"","mandatory":False},
{"name":"Producing Office","value":"","mandatory":False},
{"name":"Business Division","value":"","mandatory":False},
{"name":"Business Subdivision","value":"","mandatory":False},
{"name":"Occurrence Trigger","value":"","mandatory":False},
{"name":"TRIA Covered","value":"","mandatory":False},
{"name":"Country","value":"","mandatory":False},
{"name":"State","value":"","mandatory":False},
{"name":"New Or Renewal","value":"","mandatory":False},
{"name":"Stacking ID","value":"","mandatory":False},
{"name":"SIC Code","value":"","mandatory":False},
{"name":"ISO Code","value":"","mandatory":False},
{"name":"NAICS Code","value":"","mandatory":False},
{"name":"Industry Description","value":"","mandatory":False},
{"name":"Sub Industry Description","value":"","mandatory":False},
{"name":"Exposure Amount","value":"","mandatory":False},
{"name":"Exposure CCY","value":"","mandatory":False},
{"name":"Exposure Bas","value":"","mandatory":False}
]

possibleMappingPoc1 = {
                       "Cedant_Legal_Entity":["Policy_Company_1"],
                       "Cedant_Program":["Reinsrs"],
                       "Policy_Number":["Policy_Number"],
                       "Effective_Date":["EFF"],
                       "Expiration_Date":["EXP"],
                       "Line_of_Business":["Starr_Profit_Ce_1"],
                       "Subline_of_Business":["Starr_Profit_Ce_3"],
                       "Insured":["Insured_Name"],
                       "Period_Begin_Date":["BD"],
                       "Period_End_Date":["BD"],
                       "Gross_Premium_Change":["Subject Premium"],
                       "Ceded_Premium_Change":["Written Premium"],
                       "Ceded_Comm_Change":["Commission"],
                        "Ceded_UPR_Change":["Unearned PremiumChg_"],
                       "Date_of_Loss":["D_O_L"],
                        "Ceded_Pd_Loss_Change":["CurrentPer PaidLoss"],
                        "Ceded_Pd_LAE_Change":["CurrentPer_PaidExpense","CurrentPer_ PaidExpense"],
                        "Ceded_OS_Loss_Change":["Outstanding Loss"],
                         "Ceded_OS_LAE_Change":["Outstanding Expense"]
                       }
possibleMappingPoc2 = {
                       "Policy ID":["POLICY"],
                       "Insured Name": ["INSURED_NAME"],
                        "SIC Code":["CUR_SIC"],
                        "New Or Renewal":["POL_STATUS"],
                        "Coverage Name":["LOB"],
                         "Attachment Orig CCY":["ATTACH_FINAL"],
                         "100Pct Occurrence Limit Orig CCY":["LIMIT_FINAL"],
                          "Gross Written Premium Orig CCY":["wp"],
                          "Policy Effective Date":["Effective Date"],
                           "Policy Expiration Date":["Expiry Date"],
                           "Business Division":["Policy Listing"]
}