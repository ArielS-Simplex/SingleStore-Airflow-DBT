CHARGEBACKS_LOG_CREATE = """
        create table if not exists SALESFORCE.CHARGEBACKS_LOG (
                    Id VARCHAR,
                    Simplex_ID__c VARCHAR,
                    Name VARCHAR,
                    Status__c VARCHAR,
                    Chargeback_Posting_Date__c TIMESTAMP,
                    Final_Decision_Date__c TIMESTAMP,
                    KYC__c VARCHAR,
                    Blockchain__c VARCHAR,
                    Usable_Ticket__c VARCHAR,
                    Comments__c VARCHAR,
                    Follow_Up_Date__c TIMESTAMP,
                    Fight_Until__c TIMESTAMP,
                    Stage__c VARCHAR(255),
                    Decision__c VARCHAR(255),
                    Special_Attention__c VARCHAR(255),
                    Pre_Arbitration_Due_Date__c TIMESTAMP,
                    Documents_Submitted__c VARCHAR,
                    Fight_At__c TIMESTAMP,
                    Finance_Status__c VARCHAR(255),
                    Zoho_Status__c VARCHAR(255),
                    Merchant__c VARCHAR(255),
                    Evidence_Type__c VARCHAR(255),
                    Summary_Date__c TIMESTAMP,
                    Identity_Match__c VARCHAR(255),
                    Email_Match__c VARCHAR,
                    Phone_Match__c VARCHAR,
                    IP_Match__c VARCHAR,
                    CC_To_Account_Match__c VARCHAR,
                    Undisputed_TRX__c VARCHAR,
                    AB_Fight_Until_Date__c TIMESTAMP,
                    Public_Match__c VARCHAR,
                    AVS_Match__c VARCHAR,
                    Undisputed_ARN__c VARCHAR(255),
                    Undisputed_Long_ID__c VARCHAR(255),
                    LastModifiedDate NUMBER,
                    DW_CREATED_AT TIMESTAMP default current_timestamp(),
                    DW_UPDATED_AT TIMESTAMP default current_timestamp()
)
        """

CHARGEBACKS_LOG_TEMP_CREATE = """
        create temporary table SALESFORCE.CHARGEBACKS_LOG_TEMP (
                    Id VARCHAR,
                    Simplex_ID__c VARCHAR,
                    Name VARCHAR,
                    Status__c VARCHAR,
                    Chargeback_Posting_Date__c TIMESTAMP,
                    Final_Decision_Date__c TIMESTAMP,
                    KYC__c VARCHAR,
                    Blockchain__c VARCHAR,
                    Usable_Ticket__c VARCHAR,
                    Comments__c VARCHAR,
                    Follow_Up_Date__c TIMESTAMP,
                    Fight_Until__c TIMESTAMP,
                    Stage__c VARCHAR(255),
                    Decision__c VARCHAR(255),
                    Special_Attention__c VARCHAR(255),
                    Pre_Arbitration_Due_Date__c TIMESTAMP,
                    Documents_Submitted__c VARCHAR,
                    Fight_At__c TIMESTAMP,
                    Finance_Status__c VARCHAR(255),
                    Zoho_Status__c VARCHAR(255),
                    Merchant__c VARCHAR(255),
                    Evidence_Type__c VARCHAR(255),
                    Summary_Date__c TIMESTAMP,
                    Identity_Match__c VARCHAR(255),
                    Email_Match__c VARCHAR,
                    Phone_Match__c VARCHAR,
                    IP_Match__c VARCHAR,
                    CC_To_Account_Match__c VARCHAR,
                    Undisputed_TRX__c VARCHAR,
                    AB_Fight_Until_Date__c TIMESTAMP,
                    Public_Match__c VARCHAR,
                    AVS_Match__c VARCHAR,
                    Undisputed_ARN__c VARCHAR(255),
                    Undisputed_Long_ID__c VARCHAR(255),
                    LastModifiedDate NUMBER
)
        """

CHARGEBACKS_LOG_MERGE = """
        MERGE INTO SALESFORCE.CHARGEBACKS_LOG lg USING SALESFORCE.CHARGEBACKS_LOG_TEMP lg_tmp
            ON lg.ID = lg_tmp.ID
            WHEN MATCHED THEN
                UPDATE SET
                            Name = lg_tmp.Name,
                            Status__c = lg_tmp.Status__c,
                            Chargeback_Posting_Date__c = lg_tmp.Chargeback_Posting_Date__c,
                            Final_Decision_Date__c = lg_tmp.Final_Decision_Date__c,
                            KYC__c = lg_tmp.KYC__c,
                            Blockchain__c = lg_tmp.Blockchain__c,
                            Usable_Ticket__c = lg_tmp.Usable_Ticket__c,
                            Comments__c = lg_tmp.Comments__c,
                            Follow_Up_Date__c = lg_tmp.Follow_Up_Date__c,
                            Fight_Until__c = lg_tmp.Fight_Until__c,
                            Stage__c = lg_tmp.Stage__c,
                            Decision__c = lg_tmp.Decision__c,
                            Special_Attention__c = lg_tmp.Special_Attention__c,
                            Pre_Arbitration_Due_Date__c = lg_tmp.Pre_Arbitration_Due_Date__c,
                            Documents_Submitted__c = lg_tmp.Documents_Submitted__c,
                            Fight_At__c = lg_tmp.Fight_At__c,
                            Finance_Status__c = lg_tmp.Finance_Status__c,
                            Zoho_Status__c = lg_tmp.Zoho_Status__c,
                            Merchant__c = lg_tmp.Merchant__c,
                            Evidence_Type__c = lg_tmp.Evidence_Type__c,
                            Summary_Date__c = lg_tmp.Summary_Date__c,
                            Identity_Match__c = lg_tmp.Identity_Match__c,
                            Email_Match__c = lg_tmp.Email_Match__c,
                            Phone_Match__c = lg_tmp.Phone_Match__c,
                            IP_Match__c = lg_tmp.IP_Match__c,
                            CC_To_Account_Match__c = lg_tmp.CC_To_Account_Match__c,
                            Undisputed_TRX__c = lg_tmp.Undisputed_TRX__c,
                            AB_Fight_Until_Date__c = lg_tmp.AB_Fight_Until_Date__c,
                            Public_Match__c = lg_tmp.Public_Match__c,
                            AVS_Match__c = lg_tmp.AVS_Match__c,
                            Undisputed_ARN__c = lg_tmp.Undisputed_ARN__c,
                            Undisputed_Long_ID__c = lg_tmp.Undisputed_Long_ID__c,
                            LastModifiedDate = lg_tmp.LastModifiedDate,
                            DW_UPDATED_AT = current_timestamp()  
            WHEN NOT MATCHED THEN
                INSERT  (Id,
                        Simplex_ID__c,
                        Name,
                        Status__c,
                        Chargeback_Posting_Date__c,
                        Final_Decision_Date__c,
                        KYC__c,
                        Blockchain__c,
                        Usable_Ticket__c,
                        Comments__c,
                        Follow_Up_Date__c,
                        Fight_Until__c,
                        Stage__c,
                        Decision__c,
                        Special_Attention__c,
                        Pre_Arbitration_Due_Date__c,
                        Documents_Submitted__c,
                        Fight_At__c,
                        Finance_Status__c,
                        Zoho_Status__c,
                        Merchant__c,
                        Evidence_Type__c,
                        Summary_Date__c,
                        Identity_Match__c,
                        Email_Match__c,
                        Phone_Match__c,
                        IP_Match__c,
                        CC_To_Account_Match__c,
                        Undisputed_TRX__c,
                        AB_Fight_Until_Date__c,
                        Public_Match__c,
                        AVS_Match__c,
                        Undisputed_ARN__c,
                        Undisputed_Long_ID__c,
                        LastModifiedDate
                        ) VALUES
                        (
                        lg_tmp.Id,
                        lg_tmp.Simplex_ID__c,
                        lg_tmp.Name,
                        lg_tmp.Status__c,
                        lg_tmp.Chargeback_Posting_Date__c,
                        lg_tmp.Final_Decision_Date__c,
                        lg_tmp.KYC__c,
                        lg_tmp.Blockchain__c,
                        lg_tmp.Usable_Ticket__c,
                        lg_tmp.Comments__c,
                        lg_tmp.Follow_Up_Date__c,
                        lg_tmp.Fight_Until__c,
                        lg_tmp.Stage__c,
                        lg_tmp.Decision__c,
                        lg_tmp.Special_Attention__c,
                        lg_tmp.Pre_Arbitration_Due_Date__c,
                        lg_tmp.Documents_Submitted__c,
                        lg_tmp.Fight_At__c,
                        lg_tmp.Finance_Status__c,
                        lg_tmp.Zoho_Status__c,
                        lg_tmp.Merchant__c,
                        lg_tmp.Evidence_Type__c,
                        lg_tmp.Summary_Date__c,
                        lg_tmp.Identity_Match__c,
                        lg_tmp.Email_Match__c,
                        lg_tmp.Phone_Match__c,
                        lg_tmp.IP_Match__c,
                        lg_tmp.CC_To_Account_Match__c,
                        lg_tmp.Undisputed_TRX__c,
                        lg_tmp.AB_Fight_Until_Date__c,
                        lg_tmp.Public_Match__c,
                        lg_tmp.AVS_Match__c,
                        lg_tmp.Undisputed_ARN__c,
                        lg_tmp.Undisputed_Long_ID__c,
                        lg_tmp.LastModifiedDate
                        )
                    
"""

CHARGEBACKS_LOG_LAST_MODIFIED = """
        SELECT MAX(LastModifiedDate)
        FROM SALESFORCE.CHARGEBACKS_LOG
"""
CHECK_IF_CHARGEBACKS_LOG_EXISTS = """
    select to_boolean(count(1)) 
    from information_schema.tables 
    where table_schema = 'SALESFORCE' and table_name = 'CHARGEBACKS_LOG';
"""
