chargebacks_ids = '''SELECT Id, Simplex_ID__c   
                     FROM Chargeback__c
                     WHERE CreatedDate = LAST_N_DAYS:60'''

chargebacks_log = """
                    SELECT
                        Id, 
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
                        FROM Chargeback__c
                    WHERE LastModifiedDate >= """
