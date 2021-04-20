

Insert into FND_LOOKUP_VALUES 
(LOOKUP_TYPE
,LANGUAGE
,LOOKUP_CODE
,MEANING
,DESCRIPTION
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,CREATED_BY
,CREATION_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,SOURCE_LANG
,SECURITY_GROUP_ID
,VIEW_APPLICATION_ID
,TERRITORY_CODE
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,TAG
,LEAF_NODE
,ZD_EDITION_NAME
,ZD_SYNC) 
values 
('XXOYO_EINV_ERROR_MESSAGES'
,'US'
,'PARTIAL'
,'Though enabled in OYO but E-Invoicing not enabled in Oracle'
,null
,'Y'
,sysdate
,null
,fnd_global.user_id
,sysdate
,fnd_global.user_id
,userenv('SESSIONID')
,sysdate
,'US'
,0
,3
,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'SET2','INSERTED');

/




update xxoyo_einv_eligible_le_tbl set irn_eligible_flag='P'
where legal_entity ='MyPreferred Transformation And Hospitality Private Limited';

/

update ra_customer_trx_all 
set attribute10='Though enabled in OYO but E-Invoicing not enabled in Oracle' where 
org_id = 113
and creation_date > '01-OCT-2020' ;
/



