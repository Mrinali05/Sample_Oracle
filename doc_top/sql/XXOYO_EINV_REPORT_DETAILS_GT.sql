/*****************************************************************************************************
Component Type: Table, Synonym, Index and Grants
Component Name: XXOYO_EINV_REPORT_DETAILS_GT
Description: Global Temporary table to capture invoice line taxes level details extracted
Version Matrix
  Version     Author                Date               Description
   1.0        Mrinali Verma         25th Aug '20       As per FACNT-832-OracleERP - E-Invoicing
   1.1        Mrinali Verma         9th Oct '20        As per FACNT-832-OracleERP - E-Invoicing
                                                       added one more column for AR invoicing
 ******************************************************************************************************/

accept xxoyo_user char prompt 'Type xxoyo as username:  '
accept xxoyo_pwd char prompt 'Type xxoyo password: ' hide
accept apps_user char prompt 'Type apps as username:  '
accept apps_pwd char prompt 'Type apps password: ' hide
conn &xxoyo_user/&xxoyo_pwd



 create global temporary table xxoyo_einv_report_details_gt
 (
	  qr_code1					    	varchar2(4000)
	 ,qr_code2                          varchar2(4000)
	 ,irn						    	varchar2(70)
	 ,ack_number					    varchar2(50)
	 ,ack_date					    	varchar2(30)
	 ,document_num					    varchar2(50)
	 ,document_date					    varchar2(30)
	 ,shipment_id					    varchar2(150)
	 ,shipment_num					    varchar2(50)
	 ,invoice_type					    varchar2(20)
	 ,place_of_supply				    varchar2(10)
	 ,eway_bill_number				    varchar2(50)
	 ,supply_type					    varchar2(20)
	 ,bill_from					    	varchar2(1000)
	 ,supplier_gstin 				    varchar2(20)
	 ,supplier_state_code				varchar2(20)
	 ,bill_to					    	varchar2(1000)
	 ,recipient_gstin				    varchar2(20)
	 ,recipient_state_code_upd			varchar2(20)
	 ,ship_from					    	varchar2(1000)
	 ,dispatch_from_state_code			varchar2(20)
	 ,ship_to					    	varchar2(1000)
	 ,recipient_poc_name				varchar2(100)
	 ,recipient_phone_num				varchar2(20)
	 ,recipient_email				    varchar2(50)
	 ,ship_to_state_code				varchar2(20)
	 ,base_amount					    number
	 ,tax_amount					    number
	 ,total_amount					    number
	 ,item_code					    	varchar2(40)
	 ,item_name					    	varchar2(240)
	 ,hsn						    	varchar2(20)
	 ,quantity					    	number
	 ,rate						    	number
	 ,item_tax_amount				    number
	 ,amount 					    	number
	 ,tax_code					    	varchar2(240)
	 ,batch_id					    	number
     ,invoice_process                   varchar(2000)
	)
	;

 grant all on xxoyo.xxoyo_einv_report_details_gt to apps;

 conn &apps_user/&apps_pwd
 
 create or replace synonym apps.xxoyo_einv_report_details_gt for xxoyo.xxoyo_einv_report_details_gt;
