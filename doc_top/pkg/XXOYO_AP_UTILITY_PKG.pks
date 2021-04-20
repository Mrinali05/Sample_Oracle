create or replace package xxoyo_ap_utility_pkg

/*****************************************************************************************************
Component Type: Package Specification
Component Name: xxoyo_ap_utility_pkg
Description: This is the custom package used to 'OYO AP Supplier Master Report'..
Version Matrix
  Version     Author                Date              Ticket                Description
   1.0        Satish Dhuria       01st Mar '21        9397999715            This is the custom package 
                                                                            used to customer name,phone and email..
   2.0        Satish Dhuria       16th Mar '21        3112162673            This is the custom package used 
                                                                            to RCM CHECK condition add entity_code and 																	application_id ..
   3.0        Mrinali Verma       26th Mar '21        2313807357            As per BF-1259 contains common procedures and
   																			                                    functions for ap invoices
   4.0        Mrinali Verma         30th Mar'21        6695931245             Some new functions have been added for ap
                                                                              requestor change program 
******************************************************************************************************/
as

    function get_contact_details(p_vendor_id in number
                                 ,p_flag in varchar2)
    return varchar2;

    function get_rcm_check(p_invoice_id in number)

    return number;

/******************************************************************************************************
Component Type: Function
Component Name: po_after_inv_flag
Ticekt Number:  2313807357
******************************************************************************************************/
function po_after_inv_flag(p_invoice_id number
								          ,p_condition varchar2)
   
    return varchar2 ;


/******************************************************************************************************
Component Type: Procedure
Component Name: exception_details
Ticekt Number:  2313807357
******************************************************************************************************/
procedure exception_details(p_invoice_id number
							             ,p_type varchar2) ;


/******************************************************************************************************
Component Type: Function
Component Name: get_requestor_details
Ticekt Number:  2313807357
******************************************************************************************************/
function get_requestor_details(p_attribute9 varchar2
								              ,p_condition varchar2
								              ,p_invoice_id number)
   	return varchar2;


/******************************************************************************************************
Component Type: Function
Component Name: get_approver
Ticekt Number:  2313807357
******************************************************************************************************/
function get_approver(p_invoice_id number
					           ,p_sequence number)
	return varchar2;


/******************************************************************************************************
Component Type: Function
Component Name: get_doa_rule_id
Ticekt Number:  2313807357
******************************************************************************************************/
function get_doa_rule_id(p_document_id number
                           ,p_ou_name varchar2
                           ,p_dept varchar2
                           ,p_document_type varchar2)
return number;                       

/******************************************************************************************************
Component Type: Function
Component Name: get_initiator_details
Ticekt Number:  2313807357
******************************************************************************************************/
function get_initiator_details(p_invoice_id number
								              ,p_type varchar2)
    return varchar2;


/******************************************************************************************************
Component Type: Function
Component Name: substr_fun
Ticekt Number:  2313807357
******************************************************************************************************/
function substr_fun(p_item_key varchar2)
    return number;	


/******************************************************************************************************
Component Type: Function
Component Name: get_workflow_details
Ticekt Number:  2313807357
******************************************************************************************************/
function get_workflow_details(p_invoice_id number
							               ,p_condition varchar2)
    return varchar2 ;


/******************************************************************************************************
Component Type: Function
Component Name: get_pending_wth_user_details
Ticekt Number:  2313807357
******************************************************************************************************/
function get_pending_wth_user_details(p_invoice_id number
                                         ,p_condition varchar2)
    return varchar2;


/******************************************************************************************************
Component Type: Function
Component Name: get_error_flag
Ticekt Number:  2313807357
******************************************************************************************************/
function get_error_flag(p_item_key varchar2)
    return varchar2 ;


/******************************************************************************************************
Component Type: Function
Component Name: get_grn
Ticekt Number:  2313807357
******************************************************************************************************/
function get_grn(p_invoice_id number)
  return varchar2 ;


/******************************************************************************************************
Component Type: Function
Component Name: get_delegated_user
Ticekt Number:  2313807357
******************************************************************************************************/
function get_delegated_user(p_invoice_id number)
  return varchar2 ;
  
/******************************************************************************************************
Component Type: Function
Component Name: get_employee_number
Ticekt Number:  6695931245
******************************************************************************************************/
function get_employee_number(p_email varchar2)
  return varchar2;
end xxoyo_ap_utility_pkg;

/
show error ;