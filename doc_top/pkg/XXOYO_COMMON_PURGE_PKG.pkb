create or replace package body xxoyo_common_purge_pkg
as

/*****************************************************************************************************
Component Type: Package Body
Component Name: XXOYO_COMMON_PURGE_PKG
Description: This package is used to delete the stuck/errored rcv shipment records and inserting in a log table
Version Matrix
  Version     Author                  Date                Description
  1.0         Rajinder Nagpal         18th Dec 20202      SEEK# , purge erred/stuck rcv shipment header records
******************************************************************************************************/

    g_debug_level  number := nvl(to_number(fnd_profile.value('XXOYO_DEBUG_LEVEL')), 3);
    g_debug_mode varchar2(10) := 'FILE';
    g_debug_log varchar2(10) := 'LOG';
    g_debug_output varchar2(10) := 'OUTPUT';
    g_object varchar2(50) := 'XXOYO_COMMON_PURGE_PKG: ' || fnd_global.conc_request_id;
    g_debug_value0  number := 0;
    g_debug_value1  number := 1;
    g_debug_value2  number := 2;
    g_debug_value3  number := 3;
    g_user_id  number := fnd_global.user_id;
    g_request_id  number := fnd_global.conc_request_id;
    g_sql_errm varchar2(300);

/*****************************************************************************************************
Component Type: Procedure
Component Name: PURGE_RSH_STUCK_RECORDS
******************************************************************************************************/    

    procedure purge_rsh_stuck_records(p_err_buff varchar2
                                      ,p_ret_code varchar2
                                      ,p_orphan_days number)
    is

        v_records_inserted number := 0;

        v_records_deleted number := 0;

    begin

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Start of purge_rsh_stuck_records: ', g_object);
        
        insert into xxoyo_rcv_del_ship_log_tbl
        select shipment_header_id               
              ,last_update_date 
              ,last_updated_by 
              ,creation_date 
              ,created_by 
              ,last_update_login 
              ,receipt_source_code 
              ,vendor_id 
              ,vendor_site_id 
              ,organization_id 
              ,shipment_num 
              ,receipt_num 
              ,ship_to_location_id 
              ,bill_of_lading 
              ,packing_slip 
              ,shipped_date 
              ,freight_carrier_code 
              ,expected_receipt_date 
              ,employee_id 
              ,ship_to_org_id
              ,asn_type
              ,request_id 
              ,sysdate creation_date 
              ,g_user_id created_by 
              ,sysdate last_update_date 
              ,g_user_id last_updated_by 
              ,userenv('SESSIONID') last_update_login  
              ,g_request_id request_id
          from rcv_shipment_headers rsh
         where 
         (
              not exists
                  (select 1
                    from rcv_shipment_lines rsl 
                    where rsl.shipment_header_id = rsh.shipment_header_id
                  )
              and not exists
                  (select 1
                    from rcv_transactions rt
                   where rt.shipment_header_id = rsh.shipment_header_id 
                  )
              and not exists
                  (select 1
                    from rcv_transactions_interface rti
                   where rti.shipment_header_id = rsh.shipment_header_id
                     and rti.processing_status_code in ('RUNNING','PENDING')
                  )
              and exists
                  (select 1
                    from rcv_transactions_interface rti
                   where rti.shipment_header_id = rsh.shipment_header_id
                     and (rti.processing_status_code = 'ERROR'
                          or
                          rti.transaction_status_code = 'ERROR'
                          ) 
                  )
          )
          or
          (
              not exists
                  (select 1
                    from rcv_shipment_lines rsl 
                    where rsl.shipment_header_id = rsh.shipment_header_id
                  )
              and not exists
                  (select 1
                    from rcv_transactions rt
                   where rt.shipment_header_id = rsh.shipment_header_id 
                  )
              and not exists
                  (select 1
                    from rcv_transactions_interface rti
                   where rti.shipment_header_id = rsh.shipment_header_id
                  )
              and creation_date < sysdate - p_orphan_days    
          )   
              ;

        v_records_inserted := sql%rowcount;      

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Records inserted in xxoyo_rcv_del_ship_log_tbl: ' ||';' || v_records_inserted, g_object);              

        delete from rcv_shipment_headers rsh
         where 1 = 1
          and exists
              (select 1
                from xxoyo_rcv_del_ship_log_tbl
               where shipment_header_id = rsh.shipment_header_id
                 and request_id = g_request_id
              );

        v_records_deleted := sql%rowcount;                                           

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Records deleted from rcv_shipment_headers: ' ||';' || v_records_deleted, g_object);

        if v_records_deleted <> v_records_inserted then
            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'Reconciliation failed for headers records deleted vs inserted', g_object);        
            raise_application_error(-20902,
                              'UReconciliation failed for headers records deleted vs inserted', true);            
        end if;        

        insert into xxoyo_rcv_del_rti_log_tbl
        select 
                interface_transaction_id  
                ,group_id           
                ,last_update_date     
                ,last_updated_by    
                ,creation_date      
                ,created_by       
                ,last_update_login    
                ,request_id       
                ,transaction_type       
                ,transaction_date       
                ,processing_status_code   
                ,transaction_status_code  
                ,quantity           
                ,unit_of_measure      
                ,interface_source_code    
                ,interface_source_line_id 
                ,inv_transaction_id     
                ,category_id        
                ,item_id          
                ,item_revision        
                ,employee_id        
                ,auto_transact_code     
                ,shipment_header_id     
                ,shipment_line_id       
                ,ship_to_location_id    
                ,primary_quantity       
                ,receipt_source_code    
                ,vendor_id          
                ,vendor_site_id       
                ,from_organization_id     
                ,from_subinventory      
                ,to_organization_id     
                ,intransit_owning_org_id  
                ,source_document_code     
                ,parent_transaction_id    
                ,po_header_id         
                ,po_revision_num      
                ,po_release_id        
                ,po_line_id         
                ,po_line_location_id    
                ,shipment_num         
                ,shipped_date         
                ,expected_receipt_date    
                ,destination_context    
                ,org_id           
                ,sysdate creation_date            
                ,g_user_id created_by               
                ,sysdate last_update_date         
                ,g_user_id last_updated_by          
                ,userenv('SESSIONID') last_update_login        
                ,g_request_id request_id               
        from rcv_transactions_interface rti
       where 1 = 1
         and exists
            (select 1
              from xxoyo_rcv_del_ship_log_tbl
             where shipment_header_id = rti.shipment_header_id
               and request_id = g_request_id 
            );

        v_records_inserted := sql%rowcount;      

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Records inserted in xxoyo_rcv_del_rti_log_tbl: ' ||';' || v_records_inserted, g_object);             

        delete from rcv_transactions_interface rti
         where 1 = 1
          and exists
              (select 1
                from xxoyo_rcv_del_rti_log_tbl
               where shipment_header_id = rti.shipment_header_id
                 and request_id = g_request_id
              );   

        v_records_deleted := sql%rowcount;                                           

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Records deleted from rcv_transactions_interface: ' ||';' || v_records_deleted, g_object);

        if v_records_deleted <> v_records_inserted then
            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'Reconciliation failed for transactions records deleted vs inserted', g_object);        
            raise_application_error(-20902,
                              'UReconciliation failed for transactions records deleted vs inserted', true);            
        end if;                                                        

    exception
        when others then
            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'Unexpected error in purge_rsh_stuck_records : ' ||';' || g_sql_errm, g_object);
            raise_application_error(-20901,
                              'Unexpected error in purge_rsh_stuck_records : ' ||';' ||g_sql_errm, true);                                        
    end purge_rsh_stuck_records;

end xxoyo_common_purge_pkg;
/
show error;
