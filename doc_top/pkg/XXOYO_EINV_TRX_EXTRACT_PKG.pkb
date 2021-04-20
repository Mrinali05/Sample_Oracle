create or replace package body xxoyo_einv_trx_extract_pkg
  /************************************************************************************************************************f
  Component Type: Package Body
  Component Name: XXOYO_EINV_TRX_EXTRACT_PKG
  Description: Package for extracting the invoices data from standard table and inserting into the custom.
  Version Matrix
    Version     Author                Date               Description
     1.0        Arpit Kumar Singh     27 Aug '20      As per FACNT-832-OracleERP - E-Invoicing -
                                                         Write concurrent program
     2.0        Arpit Kumar Singh     01 Oct '20      As per FACNT-905-OracleERP - E-Invoicing -
                                                        Write Extraction program AR
     3.0        Arpit Kumar Singh     29 Oct '20      As per FACNT-1001-OracleERP - E-Invoicing -
                                                        Bug Fix
     4.0        Arpit Kumar Singh     20th Nov '20    As per FACNT-1043 OracleERP - E-Invoicing -
                                                        CN/DN
     5.0        Arpit Kumar Singh     25th Nov '20    As per FACNT-1038 OracleERP - B2C -
                                                        QR code generation
     6.0        Arpit Kumar Singh     4th Nov '20    As per FACNT-1108 OracleERP - E-Invoicing -
                                                      Bug Fix (Recipient Email)
     6.1        Arpit Kumar Singh     29th Dec '20    As per ticket id 2138373344, B2B records should be treated
                                                      as B2C if recipient GSTIN equal to supplier GSTIN and tax is 0
     6.2        Mrinali Verma         29th Dec '20    As per ticket id 6459938503, Material Transaction ID was not getting
                                                      populated for DTP-Intra, required for Material Issue Register
     6.3        Mrinali Verma         29th Dec '20    As per ticket id 6909745872, rolling back to version 6.1 because
                                                      changes done in 6.2 causing issue for MO transaction on the form.                                                 
     7.0        Mrinali Verma         2nd Feb '20     As per #ticket 2485937501 had to change the message for mth invoices
  ************************************************************************************************************************/
as
  g_debug_level number := nvl(to_number(fnd_profile.value('XXOYO_DEBUG_LEVEL')), 3);
  g_debug_mode varchar2(10) := 'FILE';
  g_debug_log varchar2(10) := 'LOG';
  g_debug_output varchar2(10) := 'OUTPUT';
  g_object varchar2(50) := 'XXOYO_EINV_TRX_EXTRACT_PKG: ' || fnd_global.conc_request_id;
  g_debug_value0 number := 0;
  g_debug_value1 number := 1;
  g_debug_value2 number := 2;
  g_debug_value3 number := 3;
  g_debug_value4 number := 4;
  g_user_id number := fnd_global.user_id;
  g_request_id number := fnd_global.conc_request_id;
  g_sql_errm varchar2(300);
  g_master_org_id number;

  g_batch_id number;
  g_generate_report varchar2(1) := 'N';

  g_recipient_email varchar2(50) := 'scm@oyorooms.com';
  g_errbuff varchar2(500);
  g_retcode varchar2(1);

  g_err_delim varchar2(5) := '####';
  g_delim varchar2(1) := '~';
  g_error_lookup_name varchar2(50) := 'XXOYO_EINV_ERROR_MESSAGES';
  g_reconcile_flag varchar2(1) := 'Y';
  g_process_name varchar2(100);
  g_mat_process varchar2(100) := xxoyo_einv_utility_pkg.g_mat_process;
  g_ar_process varchar2(100) := xxoyo_einv_utility_pkg.g_ar_process;
  --type einv_tab is table of xxoyo_einv_trx_extract_gt%rowtype index by binary_integer;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: update_standard_tbl
  As per FACNT-1038
  ******************************************************************************************************/

  procedure update_standard_tbl(p_status_msg varchar2
                                ,p_transaction_id number)
  is
  begin
    if g_process_name = g_mat_process then

          update xxoyo_po_ind_mo_tbl
             set attribute10 = p_status_msg
                ,last_update_date = sysdate
                ,last_update_by = g_user_id
           where 1 = 1
             and attribute13 = p_transaction_id
             ;

        elsif g_process_name = g_ar_process then

          update ra_customer_trx_all
             set attribute10 = p_status_msg
                ,last_update_date = sysdate
                ,last_updated_by = g_user_id
           where 1 = 1
             and attribute13 = p_transaction_id
             ;

     end if;

     xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value4, g_debug_level,
                          'Records updated in standard tbl for transaction id : ' ||p_transaction_id||' '||sql%rowcount
          , g_object);

     exception
      when others then
      g_sql_errm := substr(sqlerrm, 1, 240);

      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in update_standard_tbl : ' ||';' ||g_sql_errm
        , g_object);

      raise_application_error(-20905,
                              'Unexpected error in update_standard_tbl : ' ||';' ||g_sql_errm ,
                              true);

  end update_standard_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: gen_b2c_qr_code
  As per FACNT-1038
  ******************************************************************************************************/
  procedure gen_b2c_qr_code
  is
    v_qr_blob blob;
    v_transaction_id number;
    v_static_str varchar2(50) := 'data:image/png;base64,';
  begin
    for qr_data in (select xelvpt.vpa_details
                    ,xetht.supplier_legal_name
                    ,xetht.gross_amount
                    ,xetht.document_num
                    ,xetht.transaction_id
                    from xxoyo_einv_trx_hdr_tbl xetht
                    , xxoyo_einv_le_vpa_tbl xelvpt
                    where 1=1
                    and xetht.request_id = g_request_id
                    and xetht.irn_eligible_flag = 'N'
                    and xetht.status_code = 'PENDING'
                    and xetht.supply_type_code = 'B2C'
                    and xetht.supplier_legal_name = xelvpt.legal_entity_name
                    and xetht.org_id = xelvpt.org_id
                    and nvl(xelvpt.effective_end_date, sysdate + 1) > sysdate
                    )
    loop
      v_transaction_id := qr_data.transaction_id  ;
      v_qr_blob := xxoyo_einv_utility_pkg.xxoyo_bmp_to_png(
                        xxoyo_generate_qr_code_pkg.f_qr_as_bmp(
                          p_data => qr_data.vpa_details||chr(13)||
                          qr_data.supplier_legal_name||chr(13)||
                          qr_data.gross_amount||chr(13)||
                          qr_data.document_num||chr(13)||
                          'OYO_ORACLE-'||qr_data.transaction_id||chr(13),
                          p_error_correction => 'M')
                    );

        if ( v_qr_blob is not null) then

           update xxoyo_einv_trx_hdr_tbl
            set signed_qr_code = replace(replace((v_static_str||xxoyo_einv_utility_pkg.xxoyo_blob_to_base64_encode(v_qr_blob)),chr(13)),chr(10))
            ,status_code = 'SUCCESS'
            ,status_message = xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_SUCCESS')
            where transaction_id = qr_data.transaction_id;

            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value4, g_debug_level,
                          'Records successfully updated for transaction id : ' ||qr_data.transaction_id||' '||sql%rowcount
          , g_object);

            update_standard_tbl(xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_SUCCESS'),qr_data.transaction_id);
        else
            update xxoyo_einv_trx_hdr_tbl
            set status_code = 'ERROR'
            ,status_message = xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_ERROR')
            where transaction_id = qr_data.transaction_id;

            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value4, g_debug_level,
                          'Records updated with failure for transaction id : ' ||qr_data.transaction_id||' '||sql%rowcount
          , g_object);

            update_standard_tbl(xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_ERROR'),qr_data.transaction_id);
        end if;

    end loop;
    exception
      when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      update xxoyo_einv_trx_hdr_tbl
            set status_code = 'ERROR'
            ,status_message = g_sql_errm
            where transaction_id = v_transaction_id;

            update_standard_tbl(xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_ERROR'),v_transaction_id);

      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in gen_b2c_qr_code : ' || ';' || g_sql_errm
        , g_object);

      raise_application_error(-20905,
                              'Unexpected error in gen_b2c_qr_code :  ' || ';' || g_sql_errm,
                              true);

  end gen_b2c_qr_code;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: set_completion_msg
  ******************************************************************************************************/

  procedure set_completion_msg
  is
    v_count_err number := 0;
    v_count_run number := 0;
    v_count_inelg number := 0;
    v_count_b2c_success number := 0;
    v_count_b2c_err number := 0;
    v_count_b2c_extracted number := 0;
    v_reconcile_msg varchar2(10);

  begin

    select count(1) into v_count_err
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and supply_type_code <> 'B2C'
      and status_code = 'ERROR';

    select count(1) into v_count_run
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and supply_type_code <> 'B2C'
      and status_code = 'PENDING';

    select count(1) into v_count_inelg
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and status_code = 'SUCCESS-INELIGIBLE';

    select count(1) into v_count_b2c_err
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and supply_type_code = 'B2C'
      and status_code = 'ERROR';

    select count(1) into v_count_b2c_success
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and supply_type_code = 'B2C'
      and status_code = 'SUCCESS';

    select count(1) into v_count_b2c_extracted
    from xxoyo_einv_trx_hdr_tbl
    where 1 = 1
      and request_id = g_request_id
      and supply_type_code = 'B2C';

    select decode(g_reconcile_flag, 'N', 'Error', 'Success') into v_reconcile_msg
    from dual;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      ''
      , g_object);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records errored during validation post extraction: ' || v_count_err || ';' || g_sql_errm
      , g_object);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records marked as NOT ELIGIBLE for E-Invoicing: ' || v_count_inelg || ';' || g_sql_errm
      , g_object);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records marked for API integration for E-Invoicing: ' || v_count_run || ';' ||
                      g_sql_errm
      , g_object);


    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records marked as B2C Extracted: ' || v_count_b2c_extracted || ';' ||
                      g_sql_errm
      , g_object);

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records marked as B2C Error: ' || v_count_b2c_err || ';' ||
                      g_sql_errm
      , g_object);

     xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Count of records marked as B2C success: ' || v_count_b2c_success || ';' ||
                      g_sql_errm
      , g_object);

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Status of reconciliation between extraction and Standard Table is ' ||
                      v_reconcile_msg || ';' || g_sql_errm
      , g_object);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      ''
      , g_object);

    if v_count_err > 0 or g_reconcile_flag = 'N' then
      g_retcode := 1;
      g_errbuff := 'One of more records extracted errored in validation';
    end if;

  exception
    when others then
      g_retcode := 1;
      g_errbuff := 'Unexpected error in set_completion_msg';
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in set_completion_msg' || ';' || g_sql_errm
        , g_object);
  end set_completion_msg;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: update_ra_customer_all_tbl
  ******************************************************************************************************/
  procedure update_ra_customer_all_tbl
  is

    v_step varchar2(100);
    v_count number := 0;

  begin

    v_step := 'Update ra_customer_trx_all for attribute8-13';

    update ra_customer_trx_all rcta
    set attribute_category = 'OYO_E-INVOICE'
      , attribute8         = g_request_id --request_id
      , attribute9         = (select supply_type_code
                              from xxoyo_einv_trx_hdr_tbl xetht
                              where xetht.document_num = rcta.trx_number
                                and xetht.document_id = rcta.customer_trx_id
                                and xetht.request_id = g_request_id
    )                                     --supply_type_code
      , attribute10        = (select (case
                                        when irn_eligible_flag = 'Y' then
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'PENDING')

                                        when (irn_eligible_flag = 'N' and supply_type_code = 'B2C' and status_code = 'PENDING' ) then
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_PENDING')

                                        --#starts ticket 2485937501      
                                        when (irn_eligible_flag='N' 
                                              and supply_type_code not in ('B2C','NA') 
                                              and status_code = 'SUCCESS-INELIGIBLE' 
                                              and exists(select 1 from xxoyo_einv_eligible_le_tbl xeelt 
                                                                  where xeelt.irn_eligible_flag='P'
                                                                  and xeelt.legal_entity = xetht.supplier_legal_name
                                                                  and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1)
                                                        )
                                              ) then
                                             xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'PARTIAL')

                                        when (irn_eligible_flag='N' 
                                              and supply_type_code = 'B2C'
                                              and status_code = 'SUCCESS-INELIGIBLE'    
                                              and exists(select 1 from xxoyo_einv_eligible_le_tbl xeelt 
                                                          where xeelt.b2c_eligible_flag='P'
                                                            and xeelt.legal_entity = xetht.supplier_legal_name
                                                            and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1)
                                                        )
                                              ) then
                                             xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'PARTIAL') 

                                        --#end ticket 2485937501                                                   

                                        else
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name,'NOT_ELIGIBLE')
                                        end)
                              from xxoyo_einv_trx_hdr_tbl xetht
                              where xetht.document_num = rcta.trx_number
                                and xetht.document_id = rcta.customer_trx_id
                                and xetht.request_id = g_request_id
    )
      , attribute11        = (select irn_eligible_flag
                              from xxoyo_einv_trx_hdr_tbl xetht
                              where xetht.document_num = rcta.trx_number
                                and xetht.document_id = rcta.customer_trx_id
                                and xetht.request_id = g_request_id
    )                                     --irn_eligible_flag
      , attribute12        = 'N'          --irn_reprocess_flag
      , attribute13        = (select transaction_id
                              from xxoyo_einv_trx_hdr_tbl xetht
                              where xetht.document_num = rcta.trx_number
                                and xetht.document_id = rcta.customer_trx_id
                                and xetht.request_id = g_request_id
    )
    where 1 = 1
      and exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
         and xetht.document_num = rcta.trx_number
         and xetht.document_id = rcta.customer_trx_id
         and xetht.request_id = g_request_id
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in ra_customer_trx_all for all attributes: ' || sql%rowcount
      , g_object);

    update ra_customer_trx_all rcta
    set attribute10 = xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'EXTRACTION_ERROR')
    where 1 = 1
     -- and attribute11 = 'Y'
      and exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
         and xetht.document_num = rcta.trx_number
         and xetht.document_id = rcta.customer_trx_id
         and xetht.request_id = g_request_id
         and xetht.status_code = 'ERROR'
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in ra_customer_trx_all for attribute8-13 for ERROR: ' || sql%rowcount
      , g_object);

    /*
    update ra_cust_trx_line_gl_dist_all rctlgda
       set attribute15 = event_id
     where 1 = 1
       and exists (select 1 from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
       and rctlgda.customer_trx_id = xetht.document_id
       and xetht.request_id = g_request_id
       and xetht.status_code in ('PENDING','ERROR'))
       and not exists
          (select 1
            from xla_events xe
           where xe.event_id = rctlgda.event_id
             and xe.process_status_code = 'P'
             and xe.event_status_code = 'P'
          )
       ;

    update ra_cust_trx_line_gl_dist_all rctlgda
       set event_id = 999999999999999
     where 1 = 1
       and exists
       (select 1 from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
       and rctlgda.customer_trx_id = xetht.document_id
       and xetht.request_id = g_request_id
       and xetht.status_code in ('PENDING','ERROR'))
       and not exists
          (select 1
            from xla_events xe
           where xe.event_id = rctlgda.event_id
             and xe.process_status_code = 'P'
             and xe.event_status_code = 'P'
          )
       ;
      */

    /*v_step := 'Reconciling updates on ra_customer_trx_all table';

    select 1
      into v_count
      from dual
     where not exists
          (
            select 1
              from (
                    select count(distinct rctla.rowid) rctla_count, count(distinct xetlt.rowid) xetlt_count
                     from ra_customer_trx_all rcta, ra_customer_trx_lines_all rctla, xxoyo_einv_trx_lines_tbl xetlt
                    where 1 = 1
                      and rcta.attribute8 = xetlt.request_id
                      and rcta.customer_trx_id = rctla.customer_trx_id
                      and xetlt.request_id = g_request_id
                    )
             where 1 = 1
               and rctla_count <> xetlt_count
            )
          ;

          */

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Update and reconciliation successful for table ra_customer_trx_all '
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in update_ra_customer_all_tbl at step ' || v_step || ';' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20905,
                              'Unexpected error in update_ra_customer_all_tbl at step ' || v_step || ';' || g_sql_errm,
                              true);

  end update_ra_customer_all_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: update_ind_mo_tbl
  ******************************************************************************************************/
  procedure update_ind_mo_tbl
  is

    v_step varchar2(100);
    v_count number := 0;

  begin

    v_step := 'Update xxoyo_po_ind_mo_tbl for attribute8-13';

    update xxoyo_po_ind_mo_tbl xpimt
    set attribute8  = g_request_id --request_id
      , attribute9  = (select supply_type_code
                       from xxoyo_einv_trx_hdr_tbl xetht
                       where xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
                         and xetht.batch_id = xpimt.batch_id
                         and xetht.request_id = g_request_id
    )                              --supply_type_code
      , attribute10 = (select (case
                                        when irn_eligible_flag = 'Y' then
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'PENDING')
                                        when (irn_eligible_flag = 'N' and supply_type_code = 'B2C' and status_code = 'PENDING' ) then
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'B2C_PENDING')
                                        else
                                            xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name,'NOT_ELIGIBLE')
                                        end)
                       from xxoyo_einv_trx_hdr_tbl xetht
                       where xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
                         and xetht.batch_id = xpimt.batch_id
                         and xetht.request_id = g_request_id
    )
      , attribute11 = (select irn_eligible_flag
                       from xxoyo_einv_trx_hdr_tbl xetht
                       where xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
                         and xetht.batch_id = xpimt.batch_id
                         and xetht.request_id = g_request_id
    )                              --irn_eligible_flag
      , attribute12 = 'N'          --irn_reprocess_flag
      , attribute13 = (select transaction_id
                       from xxoyo_einv_trx_hdr_tbl xetht
                       where xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
                         and xetht.batch_id = xpimt.batch_id
                         and xetht.request_id = g_request_id
    )
    where 1 = 1
      and exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
         and xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
         and xetht.batch_id = xpimt.batch_id
         and xetht.request_id = g_request_id
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in xxoyo_po_ind_mo_tbl for all attributes: ' || sql%rowcount
      , g_object);

    update xxoyo_po_ind_mo_tbl xpimt
    set attribute10 = xxoyo_einv_utility_pkg.get_error_details(g_error_lookup_name, 'EXTRACTION_ERROR')
    where 1 = 1
      --and attribute11 = 'Y' Arpit, changed
      and exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
         and xetht.document_num = nvl(xpimt.tax_invoice_no, xpimt.del_challan_num)
         and xetht.batch_id = xpimt.batch_id
         and xetht.request_id = g_request_id
         and xetht.status_code = 'ERROR'
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in xxoyo_po_ind_mo_tbl for attribute8-13 for ERROR: ' || sql%rowcount
      , g_object);

    v_step := 'Reconciling updates on xxoyo_po_ind_mo_tbl table';

    select 1 into v_count
    from dual
    where not exists
      (
        select 1
        from (
               select count(distinct xpimt.rowid) xpimt_count, count(distinct xetlt.rowid) xetlt_count
               from xxoyo_po_ind_mo_tbl xpimt,
                    xxoyo_einv_trx_lines_tbl xetlt
               where 1 = 1
                 and xpimt.attribute8 = xetlt.request_id
                 and xpimt.attribute11 is not null
                 and xetlt.request_id = g_request_id
             )
        where 1 = 1
          and xpimt_count <> xetlt_count
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Update and reconciliation successful for table xxoyo_po_ind_mo_tbl '
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in update_ind_mo_tbl at step ' || v_step || ';' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;

      raise_application_error(-20905, 'Unexpected error in update_ind_mo_tbl at step ' || v_step || ';' || g_sql_errm,
                              true);

  end update_ind_mo_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: categorize_data
  ******************************************************************************************************/


  procedure categorize_data
    is

    begin
        update xxoyo_einv_trx_hdr_tbl
        set status_code      = 'SUCCESS-INELIGIBLE'
          , status_message   = 'Extracted as not eligible - delivery challan records' || g_err_delim
          , supply_type_code = 'NA' --Arpit, Changed
        where 1 = 1
          and status_code in ('PENDING')
          and request_id = g_request_id
          and irn_eligible_flag = 'N';

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for delivery challan records: ' || sql%rowcount
          , g_object);

        update xxoyo_einv_trx_hdr_tbl
        set supply_type_code  = 'B2C'
          , irn_eligible_flag = 'N'
        where 1 = 1
          and status_code = 'PENDING'
          and request_id = g_request_id
          and irn_eligible_flag = 'Y'
          and attribute15 in (select upper(meaning)
                              from fnd_lookup_values
                              where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                and description = 'Unregistered'
                                and enabled_flag = 'Y');

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for reporting code Unregistered(B2C): ' || sql%rowcount
          , g_object);


        update xxoyo_einv_trx_hdr_tbl
        set supply_type_code         = 'EXPWOP'
          , recipient_gstin          = 'URP'
          , recipient_state_code_upd = '96'
          , recipient_state_upd      = '96'
          , pos_state_code_upd       = '96'
          , pos_state_upd            = '96'
          , recipient_pincode_upd    = '999999'
        where 1 = 1
          and status_code in ('PENDING')
          and request_id = g_request_id
          and irn_eligible_flag = 'Y'-- Added
          and recipient_country_code <> 'IN'
          and attribute15 in (select upper(meaning)
                              from fnd_lookup_values
                              where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                and description = 'Export Type'
                                and enabled_flag = 'Y')
          and total_tax_amount = 0;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for EXPWOP: ' || sql%rowcount
          , g_object);

        update xxoyo_einv_trx_hdr_tbl
        set supply_type_code         = 'SEZWOP'
          , recipient_gstin          = 'URP'
          , recipient_state_code_upd = '96'
          , recipient_state_upd      = '96'
          , pos_state_code_upd       = '96'
          , pos_state_upd            = '96'
          , recipient_pincode_upd    = '999999'
          , sez_unit                 = 'true'
        where 1 = 1
          and status_code = 'PENDING'
          and request_id = g_request_id
          and irn_eligible_flag = 'Y'-- Added
          and attribute15 in (select upper(meaning)
                              from fnd_lookup_values
                              where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                and description = 'SEZ Type'
                                and enabled_flag = 'Y')
          and total_tax_amount = 0;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for SEZWOP: ' || sql%rowcount
          , g_object);

        update xxoyo_einv_trx_hdr_tbl
        set supply_type_code         = 'SEZWP'
          , recipient_gstin          = 'URP'
          , recipient_state_code_upd = '96'
          , recipient_state_upd      = '96'
          , pos_state_code_upd       = '96'
          , pos_state_upd            = '96'
          , recipient_pincode_upd    = '999999'
          , sez_unit                 = 'true'
        where 1 = 1
          and status_code = 'PENDING'
          and request_id = g_request_id
          and irn_eligible_flag = 'Y'-- Added
          and attribute15 in (select upper(meaning)
                              from fnd_lookup_values
                              where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                and description = 'SEZ Type'
                                and enabled_flag = 'Y')
          and total_tax_amount <> 0;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for SEZWP: ' || sql%rowcount
          , g_object);


        update xxoyo_einv_trx_hdr_tbl
        set supply_type_code = 'B2B'
        where 1 = 1
          and status_code = 'PENDING'
          and request_id = g_request_id
          and irn_eligible_flag = 'Y'-- Added
          and attribute15 in (select upper(meaning)
                              from fnd_lookup_values
                              where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                and description = 'Regular'
                                and enabled_flag = 'Y');
        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for Regular: ' || sql%rowcount
          , g_object);

       ----start 2138373344
       update xxoyo_einv_trx_hdr_tbl xetht
        set supply_type_code = 'B2C'
         , irn_eligible_flag = 'N'
        where 1 = 1
          and xetht.supply_type_code = 'B2B'
          and xetht.request_id = g_request_id
          and xetht.recipient_gstin = xetht.supplier_gstin
          and exists
            (
            select 1
            from xxoyo_einv_trx_taxes_tbl xettt
            where xettt.transaction_id = xetht.transaction_id
            and xettt.taxable_amount = 0
            )
          ;
        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for B2C(EXCEPTION): ' || sql%rowcount
          , g_object);
        ---end 2138373344


        update xxoyo_einv_trx_hdr_tbl
        set status_code    = 'ERROR'
          , status_message = status_message || 'Unexpected customer registration code ' || attribute15 || ';' || g_err_delim
          , supply_type_code = 'NA' --- Arpit, Changed
          , irn_eligible_flag = 'N' --- Arpit, Changed
        where 1 = 1
          and status_code in ('PENDING')
          and request_id = g_request_id
          and attribute15 not in (select upper(meaning)
                                  from fnd_lookup_values
                                  where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                                    and enabled_flag = 'Y');
        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for Unexpected customer registration code: ' || sql%rowcount
          , g_object);

        update xxoyo_einv_trx_hdr_tbl
        set status_code      = 'ERROR'
          , status_message   = status_message || 'Null customer registration code ' || attribute15 || ';' || g_err_delim
          , supply_type_code = 'NA'
          , irn_eligible_flag = 'N' --- Arpit, Changed
        where 1 = 1
          and status_code in ('PENDING', 'ERROR')
          and request_id = g_request_id
          and attribute15 is null;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for Null customer registration code: ' || sql%rowcount
          , g_object);

        update xxoyo_einv_trx_hdr_tbl
        set status_code       = 'SUCCESS-INELIGIBLE'
          , status_message    = status_message || 'LE have revenue less than 500/100 crore INR' || g_err_delim
          , irn_eligible_flag = 'N'
        where 1 = 1
          and request_id = g_request_id
          and supply_type_code <> 'B2C'
          and not exists(select 1
                         from xxoyo_einv_eligible_le_tbl xeelt
                         where xeelt.irn_eligible_flag  in ('Y','P')   --#ticket 2485937501
                           and xeelt.legal_entity = supplier_legal_name
                           --and nvl(xeelt.effective_end_date, sysdate + 1) > sysdate
                           and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1) 
          );

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for ineligible LEs: ' || sql%rowcount
          , g_object);

        --mrinali change --#ticket 2485937501
        update xxoyo_einv_trx_hdr_tbl     
        set status_code       = 'SUCCESS-INELIGIBLE'
          , status_message    = status_message || 'Though enabled in OYO but B2B E-Invoicing not enabled in Oracle' || g_err_delim
          , irn_eligible_flag = 'N'
        where 1 = 1
          and request_id = g_request_id
          and supply_type_code <> 'B2C' --B2B,SEZWP,SEZWOP,EXPWP,EXPWOP,NA
          and exists(select 1
                         from xxoyo_einv_eligible_le_tbl xeelt
                         where xeelt.irn_eligible_flag ='P'
                           and xeelt.legal_entity = supplier_legal_name
                           --and nvl(xeelt.effective_end_date, sysdate + 1) > sysdate
                           and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1) 
          );
          xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Though enabled in OYO but B2B E-Invoicing not enabled in Oracle: ' || sql%rowcount
          , g_object);


        update xxoyo_einv_trx_hdr_tbl
        set status_code       = 'SUCCESS-INELIGIBLE'
          , status_message    = status_message || 'LE have revenue less than 500/100 crore INR(B2C)' || g_err_delim
          , irn_eligible_flag = 'N'
        where 1 = 1
          and request_id = g_request_id
          and supply_type_code = 'B2C'
          and not exists(select 1
                         from xxoyo_einv_eligible_le_tbl xeelt
                         where xeelt.b2c_eligible_flag in ('Y','P')   --#ticket 2485937501
                           and xeelt.legal_entity = supplier_legal_name
                           --and nvl(xeelt.effective_end_date, sysdate + 1) > sysdate
                           and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1) 
          );

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for ineligible LEs(B2C): ' || sql%rowcount
          , g_object);

        --mrinali change --#ticket 2485937501
        update xxoyo_einv_trx_hdr_tbl     
        set status_code       = 'SUCCESS-INELIGIBLE'
          , status_message    = status_message || 'Though enabled in OYO but B2C E-Invoicing not enabled in Oracle' || g_err_delim
          , irn_eligible_flag = 'N'
        where 1 = 1
          and request_id = g_request_id
          and supply_type_code = 'B2C'
          and exists(select 1
                         from xxoyo_einv_eligible_le_tbl xeelt
                         where xeelt.b2c_eligible_flag ='P'
                           and xeelt.legal_entity = supplier_legal_name
                           --and nvl(xeelt.effective_end_date, sysdate + 1) > sysdate
                           and sysdate between xeelt.effective_start_date and nvl(xeelt.effective_end_date, sysdate + 1) 
          );
          xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Though enabled in OYO but B2C E-Invoicing not enabled in Oracle: ' || sql%rowcount
          , g_object);


    exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in categorize_data: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20905, 'Unexpected error in categorize_data' || g_sql_errm, true);

    end categorize_data;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: validate_ar_data
  ******************************************************************************************************/

  procedure validate_ar_data
  is

  begin

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'in validate_ar_data'
      , g_object);

    --FACNT-1043
    update xxoyo_einv_trx_hdr_tbl
        set status_code       = 'SUCCESS-INELIGIBLE'
          , status_message    = status_message || 'Ineligible Document Type Code' || g_err_delim
          , irn_eligible_flag = 'N'  --- Arpit, Changed
        where 1 = 1
          and status_code in ('PENDING', 'SUCCESS-INELIGIBLE')
          and request_id = g_request_id
          and document_type_code not in (select meaning
                                         from fnd_lookup_values
                                         where lookup_type = 'XXOYO_EINV_AR_DOCUMENT_TYPES'
                                           and description = 'Document Type Code'
                                           and enabled_flag = 'Y');

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Records updated for Ineligible Document Type Code: ' || sql%rowcount
          , g_object);



    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Country value India is not eligible for EXPORT customers' || g_err_delim
    where 1 = 1
      and status_code in 'PENDING'
      and request_id = g_request_id
      and recipient_country_code = 'IN'
      and attribute15 in (select upper(meaning)
                          from fnd_lookup_values
                          where lookup_type = 'XXOYO_EINV_AR_CUSTOMER_TYPES'
                            and description = 'Export Type'
                            and enabled_flag = 'Y');

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for EXPORT having IN country code: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid or null recipient GSTIN ' || ';' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and supply_type_code = 'B2B'
      and (recipient_gstin is null or recipient_gstin = 'GSTNOTAVAILABLE' or length(recipient_gstin) <> 15);

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid or null recipient GSTIN: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Recipient GSTIN is equal to Supplier GSTIN ' || ';' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and supply_type_code = 'B2B'
      and recipient_gstin = supplier_gstin;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Recipient GSTIN is equal to Supplier GSTIN: ' || sql%rowcount
      , g_object);

    ---FACNT-1108 check removed for the recipient mail
    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message ||
                         'Invalid recipient data, check billing address,place,phone number,country and state code' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (recipient_legal_name is null
      or recipient_address1 is null
      or recipient_state_upd is null
      or recipient_phone_num is null
      or recipient_country_code is null
      or recipient_place is null
      or recipient_state_code_upd is null);

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid recipient data: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message ||
                         'Invalid supplier data, check legal name and address details' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (supplier_legal_name is null
      or supplier_address1 is null
      or supplier_state is null
      or supplier_country_code is null
      or supplier_place is null);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid supplier data: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invoice Type or POS State or SEZ unit or document_date or Supply Type is null' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (shipment_type is null
      or pos_state_upd is null
      or sez_unit is null
      or document_date is null
      or supply_type_code is null
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid Invoice Type: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid or null Supplier GSTIN' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (supplier_gstin is null or supplier_gstin = 'GSTNOTAVAILABLE')
    ;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid or null Supplier GSTIN: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Document num can not start with 0, / and -' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and substr(document_num, 1, 1) in ('0', '/', '-');
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid document_num: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid Recipient or Supplier GSTN' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (substr(supplier_gstin, 0, 2) <> supplier_state_code
      or substr(recipient_gstin, 0, 2) <> recipient_state_code_upd)
      and supply_type_code = 'B2B';
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid Recipient or Supplier GSTN: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid PAN number in GSTIN' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and supply_type_code = 'B2B'
      and substr(recipient_gstin, 3, 10) <>
          nvl(xxoyo_einv_utility_pkg.get_third_party_reg(cust_account_id, cust_acct_site_bill_id, 'PAN'), 'XXXXXX');

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for invalid PAN number in GSTIN: ' || sql%rowcount
      , g_object);

    --rajinder
    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid or null HSN or SAC Code' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      --and irn_eligible_flag = 'Y' --rajinder, --- Arpit, Changed
      and exists
      (select 1
       from xxoyo_einv_trx_lines_tbl xetlt
       where 1 = 1
         and xetlt.request_id = xetht.request_id
         and xetlt.transaction_id = xetht.transaction_id
         and (case
                when xetlt.hsn_or_sac_class = 'GOODS' and not regexp_like(length(xetlt.hsn_or_sac_code), '4|6|8') then
                  0
                when xetlt.hsn_or_sac_class = 'SERVICE' and not regexp_like(length(xetlt.hsn_or_sac_code), '4|5|6') then
                  0
                end = 0
         or hsn_or_sac_class is null
         )
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid or null HSN or SAC Code: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Line count less than 1 or greater than 1000' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and exists
      (select 1
       from xxoyo_einv_trx_lines_tbl xetlt
       where 1 = 1
         and xetlt.request_id = xetht.request_id
         and xetlt.transaction_id = xetht.transaction_id
       group by transaction_id
       having count(1) not between 1 and 1000
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Line count less than 1 or greater than 1000: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Incorrect taxes extracted' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and exists
      (
        select 1
        from xxoyo_einv_trx_taxes_tbl xettt
        where 1 = 1
          and xetht.request_id = xettt.request_id
          and xetht.transaction_id = xettt.transaction_id
          and xxoyo_einv_utility_pkg.validate_supply_type_taxes(g_request_id, xettt.transaction_id,
                                                                xetht.supply_type_code, xetht.shipment_type) = 1
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Incorrect taxes extracted: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Line of Business is null' || g_err_delim
    where 1 = 1
      and request_id = g_request_id
      and status_code in ('PENDING', 'ERROR')
      and line_of_business is null;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Line of Business is null: ' || sql%rowcount
      , g_object);

      -- Check for the CN and DN
      update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Original invoice Number and Date is not correct' || g_err_delim
    where 1 = 1
      and request_id = g_request_id
      and status_code in ('PENDING', 'ERROR')
      and document_type_code in (select meaning
                                         from fnd_lookup_values
                                         where lookup_type = 'XXOYO_EINV_AR_DOCUMENT_TYPES'
                                           and description = 'Document Type Code'
                                           and enabled_flag = 'Y'
                                           and lookup_code in ('CM','DM'))
      and not exists (select 1 from ra_customer_trx_all rcta
                       where rcta.trx_number = xetht.preceding_doc_num
                         and to_char(rcta.trx_date,'RRRR-MM-DD') = xetht.preceding_doc_date
                    );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Original invoice Number is not correct: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_message = 'ERROR:' || status_message
    where 1 = 1
      and request_id = g_request_id
      and status_code = 'ERROR';
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for status message ERROR: ' || sql%rowcount
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in validate_ar_data: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20905, 'Unexpected error in validate_ar_data' || g_sql_errm, true);

  end validate_ar_data;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: validate_mat_data
  ******************************************************************************************************/

  procedure validate_mat_data
  is

  begin

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'in validate_mat_data'
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid or null recipient GSTIN ' || ';' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING')
      and request_id = g_request_id
      and supply_type_code = 'B2B'
      and (recipient_gstin is null or recipient_gstin = 'GSTNOTAVAILABLE' or length(recipient_gstin) <> 15);

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid or null recipient GSTIN: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Recipient GSTIN is equal to Supplier GSTIN ' || ';' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and supply_type_code = 'B2B'
      and recipient_gstin = supplier_gstin;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Recipient GSTIN is equal to Supplier GSTIN: ' || sql%rowcount
      , g_object);



    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message ||
                         'Invalid recipient data, check billing address,place,phone number,email,country and state code' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (recipient_legal_name is null
      or recipient_address1 is null
      or recipient_state_upd is null
      or recipient_phone_num is null
      or recipient_email is null
      or recipient_country_code is null
      or recipient_place is null
      or recipient_state_code_upd is null);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid recipient data: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message ||
                         'Invalid supplier data, check legal name and address details' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (supplier_legal_name is null
      or supplier_address1 is null
      or supplier_state is null
      or supplier_country_code is null
      or supplier_place is null);
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid supplier data: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invoice Type or POS State or SEZ unit or document_date or Supply Type is null' ||
                         g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (shipment_type is null
      or pos_state_upd is null
      or sez_unit is null
      or document_date is null
      or supply_type_code is null
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invoice Type or POS: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Supplier GSTIN is null' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and supplier_gstin is null;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Supplier GSTIN is null: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Document num to start with 0, / and -' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and substr(document_num, 1, 1) in ('0', '/', '-');

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for invalid Document num: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid Recipient or Supplier GSTN' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and (substr(supplier_gstin, 0, 2) <> supplier_state_code
      or substr(recipient_gstin, 0, 2) <> recipient_state_code_upd)
      and supply_type_code = 'B2B';

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid Recipient or Supplier GSTN: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Invalid or null HSN or SAC Code' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and exists
      (select 1
       from xxoyo_einv_trx_lines_tbl xetlt
       where 1 = 1
         and xetlt.request_id = xetht.request_id
         and xetlt.transaction_id = xetht.transaction_id
         and case
               when xetlt.hsn_or_sac_class = 'GOODS' and not regexp_like(length(xetlt.hsn_or_sac_code), '4|6|8') then
                 0
               when xetlt.hsn_or_sac_class = 'SERVICE' and not regexp_like(length(xetlt.hsn_or_sac_code), '4|5|6') then
                 0
               end = 0
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Invalid or null HSN or SAC Code: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Line count less than 1 or greater than 1000' || g_err_delim
    where 1 = 1
      and status_code in ('PENDING', 'ERROR')
      and request_id = g_request_id
      and exists
      (select 1
       from xxoyo_einv_trx_lines_tbl xetlt
       where 1 = 1
         and xetlt.request_id = xetht.request_id
         and xetlt.transaction_id = xetht.transaction_id
       group by transaction_id
       having count(1) not between 1 and 1000
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Line count less than 1 or greater than 1000: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Incorrect taxes extracted' || g_err_delim
    where 1 = 1
      and request_id = g_request_id
      and status_code in ('PENDING', 'ERROR')
      and exists
      (
        select 1
        from xxoyo_einv_trx_taxes_tbl xettt
        where 1 = 1
          and xetht.request_id = xettt.request_id
          and xetht.transaction_id = xettt.transaction_id
          and xxoyo_einv_utility_pkg.validate_supply_type_taxes(g_request_id, xettt.transaction_id,
                                                                xetht.supply_type_code, xetht.shipment_type) = 1
      );
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Incorrect taxes extracted: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set status_code    = 'ERROR'
      , status_message = status_message || 'Line of Business is null' || g_err_delim
    where 1 = 1
      and request_id = g_request_id
      and status_code in ('PENDING', 'ERROR')
      and line_of_business is null;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for Line of Business is null: ' || sql%rowcount
      , g_object);


    update xxoyo_einv_trx_hdr_tbl xetht
    set status_message = 'ERROR:' || status_message
    where 1 = 1
      and request_id = g_request_id
      and status_code = 'ERROR';

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for ERROR: ' || sql%rowcount
      , g_object);


  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in validate_mat_data: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20905, 'Unexpected error in validate_mat_data' || g_sql_errm, true);

  end validate_mat_data;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: reconcile_mat_taxes
  ******************************************************************************************************/

  procedure reconcile_mat_taxes
  is

    v_count number := 0;
    v_step varchar2(100);

  begin

    v_step := 'Duplicate item and tax codes in an invoice';

    select 1 into v_count
    from dual
    where not exists
      (select 1
       from xxoyo_einv_trx_taxes_tbl
       where 1 = 1
         and request_id = g_request_id
       group by transaction_id, item_line_number, tax_name
       having count(1) > 1
      );


    v_step := 'Validate amount between line and taxes table';

    select 1 into v_count
    from dual
    where not exists
      (select 1
       from xxoyo_einv_trx_lines_tbl xetlt
       where 1 = 1
         and xetlt.request_id = g_request_id
         and exists
         (
           select 1
           from (select sum(taxable_amount) sum_tax_amount, xettt.transaction_id, xettt.item_line_number
                 from xxoyo_einv_trx_taxes_tbl xettt
                 where 1 = 1
                   and xettt.request_id = xetlt.request_id
                   and xettt.transaction_id = xetlt.transaction_id
                   and xettt.item_line_number = xetlt.line_number
                 group by xettt.transaction_id, xettt.item_line_number
                ) xettt_sum
           where 1 = 1
             and xettt_sum.sum_tax_amount <> xetlt.item_tax_amount
             and xettt_sum.transaction_id = xetlt.transaction_id
             and xettt_sum.item_line_number = xetlt.line_number
         )
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Reconciliation successful at taxes level'
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Reconciliation failed of tax records in reconcile_mat_taxes at step ' || v_step || ';' ||
                        g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20908, 'Reconciliation failed of tax records in reconcile_mat_taxes: ' || g_sql_errm,
                              true);
  end reconcile_mat_taxes;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: insert_in_taxes_tbl
  ******************************************************************************************************/

  procedure insert_in_taxes_tbl
  is

  begin

    insert into xxoyo_einv_trx_taxes_tbl
    ( transaction_id
    , item_line_number
    , tax_name
    , tax_rate
    , taxable_amount
    , tax_lines_line_id
    , tax_lines_tax_type_id
    , creation_date
    , created_by
    , last_update_date
    , last_updated_by
    , last_update_login
    , request_id)
    select distinct xetlt.transaction_id
                  , xetlt.line_number
                  , xeteg.tax_name
                  , xeteg.tax_rate
                  , xeteg.taxable_amount
                  , xeteg.tax_lines_line_id
                  , xeteg.tax_lines_tax_type_id
                  , sysdate
                  , xetlt.created_by
                  , sysdate
                  , xetlt.last_updated_by
                  , userenv('SESSIONID')
                  , g_request_id
    from xxoyo_einv_trx_extract_gt xeteg,
         xxoyo_einv_trx_hdr_tbl xetht,
         xxoyo_einv_trx_lines_tbl xetlt
    where 1 = 1
      and xeteg.document_num = xetht.document_num
      --and xeteg.batch_id = xetht.batch_id --Need to see this check for AR
      and xetht.request_id = g_request_id
      and xetht.transaction_id = xetlt.transaction_id
      and xetht.request_id = xetlt.request_id
      and xeteg.tax_factor_trx_id = xetlt.tax_factor_trx_id
      and xeteg.tax_factor_trx_line_id = xetlt.tax_factor_trx_line_id
      and xeteg.tax_factor_trx_id is not null;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records inserted in xxoyo_einv_trx_taxes_tbl: ' || sql%rowcount
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in insert_in_taxes_tbl: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20901, 'Unexpected error in insert_in_taxes_tbl' || g_sql_errm, true);
  end insert_in_taxes_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: reconcile_mat_lines
  ******************************************************************************************************/

  procedure reconcile_mat_lines
  is

    v_count number := 0;
    v_step varchar2(100);

  begin

    v_step := 'Duplicate item codes in an invoice';

    select 1 into v_count
    from dual
    where not exists
      (select 1
       from xxoyo_einv_trx_lines_tbl
       where 1 = 1
         and request_id = g_request_id
       group by transaction_id, item_code
       having count(1) > 1
      );

    v_step := 'Validate sum of gross amount between header and lines table';

    select 1 into v_count
    from dual
    where not exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl xetht
       where 1 = 1
         and xetht.request_id = g_request_id
         and exists
         (
           select 1
           from (select sum(item_gross_amount) sum_gross_amount, xetlt.transaction_id
                 from xxoyo_einv_trx_lines_tbl xetlt
                 where 1 = 1
                   and xetlt.request_id = xetht.request_id
                   and xetlt.transaction_id = xetht.transaction_id
                 group by xetlt.transaction_id
                ) xetlt_sum
           where 1 = 1
             and xetlt_sum.sum_gross_amount <> xetht.gross_amount
             and xetlt_sum.transaction_id = xetht.transaction_id
         )
      );

    v_step := 'Validate number of lines from MO staging table';

    select 1 into v_count
    from dual
    where not exists
      (
        select 1
        from (
               select xetht.document_num, xetht.batch_id, count(1) extracted_count
               from xxoyo_einv_trx_lines_tbl xetlt,
                    xxoyo_einv_trx_hdr_tbl xetht
               where 1 = 1
                 and xetlt.transaction_id = xetht.transaction_id
                 and xetlt.request_id = xetlt.request_id
                 and xetht.request_id = g_request_id
               group by xetht.document_num, xetht.batch_id
             ) extracted_data
        where exists
                (
                  select *
                  from (
                         select xpimt.tax_invoice_no, xpimt.batch_id, count(1) actual_count
                         from xxoyo_po_ind_mo_tbl xpimt
                         where 1 = 1
                           and extracted_data.document_num = xpimt.tax_invoice_no
                           and extracted_data.batch_id = xpimt.batch_id
                         group by xpimt.tax_invoice_no, xpimt.batch_id
                       ) actual_data
                  where actual_data.tax_invoice_no = extracted_data.document_num
                    and actual_data.batch_id = extracted_data.batch_id
                    and actual_data.actual_count <> extracted_data.extracted_count
                )
      );

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Reconciliation successful at line level'
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Reconciliation failed of line records in reconcile_mat_lines at step ' || v_step || ';' ||
                        g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20908, 'Reconciliation failed of line records in reconcile_mat_lines: ' || g_sql_errm,
                              true);
  end reconcile_mat_lines;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: insert_in_lines_tbl
  ******************************************************************************************************/

  procedure insert_in_lines_tbl
  is

    v_line_ctr number := 0;
    v_count number := 0;

    cursor c_lines_cur is (select distinct xetht.transaction_id
                                         , item_name
                                         , item_code
                                         , inventory_item_id
                                         , item_quantity
                                         , item_price
                                         , hsn_or_sac_code
                                         , item_base_amount
                                         , item_tax_amount
                                         , item_gross_amount
                                         , tax_factor_trx_id
                                         , tax_factor_trx_line_id
                                         , tax_factor_entity
                                         , tax_factor_det_id
                                         , tax_lines_det_factor_id
                                         , tax_lines_trx_id
                                         , tax_lines_trx_line_id
                                         , tax_lines_entity
                                         , mmt_transaction_id
                                         , g_request_id request_id
                           from xxoyo_einv_trx_extract_gt xeteg,
                                xxoyo_einv_trx_hdr_tbl xetht
                           where 1 = 1
                             and xeteg.document_num = xetht.document_num
                             --and xeteg.batch_id = xetht.batch_id --Need to see this check for AR
                             and xetht.request_id = g_request_id
    );

  begin

    for i in c_lines_cur
      loop

        v_count := v_count + 1;

        select nvl(max(line_number), 0) + 1 into v_line_ctr
        from xxoyo_einv_trx_lines_tbl
        where transaction_id = i.transaction_id
          and request_id = g_request_id;

        insert into xxoyo_einv_trx_lines_tbl
        ( transaction_id
        , line_number
        , item_name
        , item_code
        , inventory_item_id
        , item_quantity
        , item_price
        , hsn_or_sac_code
        , item_base_amount
        , item_tax_amount
        , item_gross_amount
        , tax_factor_trx_id
        , tax_factor_trx_line_id
        , tax_factor_entity
        , tax_factor_det_id
        , tax_lines_det_factor_id
        , tax_lines_trx_id
        , tax_lines_trx_line_id
        , tax_lines_entity
        , mmt_transaction_id
        , request_id
        , creation_date
        , created_by
        , last_update_date
        , last_updated_by
        , last_update_login)
        values ( i.transaction_id
               , v_line_ctr
               , i.item_name
               , i.item_code
               , i.inventory_item_id
               , i.item_quantity
               , nvl(i.item_price, 0)
               , i.hsn_or_sac_code
               , i.item_base_amount
               , i.item_tax_amount
               , i.item_gross_amount
               , i.tax_factor_trx_id
               , i.tax_factor_trx_line_id
               , i.tax_factor_entity
               , i.tax_factor_det_id
               , i.tax_lines_det_factor_id
               , i.tax_lines_trx_id
               , i.tax_lines_trx_line_id
               , i.tax_lines_entity
               , i.mmt_transaction_id
               , i.request_id
               , sysdate
               , g_user_id
               , sysdate
               , g_user_id
               , userenv('SESSIONID'));

      end loop;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records inserted in xxoyo_einv_trx_lines_tbl: ' || v_count
      , g_object);

    --rajinder, write a function to derive GOODS vs SERVICE based on prefix 9
    update xxoyo_einv_trx_lines_tbl xetlt
    set hsn_or_sac_class = xxoyo_einv_utility_pkg.get_hsn_or_sac_class(hsn_or_sac_code)
    where 1 = 1
      and request_id = g_request_id;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for hsn_or_sac_class: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set is_service = 'M'
    where exists
            (select 1
             from (
                    (select count(distinct hsn_or_sac_class) hsn_sac
                     from xxoyo_einv_trx_lines_tbl xetlt
                     where 1 = 1
                       and xetht.request_id = xetlt.request_id
                       and xetht.transaction_id = xetlt.transaction_id
                    )
                  )
             where hsn_sac > 1
            )
      and request_id = g_request_id;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for is_service as M: ' || sql%rowcount
      , g_object);

    update xxoyo_einv_trx_hdr_tbl xetht
    set is_service = (select is_service
                      from (select distinct decode(hsn_or_sac_class, 'GOODS', 'N', 'SERVICE', 'Y') is_service
                            from xxoyo_einv_trx_lines_tbl xetlt
                            where 1 = 1
                              and xetht.request_id = xetlt.request_id
                              and xetht.transaction_id = xetlt.transaction_id
                           )
                      group by is_service
                      having count(1) = 1
    )
    where 1 = 1
      and request_id = g_request_id
      and is_service is null;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for is_service as Y or N: ' || sql%rowcount
      , g_object);


    update xxoyo_einv_trx_hdr_tbl xetht
    set total_base_amount = (select sum(item_base_amount)
                             from xxoyo_einv_trx_lines_tbl
                             where transaction_id = xetht.transaction_id)
      , total_tax_amount  = (select sum(item_tax_amount)
                             from xxoyo_einv_trx_lines_tbl
                             where transaction_id = xetht.transaction_id)
      , gross_amount      = (select sum(item_gross_amount)
                             from xxoyo_einv_trx_lines_tbl
                             where transaction_id = xetht.transaction_id)
    where 1 = 1
      and request_id = g_request_id;
    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                      'Records updated for amounts in header table: ' || sql%rowcount
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in insert_in_lines_tbl: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20901, 'Unexpected error in insert_in_lines_tbl' || g_sql_errm, true);
  end insert_in_lines_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: print_ind_po_records
  ******************************************************************************************************/

  procedure print_ind_po_records
  is

    cursor cur_err_details is select tax_invoice_no, del_challan_num, batch_id
                              from xxoyo_po_ind_mo_tbl xpimt
                              where 1 = 1
                                and xpimt.creation_date > trunc(sysdate - 90)
                                and xpimt.creation_date > to_date('01-OCT-2020','DD-MON-RRRR')
                                and process_flag <> 'E'
                                and batch_id is not null
                                and (xpimt.del_challan_num is not null or xpimt.tax_invoice_no is not null) ----6909745872
                                and not exists
                                (select 1
                                 from xxoyo_einv_trx_hdr_tbl xetht
                                 where 1 = 1
                                   and case
                                         when invoice_process = 'DTP Intra-state' then
                                           xpimt.del_challan_num
                                         else xpimt.tax_invoice_no
                                         end = xetht.document_num
                                   and xetht.batch_id = xpimt.batch_id
                                );

  begin

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'tax_invoice_no' || g_delim || 'del_challan_num' || g_delim || 'batch_id'
      , g_object);

    for i in cur_err_details
      loop

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                          i.tax_invoice_no || g_delim || i.del_challan_num || g_delim || i.batch_id
          , g_object);

      end loop;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in print_ind_po_records: ' || g_sql_errm
        , g_object);
  end print_ind_po_records;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: reconcile_header_tbl
  ******************************************************************************************************/

  procedure reconcile_header_tbl
  is

    v_count number := 0;
    v_step varchar2(100);

  begin

    v_step := 'Duplicate document num extracted within a request ID';

    select 1 into v_count
    from dual
    where not exists
      (select 1
       from xxoyo_einv_trx_hdr_tbl
       where 1 = 1
         and request_id = g_request_id
       group by document_num
       having count(1) > 1
      );

    v_step := 'Duplicate document num extracted again';

    select 1 into v_count
    from dual
    where not exists
      (
        select 1
        from xxoyo_einv_trx_hdr_tbl xetht
        where 1 = 1
          and xetht.request_id = g_request_id
          and exists
          (select 1
           from xxoyo_einv_trx_hdr_tbl xetht1
           where 1 = 1
             and xetht1.document_num = xetht.document_num
             and xetht1.request_id <> xetht.request_id
             and xetht1.status_code not in ('ERROR','RE-EXTRACTED')
          )
      );

    if g_process_name = g_mat_process then
        v_step := 'Validating data against xxoyo_po_ind_mo_tbl';

        begin

          select 1
            into v_count
            from dual
           where not exists
                  (
                  select tax_invoice_no,del_challan_num
                    from xxoyo_po_ind_mo_tbl xpimt
                   where 1 = 1
                     and xpimt.creation_date  > to_date('01-OCT-2020','DD-MON-RRRR')
                     and process_flag <> 'E'
                     and batch_id is not null
                     and (tax_invoice_no is not null or del_challan_num is not null)
                     and not exists
                          (select 1
                             from xxoyo_einv_trx_hdr_tbl xetht
                            where 1 = 1
                              and case
                                      when invoice_process = 'DTP Intra-state' then
                                          xpimt.del_challan_num
                                      else xpimt.tax_invoice_no
                                  end = xetht.document_num
                              and xetht.batch_id = xpimt.batch_id
                          )
                  )
                  ;

        exception
          when others then
            g_reconcile_flag := 'N';
            print_ind_po_records;
        end;
    end if ;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Reconciliation failed of header records in reconcile_header_tbl at step ' || v_step || ';' ||
                        g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20908,
                              'Reconciliation failed of header records in reconcile_header_tbl: ' || g_sql_errm, true);
  end reconcile_header_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: update_mat_hdr_rec
  ******************************************************************************************************/
  procedure update_mat_hdr_rec
  is
  begin
    update xxoyo_einv_trx_hdr_tbl
    set transaction_id             = xxoyo_einv_trx_id.nextval
      , creation_date              = sysdate
      , created_by                 = g_user_id
      , last_update_date           = sysdate
      , last_updated_by            = g_user_id
      , last_update_login          = userenv('SESSIONID')
      , status_code                = 'PENDING'
      , line_of_business           = xxoyo_einv_utility_pkg.get_line_of_business(owning_organization_id, 'IO')
      , supplier_country_code      = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'COUNTRY')
      , supplier_address1          = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'ADDRESS2')
      , supplier_place             = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'PLACE')
      , supplier_state_code        = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_NUMBER')
      , supplier_state             = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_CODE')
      , supplier_state_en          = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_NAME')
      , supplier_pincode           = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'PINCODE')
      , recipient_legal_name       = decode(invoice_process,
                                            'DTP Intra-state', supplier_legal_name,
                                            xxoyo_einv_utility_pkg.get_location_details(recipient_location_id,
                                                                                        'ADDRESS1')
      )
      , recipient_address1         = decode(invoice_process,
                                            'DTP Intra-state',
                                            xxoyo_einv_utility_pkg.get_location_details(recipient_location_id,
                                                                                        'ADDRESS1')
      , xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'ADDRESS2')
      )
      , recipient_place            = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'PLACE')
      , recipient_state_code_orig  = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'STATE_NUMBER')
      , recipient_state_code_upd   = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'STATE_NUMBER')
      , recipient_state_orig       = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'STATE_CODE')
      , recipient_state_upd        = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'STATE_CODE')
      , recipient_state_en         = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'STATE_NAME')
      , recipient_pincode_orig     = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'PINCODE')
      , recipient_pincode_upd      = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'PINCODE')
      , recipient_country_code     = xxoyo_einv_utility_pkg.get_location_details(recipient_location_id, 'COUNTRY')
      , ship_to_legal_name         = decode(invoice_process,
                                            'Intransit',
                                            xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS1')
      , 'DTP Inter-state', nvl(xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'DESCRIPTION'),
                               xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS1'))
      , 'DTP Intra-state', nvl(xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'DESCRIPTION'),
                               xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS1'))
      )
      , ship_to_address1           = decode(invoice_process,
                                            'Intransit',
                                            xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS2')
                                            , 'DTP Inter-state', nvl(xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS1'),
                                                                     xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS2'))
                                            , 'DTP Intra-state', nvl(xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS1'),
                                                                     xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'ADDRESS2'))
                                            )
      , ship_to_place              = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'PLACE')
      , ship_to_state_code         = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'STATE_NUMBER')
      , ship_to_state              = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'STATE_CODE')
      , ship_to_state_en           = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'STATE_NAME')
      , ship_to_pincode            = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'PINCODE')
      , ship_to_country_code       = xxoyo_einv_utility_pkg.get_location_details(ship_to_location_id, 'COUNTRY')
      , dispatch_from_address1     = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'ADDRESS2')
      , dispatch_from_place        = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'PLACE')
      , dispatch_from_state_code   = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_NUMBER')
      , dispatch_from_state        = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_CODE')
      , dispatch_from_state_en     = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_NAME')
      , dispatch_from_pincode      = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'PINCODE')
      , dispatch_from_country_code = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'COUNTRY')
    where request_id = g_request_id;

    update xxoyo_einv_trx_hdr_tbl
    set pos_state_code_orig = decode(invoice_process,
                                     'Intransit', recipient_state_code_orig
      , 'DTP Inter-state', nvl(ship_to_state_code, recipient_state_code_orig)
      , 'DTP Intra-state', recipient_state_code_orig
      )
      , pos_state_code_upd  = decode(invoice_process,
                                     'Intransit', recipient_state_code_orig
      , 'DTP Inter-state', nvl(ship_to_state_code, recipient_state_code_orig)
      , 'DTP Intra-state', recipient_state_code_orig
      )
      , pos_state_orig      = decode(invoice_process,
                                     'Intransit', recipient_state_orig
      , 'DTP Inter-state', nvl(ship_to_state, recipient_state_orig)
      , 'DTP Intra-state', recipient_state_orig
      )
      , pos_state_upd       = decode(invoice_process,
                                     'Intransit', recipient_state_orig
      , 'DTP Inter-state', nvl(ship_to_state, recipient_state_orig)
      , 'DTP Intra-state', recipient_state_orig
      )
    where request_id = g_request_id;

    update xxoyo_einv_trx_hdr_tbl
    set shipment_type = case
                          when supplier_state = pos_state_orig then
                            'INTRA'
                          when supplier_state <> pos_state_orig then
                            'INTER'
      end
    where request_id = g_request_id;


    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in xxoyo_einv_trx_hdr_tbl for Stock Transfer and transaction ids: ' || sql%rowcount
      , g_object);

    exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in update_mat_hdr_rec: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20907, 'Unexpected error in update_mat_hdr_rec' || g_sql_errm, true);
  end update_mat_hdr_rec;


  /******************************************************************************************************
  Component Type: Procedure
  Component Name: update_ar_hdr_rec
  ******************************************************************************************************/

  procedure update_ar_hdr_rec
  is
  begin
     update xxoyo_einv_trx_hdr_tbl
    set transaction_id             = xxoyo_einv_trx_id.nextval
      , creation_date              = sysdate
      , created_by                 = g_user_id
      , last_update_date           = sysdate
      , last_updated_by            = g_user_id
      , last_update_login          = userenv('SESSIONID')
      , status_code                = 'PENDING'
      , line_of_business           = xxoyo_einv_utility_pkg.get_line_of_business(org_id, 'OU')
      , supplier_country_code      = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'COUNTRY')
      , supplier_address1          = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'ADDRESS2')
      , supplier_place             = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'PLACE')
      , supplier_state_code        = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_NUMBER')
      , supplier_state             = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_CODE')
      , supplier_state_en          = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'STATE_NAME')
      , supplier_pincode           = xxoyo_einv_utility_pkg.get_location_details(supplier_location_id, 'PINCODE')
      , recipient_legal_name       = recipient_poc_name

      , recipient_address1         = nvl(
        xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'ADDRESS2'),
        xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'ADDRESS1'))

      , recipient_place            = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'PLACE')
      , recipient_state_code_orig  = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id,
                                                                                      'STATE_NUMBER')
      , recipient_state_code_upd   = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id,
                                                                                      'STATE_NUMBER')
      , recipient_state_orig       = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id,
                                                                                      'STATE_CODE')
      , recipient_state_upd        = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id,
                                                                                      'STATE_CODE')
      , recipient_state_en         = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id,
                                                                                      'STATE_NAME')
      , recipient_pincode_orig     = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'PINCODE')
      , recipient_pincode_upd      = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'PINCODE')
      , recipient_country_code     = xxoyo_einv_utility_pkg.get_cust_location_details(recipient_location_id, 'COUNTRY')
      , ship_to_address1           = nvl(
        xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'ADDRESS2'),
        xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'ADDRESS1'))
      , ship_to_place              = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'PLACE')
      , ship_to_state_code         = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id,
                                                                                      'STATE_NUMBER')
      , ship_to_state              = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'STATE_CODE')
      , ship_to_state_en           = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'STATE_NAME')
      , ship_to_pincode            = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'PINCODE')
      , ship_to_country_code       = xxoyo_einv_utility_pkg.get_cust_location_details(ship_to_location_id, 'COUNTRY')
      , dispatch_from_address1     = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'ADDRESS2')
      , dispatch_from_place        = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'PLACE')
      , dispatch_from_state_code   = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_NUMBER')
      , dispatch_from_state        = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_CODE')
      , dispatch_from_state_en     = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'STATE_NAME')
      , dispatch_from_pincode      = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'PINCODE')
      , dispatch_from_country_code = xxoyo_einv_utility_pkg.get_location_details(dispatch_location_id, 'COUNTRY')
    where request_id = g_request_id;

    update xxoyo_einv_trx_hdr_tbl xetht
    set pos_state_code_orig = ship_to_state_code
      , pos_state_code_upd  = ship_to_state_code
      , pos_state_orig      = ship_to_state
      , pos_state_upd       = ship_to_state
      , document_type_code  = (select meaning
                               from fnd_lookup_values
                               where lookup_type = 'XXOYO_EINV_AR_DOCUMENT_TYPES'
                                 and lookup_code = xetht.document_type_code
                                 and description = 'Document Type Code'
                                 and enabled_flag = 'Y')
    where request_id = g_request_id;

    update xxoyo_einv_trx_hdr_tbl
    set shipment_type = case
                          when supplier_state = pos_state_orig and
                               (attribute15 like '%SEZ%' or attribute15 like '%EXP%') then
                            'INTER'
                          when supplier_state = pos_state_orig then
                            'INTRA'
                          when supplier_state <> pos_state_orig then
                            'INTER'
      end
    where request_id = g_request_id;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated in xxoyo_einv_trx_hdr_tbl for AR and transaction ids: ' || sql%rowcount
      , g_object);
    exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in update_ar_hdr_rec: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20907, 'Unexpected error in update_ar_hdr_rec' || g_sql_errm, true);
  end update_ar_hdr_rec;


  /******************************************************************************************************
  Component Type: Procedure
  Component Name: insert_in_hdr_tbl
  ******************************************************************************************************/

  procedure insert_in_hdr_tbl
  is

  begin

    insert into xxoyo_einv_trx_hdr_tbl
    (supplier_country_code
    ,supplier_gstin
    ,supplier_legal_name
    ,supplier_location_id
    ,sez_unit
    ,recipient_gstin
    ,recipient_location_id
    ,recipient_phone_num
    ,recipient_email
    ,recipient_poc_name
    ,ship_to_legal_name
    ,ship_to_location_id
    ,dispatch_from_legal_name
    ,dispatch_location_id
    ,document_num
    ,document_date
    ,document_type_code
    ,document_id
    ,attribute15
    ,cust_account_id
    ,cust_acct_site_id
    ,cust_acct_site_bill_id
    ,cust_acct_site_ship_to
    ,party_id
    ,party_site_id
    ,preceding_doc_num
    ,preceding_doc_date
    ,preceding_doc_id
    ,is_service
    ,shipment_id
    ,shipment_num
    ,pos_state_code_orig
    ,pos_state_orig
    ,tax_scheme
    ,batch_id
    ,batch_source_id
    ,org_id
    ,owning_organization_id
    ,transfer_organization_id
    ,request_id
    ,invoice_process
    ,irn_eligible_flag
    ,mail_sent
    ,document_created_by
    )
      (
        select distinct
          supplier_country_code
             ,supplier_gstin
             ,supplier_legal_name
             ,supplier_location_id
             ,sez_unit
             ,recipient_gstin
             ,recipient_location_id
             ,recipient_phone_num
             ,recipient_email
             ,recipient_poc_name
             ,ship_to_legal_name
             ,ship_to_location_id
             ,dispatch_from_legal_name
             ,dispatch_location_id
             ,document_num
             ,to_char(trunc(document_date), 'RRRR-MM-DD')
             ,document_type_code
             ,document_id
             ,customer_type_code
             ,cust_account_id
             ,cust_acct_site_id
             ,cust_acct_site_bill_id
             ,cust_acct_site_ship_to
             ,party_id
             ,party_site_id
             ,preceding_doc_num
             ,to_char(trunc(preceding_doc_date), 'RRRR-MM-DD')
             ,preceding_doc_id
             ,is_service
             ,shipment_id
             ,shipment_num
             ,pos_state_code_orig
             ,pos_state_orig
             ,tax_scheme
             ,batch_id
             ,batch_source_id
             ,org_id
             ,owning_organization_id
             ,transfer_organization_id
             ,g_request_id
             ,query_type
             ,decode(query_type, 'DTP Intra-state', 'N', 'DTP Inter-state', 'Y', 'Intransit','Y','Y')
             ,'N' mail_sent
             ,document_created_by
        from xxoyo_einv_trx_extract_gt xeteg
      )
    ;


    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records inserted in xxoyo_einv_trx_hdr_tbl: ' || sql%rowcount
      , g_object);

            update xxoyo_einv_trx_hdr_tbl xetht
               set status_code = 'RE-EXTRACTED'
                  ,last_update_date = sysdate
                  ,last_updated_by = fnd_global.user_id
             where 1 = 1
               and status_code = 'ERROR'
               and request_id <> g_request_id
               and exists
                      (select 1
                         from xxoyo_einv_trx_hdr_tbl xetht1
                        where 1 = 1
                          and xetht.document_num = xetht1.document_num
                          and nvl(xetht.batch_id, 1) = nvl(xetht1.batch_id, 1)
                          and nvl(xetht.document_id, 1) = nvl(xetht1.document_id, 1)
                          and request_id = g_request_id
                      )
                ;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records updated for RE-EXTRACTED: ' || sql%rowcount
      , g_object);

    if g_process_name = g_mat_process then
      update_mat_hdr_rec;
    elsif g_process_name = g_ar_process then
      update_ar_hdr_rec;
    end if ;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in insert_in_hdr_tbl: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20907, 'Unexpected error in insert_in_hdr_tbl' || g_sql_errm, true);
  end insert_in_hdr_tbl;


  /******************************************************************************************************
  Component Type: Procedure
  Component Name: insert_in_gt_tbl
  ******************************************************************************************************/
  procedure insert_in_gt_tbl(p_einv_tab einv_tab)
  is

    v_count number := 0;

  begin

    for i in p_einv_tab.first .. p_einv_tab.last
      loop
        insert into xxoyo_einv_trx_extract_gt values p_einv_tab(i);
        v_count := v_count + 1;
      end loop;

    xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                      'Records inserted in xxoyo_einv_trx_extract_gt: ' || v_count
      , g_object);

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in insert_in_gt_tbl: ' || g_sql_errm
        , g_object);
      raise_application_error(-20902, 'Unexpected error in insert_in_gt_tbl' || g_sql_errm, true);
  end insert_in_gt_tbl;

  /******************************************************************************************************
  Component Type: Procedure
  Component Name: extract_mtl_trx_data
  ******************************************************************************************************/

  procedure extract_mtl_trx_data
  is


    v_einv_tab einv_tab;
    empty_set einv_tab;
    c_query_intransit varchar2(32767) := 'select /*+ leading(stg) */
                                                    ''Intransit'' query_type
                                                    ,null supplier_country_code
                                                    ,xxoyo_einv_utility_pkg.get_gstin(mmt.owning_organization_id) supplier_gstin
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(mmt.owning_organization_id,''IO'') supplier_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.owning_organization_id,''ORG_ID'') supplier_location_id
                                                    ,''false'' sez_unit
                                                    ,xxoyo_einv_utility_pkg.get_gstin(mmt.transfer_organization_id) recipient_gstin
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.transfer_organization_id,''ORG_ID'') recipient_location_id
                                                    ,stg.poc_no recipient_phone_num
                                                    ,''scm@oyorooms.com'' recipient_email
                                                    ,mmt.attribute2 recipient_poc_name
                                                    ,null ship_to_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.transfer_organization_id,''ORG_ID'') ship_to_location_id
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(mmt.owning_organization_id,''IO'') dispatch_from_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.owning_organization_id,''ORG_ID'') dispatch_location_id
                                                    ,nvl(mmt.actual_cost,0) * abs(mmt.transaction_quantity) item_base_amount
                                                    ,xxoyo_einv_utility_pkg.get_total_taxes_amount(mmt.original_transaction_temp_id,
                                                                                                    mmt.organization_id,
                                                                                                    nvl(mmt.transaction_batch_id,mmt.transaction_set_id)) item_tax_amount
                                                    ,nvl(mmt.actual_cost,0) * abs(mmt.transaction_quantity) +
                                                                                                    xxoyo_einv_utility_pkg.get_total_taxes_amount(mmt.original_transaction_temp_id,
                                                                                                    mmt.organization_id,
                                                                                                    nvl(mmt.transaction_batch_id,mmt.transaction_set_id)) item_gross_amount
                                                    ,msi.description item_name
                                                    ,msi.segment1 item_code
                                                    ,mmt.actual_cost item_price
                                                    ,abs(mmt.transaction_quantity) item_quantity
                                                    ,jtdfl.hsn_code hsn_or_sac_code
                                                    ,mmt.inventory_item_id inventory_item_id
                                                    ,jtype.tax_type_name tax_name
                                                    ,jaidet.tax_rate_percentage tax_rate
                                                    ,jaidet.unround_tax_amt_fun_curr taxable_amount
                                                    ,jaidet.tax_invoice_num document_num
                                                    ,stg.creation_date document_date
                                                    ,stg.created_by
                                                    ,''Regular'' document_type_code
                                                    ,null document_id
                                                    ,upper(xxoyo_einv_utility_pkg.get_first_party_reporting_code(mmt.transfer_organization_id))  customer_type_code
                                                    ,null CUST_ACCOUNT_ID
                                                    ,null CUST_ACCT_SITE_ID
                                                    ,null CUST_ACCT_SITE_BILL_ID
                                                    ,null CUST_ACCT_SITE_SHIP_TO
                                                    ,null PARTY_ID
                                                    ,null PARTY_SITE_ID
                                                    ,null preceding_doc_num
                                                    ,null PRECEDING_DOC_DATE
                                                    ,null PRECEDING_DOC_ID
                                                    ,null PRECEDING_line_ID
                                                    ,''Y'' is_service
                                                    ,mmt.attribute1 shipment_id
                                                    ,mmt.shipment_number shipment_num
                                                    ,null pos_state_code_orig
                                                    ,null pos_state_orig
                                                    ,''GST'' tax_scheme
                                                    ,mmt.attribute4 batch_id
                                                    ,null batch_source_id
                                                    ,xxoyo_einv_utility_pkg.get_org_id(mmt.owning_organization_id,''IO'') org_id
                                                    ,mmt.owning_organization_id owning_organization_id
                                                    ,mmt.transfer_organization_id transfer_organization_id
                                                    ,jtdfl.trx_id tax_factor_trx_id
                                                    ,jtdfl.trx_line_id tax_factor_trx_line_id
                                                    ,jtdfl.entity_code tax_factor_entity
                                                    ,jtdfl.det_factor_id tax_factor_det_id
                                                    ,jaidet.tax_line_id tax_lines_line_id
                                                    ,jaidet.det_factor_id tax_lines_det_factor_id
                                                    ,jaidet.trx_id tax_lines_trx_id
                                                    ,jaidet.trx_line_id tax_lines_trx_line_id
                                                    ,jaidet.entity_code tax_lines_entity
                                                    ,mmt.transaction_id mmt_transaction_id
                                                    ,jaidet.tax_type_id tax_lines_tax_type_id
                                                    from

                                                          mtl_material_transactions mmt

                                                          ,jai_tax_lines jaidet
                                                          ,jai_tax_det_fct_lines_v jtdfl
                                                          ,jai_tax_types jtype

                                                          ,mtl_system_items_b msi

                                                          ,xxoyo_po_ind_mo_tbl stg
                                                    where 1=1
                                                          --and mmt.attribute4 in (1327,1323)
                                                          and stg.creation_date > trunc(sysdate - 90)
                                                          and stg.creation_date >= to_date(''01-OCT-2020'',''DD-MON-RRRR'')
                                                          and nvl(stg.attribute12, ''Y'') = ''Y''

                                                          and mmt.transaction_batch_id = jtdfl.trx_id
                                                          and jtdfl.trx_line_id=mmt.original_transaction_temp_id
                                                          and jtdfl.entity_code=''MTL_TRANSACTION''

                                                          and jaidet.trx_line_id=mmt.original_transaction_temp_id
                                                          and jaidet.organization_id=mmt.organization_id
                                                          and nvl(mmt.transaction_batch_id,mmt.transaction_set_id) = jaidet.trx_id
                                                          and jaidet.entity_code=''MTL_TRANSACTION''
                                                          --and jaidet.trx_id = jtdfl.trx_id
                                                          and jaidet.det_factor_id = jtdfl.det_factor_id

                                                          and jaidet.tax_type_id =jtype.tax_type_id

                                                          and stg.tax_invoice_no=jaidet.tax_invoice_num
                                                          and stg.project is null
                                                          and stg.mo_header_id is null--Physical

                                                          and stg.item_id = mmt.inventory_item_id

                                                          and msi.inventory_item_id=mmt.inventory_item_id
                                                          and msi.organization_id=mmt.organization_id
                                                          ';

    c_query_dtp_inter varchar2(32767) := 'select  /*+ leading(stg) */ distinct
                                                    ''DTP Inter-state'' query_type
                                                    ,null supplier_country_code
                                                    ,xxoyo_einv_utility_pkg.get_gstin(mmt.owning_organization_id) supplier_gstin
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(mmt.owning_organization_id,''IO'') supplier_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.owning_organization_id,''ORG_ID'') supplier_location_id
                                                    ,''false'' sez_unit
                                                    ,xxoyo_einv_utility_pkg.get_gstin(mmt.transfer_organization_id) recipient_gstin
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.transfer_organization_id,''ORG_ID'') recipient_location_id
                                                    ,moh.attribute3 recipient_phone_num
                                                    ,''scm@oyorooms.com'' recipient_email
                                                    ,moh.attribute2 recipient_poc_name
                                                    ,null ship_to_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(moh.attribute6,''LOC_CODE'') ship_to_location_id
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(mmt.owning_organization_id,''IO'') dispatch_from_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(mmt.owning_organization_id,''ORG_ID'') dispatch_location_id
                                                    ,nvl(mmt.actual_cost,0) * ABS(mmt.transaction_quantity) item_base_amount
                                                    ,xxoyo_einv_utility_pkg.get_total_taxes_amount(mmt.original_transaction_temp_id,
                                                                        mmt.organization_id,
                                                                        nvl(mmt.transaction_batch_id,mmt.transaction_set_id)) item_tax_amount
                                                    ,nvl(mmt.actual_cost,0) * ABS(mmt.transaction_quantity) +
                                                    xxoyo_einv_utility_pkg.get_total_taxes_amount(mmt.original_transaction_temp_id,
                                                                                                  mmt.organization_id,
                                                                                                nvl(mmt.transaction_batch_id,mmt.transaction_set_id)) item_gross_amount
                                                    ,msi.description item_name
                                                    ,msi.segment1 item_code
                                                    ,mmt.actual_cost item_price
                                                    ,abs(mmt.transaction_quantity) item_quantity
                                                    ,jtdfl.hsn_code hsn_or_sac_code
                                                    ,msi.inventory_item_id inventory_item_id
                                                    ,jtype.tax_type_name tax_name
                                                    ,jaidet.tax_rate_percentage tax_rate
                                                    ,jaidet.unround_tax_amt_fun_curr taxable_amount
                                                    ,jaidet.tax_invoice_num document_num
                                                    ,stg.creation_date document_date
                                                    ,stg.created_by
                                                    ,''Regular'' document_type_code
                                                    ,null document_id
                                                    ,upper(xxoyo_einv_utility_pkg.get_first_party_reporting_code(mmt.transfer_organization_id))  customer_type_code
                                                    ,null CUST_ACCOUNT_ID
                                                    ,null CUST_ACCT_SITE_ID
                                                    ,null CUST_ACCT_SITE_BILL_ID
                                                    ,null CUST_ACCT_SITE_SHIP_TO
                                                    ,null PARTY_ID
                                                    ,null PARTY_SITE_ID
                                                    ,null preceding_doc_num
                                                    ,null PRECEDING_DOC_DATE
                                                    ,null PRECEDING_DOC_ID
                                                    ,null PRECEDING_line_ID
                                                    ,''Y'' is_service
                                                    ,moh.attribute1 shipment_id
                                                    ,mmt.shipment_number shipment_num
                                                    ,null pos_state_code_orig
                                                    ,null pos_state_orig
                                                    ,''GST'' tax_scheme
                                                    ,mmt.attribute4 batch_id
                                                    ,null batch_source_id
                                                    ,xxoyo_einv_utility_pkg.get_org_id(mmt.owning_organization_id,''IO'') org_id
                                                    ,mmt.owning_organization_id owning_organization_id
                                                    ,mmt.transfer_organization_id transfer_organization_id
                                                    ,jtdfl.trx_id tax_factor_trx_id
                                                    ,jtdfl.trx_line_id tax_factor_trx_line_id
                                                    ,jtdfl.entity_code tax_factor_entity
                                                    ,jtdfl.det_factor_id tax_factor_det_id
                                                    ,jaidet.tax_line_id tax_lines_line_id
                                                    ,jaidet.det_factor_id tax_lines_det_factor_id
                                                    ,jaidet.trx_id tax_lines_trx_id
                                                    ,jaidet.trx_line_id tax_lines_trx_line_id
                                                    ,jaidet.entity_code tax_lines_entity
                                                    ,mmt.transaction_id mmt_transaction_id
                                                    ,jaidet.tax_type_id tax_lines_tax_type_id
                                                    from
                                                            mtl_txn_request_headers moh

                                                            ,mtl_material_transactions mmt
                                                            ,mtl_system_items_b msi

                                                            ,jai_tax_lines jaidet
                                                            ,jai_tax_det_fct_lines_v jtdfl
                                                            ,jai_tax_types jtype

                                                            ,xxoyo_po_ind_mo_tbl stg
                                                    where   1=1
                                                            --and mmt.attribute4=72

                                                            and stg.creation_date > trunc(sysdate - 90)
                                                            and stg.creation_date >= to_date(''01-OCT-2020'',''DD-MON-RRRR'')
                                                            and nvl(stg.attribute12, ''Y'') = ''Y''

                                                            and mmt.transaction_batch_id = jtdfl.trx_id
                                                            and jtdfl.trx_line_id=mmt.original_transaction_temp_id
                                                            and jtdfl.entity_code=''MTL_TRANSACTION''

                                                            and jaidet.trx_line_id=mmt.original_transaction_temp_id
                                                            and jaidet.organization_id=mmt.organization_id
                                                            and nvl(mmt.transaction_batch_id,mmt.transaction_set_id) = jaidet.trx_id
                                                            and jaidet.entity_code=''MTL_TRANSACTION''
                                                            --and jaidet.trx_id = jtdfl.trx_id
                                                            and jaidet.det_factor_id = jtdfl.det_factor_id
                                                            and jaidet.tax_type_id =jtype.tax_type_id

                                                            and stg.tax_invoice_no=jaidet.tax_invoice_num
                                                            and moh.header_id=stg.mo_header_id


                                                            and stg.item_id = mmt.inventory_item_id

                                                            and msi.inventory_item_id=mmt.inventory_item_id
                                                            and msi.organization_id=mmt.organization_id
                                                            ';

    c_query_dtp_intra varchar2(32767) := 'select  /*+ leading(stg) */ distinct
                                                    ''DTP Intra-state'' query_type
                                                    ,null supplier_country_code
                                                    ,xxoyo_einv_utility_pkg.get_gstin(stg.from_warehouse_id) supplier_gstin
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(stg.from_warehouse_id,''IO'') supplier_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(stg.from_warehouse_id,''ORG_ID'') supplier_location_id
                                                    ,''false'' sez_unit
                                                    ,null recipient_gstin
                                                    ,xxoyo_einv_utility_pkg.get_location_id(moh.attribute6,''LOC_CODE'') recipient_location_id
                                                    ,moh.attribute3 recipient_phone_num
                                                    ,''scm@oyorooms.com'' recipient_email
                                                    ,moh.attribute2 recipient_poc_name
                                                    ,null ship_to_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(moh.attribute6,''LOC_CODE'') ship_to_location_id
                                                    ,xxoyo_einv_utility_pkg.get_legal_name(stg.from_warehouse_id,''IO'') dispatch_from_legal_name
                                                    ,xxoyo_einv_utility_pkg.get_location_id(stg.from_warehouse_id,''ORG_ID'') dispatch_location_id
                                                    ,nvl(stg.material_cost,0) * ABS(stg.quantity) item_base_amount
                                                    ,0 item_tax_amount
                                                    ,nvl(stg.material_cost,0) * ABS(stg.quantity)item_gross_amount
                                                    ,msi.description item_name
                                                    ,msi.segment1 item_code
                                                    ,stg.material_cost item_price
                                                    ,abs(stg.quantity) item_quantity
                                                    ,null hsn_or_sac_code
                                                    ,msi.inventory_item_id inventory_item_id
                                                    ,null tax_name
                                                    ,null tax_rate
                                                    ,null taxable_amount
                                                    ,stg.del_challan_num document_num
                                                    ,stg.creation_date document_date
                                                    ,stg.created_by
                                                    ,''Regular'' document_type_code
                                                    ,null document_id
                                                    ,null DOCUMENT_CATEGORY
                                                    ,null CUST_ACCOUNT_ID
                                                    ,null CUST_ACCT_SITE_ID
                                                    ,null CUST_ACCT_SITE_BILL_ID
                                                    ,null CUST_ACCT_SITE_SHIP_TO
                                                    ,null PARTY_ID
                                                    ,null PARTY_SITE_ID
                                                    ,null preceding_doc_num
                                                    ,null PRECEDING_DOC_DATE
                                                    ,null PRECEDING_DOC_ID
                                                    ,null PRECEDING_line_ID
                                                    ,''Y'' is_service
                                                    ,moh.attribute1 shipment_id
                                                    ,stg.shipment_number shipment_num
                                                    ,null pos_state_code_orig
                                                    ,null pos_state_orig
                                                    ,''GST'' tax_scheme
                                                    ,to_char(stg.batch_id) batch_id
                                                    ,null batch_source_id
                                                    ,xxoyo_einv_utility_pkg.get_org_id(stg.from_warehouse_id,''IO'') org_id
                                                    ,moh.organization_id owning_organization_id
                                                    ,null transfer_organization_id
                                                    ,null       tax_factor_trx_id
                                                    ,null       tax_factor_trx_line_id
                                                    ,null       tax_factor_entity
                                                    ,null       tax_factor_det_id
                                                    ,null       tax_lines_line_id
                                                    ,null       tax_lines_det_factor_id
                                                    ,null       tax_lines_trx_id
                                                    ,null       tax_lines_trx_line_id
                                                    ,null       tax_lines_entity
                                                    ,null        mmt_transaction_id --6459938503 --6909745872
                                                    --,mmt.transaction_id        mmt_transaction_id --6459938503 --6909745872
                                                    ,null       tax_lines_tax_type_id
                                                    from  mtl_txn_request_headers moh
                                                          ,mtl_system_items_b msi
                                                          ,xxoyo_po_ind_mo_tbl stg
                                                          --,mtl_material_transactions mmt --6459938503 --6909745872
                                                          --,mtl_txn_request_lines mtrl --6459938503 --6909745872
                                                    WHERE 1=1

                                                          and stg.creation_date > trunc(sysdate - 90)
                                                          and stg.creation_date >= to_date(''01-OCT-2020'',''DD-MON-RRRR'')
                                                          and nvl(stg.attribute12, ''Y'') = ''Y''

                                                          and moh.header_id=stg.mo_header_id

                                                          and stg.tax_invoice_no is null
                                                          and stg.del_challan_num is not null
                                                          and stg.to_warehouse_id is null


                                                          --and stg.mo_status = ''TRANSACTED''
                                                          and msi.inventory_item_id=stg.item_id
                                                          and msi.organization_id=stg.from_warehouse_id
                                                          --6459938503 --6909745872
                                                          --and moh.header_id = mtrl.header_id
                                                          --and mtrl.line_id = mmt.move_order_line_id
                                                          --and stg.mo_line_id=mtrl.line_id
                                                          ';

    c_query varchar2(32767);

    c_ref_cur sys_refcursor;

    v_query_type varchar2(20);

  begin

    for i in 1 .. 3
      loop
        if i = 1 then
          c_query := c_query_intransit;
          v_query_type := 'Intransit';
        elsif i = 2 then
          c_query := c_query_dtp_inter;
          v_query_type := 'DTP Inter-state';
        elsif i = 3 then
          c_query := c_query_dtp_intra;
          v_query_type := 'DTP Intra-state';
        end if;
        /*
        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                          'Started extraction for ' || v_query_type
          , g_object);
        */

        open c_ref_cur for c_query;
        fetch c_ref_cur bulk collect into v_einv_tab;
        close c_ref_cur;

        if v_einv_tab.count > 0 then
          insert_in_gt_tbl(v_einv_tab);
        else
          xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'No records extracted for ' || v_query_type
            , g_object);

        end if;

      end loop;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in extract_mtl_trx_data: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20903, 'Unexpected error in extract_mtl_trx_data' || g_sql_errm, true);

  end extract_mtl_trx_data;
  /******************************************************************************************************
  Component Type: Procedure
  Component Name: extract_ar_trx_data
  ******************************************************************************************************/

  procedure extract_ar_trx_data
  is


    v_einv_tab einv_tab;
    empty_set einv_tab;
    c_query_oe varchar2(32767) := 'SELECT /*+ leading(rcta) */
                                          rcta.interface_header_context                                                                      query_type
                                           , null supplier_country
                                           , xxoyo_einv_utility_pkg.get_gstin(ooh.ship_from_org_id)                        supplier_gstin
                                           , xxoyo_einv_utility_pkg.get_legal_name(ooh.ship_from_org_id, ''IO'')             supplier_legal_name
                                           , xxoyo_einv_utility_pkg.get_location_id(ooh.ship_from_org_id, ''ORG_ID'')        supplier_location_id
                                           , ''false''                                                                        sez_unit
                                           ,xxoyo_einv_utility_pkg.get_third_party_reg(ac.customer_id,rcta.bill_to_site_use_id,''GSTN'') recipient_gstin
                                            ,xxoyo_einv_utility_pkg.get_cust_location_id(rcta.bill_to_site_use_id, ''BILL_TO'') recipient_location_id
                                            ,ac.customer_number recipient_phone_num
                                           ,xxoyo_einv_utility_pkg.get_recipient_email(ac.customer_id) recipient_email --FACNT-1108
                                           ,ac.customer_name recipient_poc_name
                                           ,(select ac.customer_name from ar_customers where customer_id = rcta.ship_to_customer_id) ship_to_legal_name
                                           ,nvl(xxoyo_einv_utility_pkg.get_cust_location_id(rcta.ship_to_site_use_id, ''SHIP_TO'')
                                            ,xxoyo_einv_utility_pkg.get_cust_location_id(rcta.bill_to_site_use_id, ''BILL_TO'')) ship_to_location_id
                                           ,xxoyo_einv_utility_pkg.get_legal_name(ooh.ship_from_org_id, ''IO'')             dispatch_from_legal_name
                                           ,xxoyo_einv_utility_pkg.get_location_id(ooh.ship_from_org_id, ''ORG_ID'')        dispatch_from_location_id
                                           ,abs(ool.UNIT_SELLING_PRICE * ool.ordered_quantity)                                item_base_amount
                                           ,abs(xxoyo_einv_utility_pkg.get_total_taxes_amount_AR(ooh.header_id, ool.line_id,''OE_ORDER_HEADERS''))  item_tax_amount
                                           ,abs(xxoyo_einv_utility_pkg.get_total_taxes_amount_AR(ooh.header_id, ool.line_id,''OE_ORDER_HEADERS'') +
                                            (ool.UNIT_SELLING_PRICE * ool.ordered_quantity))                                item_gross_amount
                                           ,msib.description                                                               item_name
                                           ,msib.segment1                                                                  item_code
                                           ,abs(ool.unit_selling_price)                                                         item_price
                                           ,ool.ordered_quantity                                                           item_quantity
                                           ,nvl(jtdfl.hsn_code,jtdfl.sac_code)                                              hsn_or_sac_code
                                           ,msib.inventory_item_id                                                         inventory_item_id
                                           ,jtype.tax_type_name                                                            tax_name
                                           ,jaidet.tax_rate_percentage                                                     tax_rate
                                           ,abs(jaidet.unround_tax_amt_fun_curr)                                                taxable_amount
                                           ,rcta.TRX_NUMBER                                                                document_num
                                           ,rcta.TRX_DATE                                                                  document_date
                                           ,rcta.created_by
                                           ,rctta.type document_type_code
                                           ,rcta.customer_trx_id                                                           document_id
                                           ,upper(xxoyo_einv_utility_pkg.get_reporting_code(ac.customer_id,rcta.bill_to_site_use_id))  customer_type_code
                                           ,rcta.bill_to_customer_id cust_account_id
                                           ,(select cust_acct_site_id from hz_cust_site_uses_all where site_use_id = rcta.bill_to_site_use_id and site_use_code = ''BILL_TO'')  cust_acct_site_id
                                           ,rcta.bill_to_site_use_id cust_acct_site_bill_id
                                           ,rcta.ship_to_site_use_id cust_acct_site_ship_to
                                           ,null PARTY_ID
                                           ,null PARTY_SITE_ID
                                           ,jtdfl.original_tax_invoice_num preceding_doc_num
                                           ,jtdfl.original_tax_invoice_date preceding_doc_date
                                           ,(select customer_trx_id from ra_customer_trx_all where trx_number = jtdfl.original_tax_invoice_num) preceding_doc_id
                                           ,null preceding_line_id
                                           , null                                                      is_service
                                           , rcta.shipment_id                                                                           shipment_id
                                           , null                                                                           shipment_num
                                           , null                                                                           pos_state_code_orig
                                           , null                                                                           pos_state_orig
                                           , ''GST''                                                                          tax_scheme
                                           , rcta.batch_id                                                                           batch_id
                                           , rcta.batch_source_id batch_source_id
                                           , rcta.org_id                 org_id
                                           , ooh.ship_from_org_id                                                           owning_organization_id
                                           , ool.invoice_to_org_id                                                          transfer_organization_id
                                           , jtdfl.trx_id                                                                   tax_factor_trx_id
                                           , jtdfl.trx_line_id                                                              tax_factor_trx_line_id
                                           , jtdfl.entity_code                                                              tax_factor_entity
                                           , jtdfl.det_factor_id                                                            tax_factor_det_id
                                           , jaidet.tax_line_id                                                             tax_lines_line_id
                                           , jaidet.det_factor_id                                                           tax_lines_det_factor_id
                                           , jaidet.trx_id                                                                  tax_lines_trx_id
                                           , jaidet.trx_line_id                                                             tax_lines_trx_line_id
                                           , jaidet.entity_code                                                             tax_lines_entity
                                           , null                                                                           transaction_id
                                           , jaidet.tax_type_id                                                             tax_lines_tax_type_id


                                      FROM oe_order_headers_all ooh,
                                           oe_order_lines_all ool,
                                           ra_customer_trx_all rcta,
                                           ra_customer_trx_lines_all rctla,
                                           mtl_system_items_b msib,
                                           jai_tax_lines jaidet
                                          ,jai_tax_types jtype
                                          ,jai_tax_det_fct_lines_v jtdfl
                                          , RA_CUST_TRX_TYPES_ALL rctta
                                          ,ar_customers ac


                                      where 1 = 1
                                        and rcta.creation_date > sysdate - 90
                                        and rcta.creation_date >= to_date(''01-OCT-2020'',''DD-MON-RRRR'')
                                        and rcta.complete_flag = ''Y''
                                        and rcta.customer_trx_id = rctla.customer_trx_id
                                        and nvl(rcta.attribute12,''Y'') = ''Y''

                                        and rctla.interface_line_attribute1 = ooh.order_number

                                        and rcta.bill_to_customer_id = ac.customer_id

                                        and rcta.interface_header_context = ''ORDER ENTRY''
                                        and rctla.interface_line_context = ''ORDER ENTRY''

                                        and rcta.cust_trx_type_id = rctta.cust_trx_type_id
                                        and rcta.org_id = rctta.org_id
                                        and rctta.type in (select lookup_code from fnd_lookup_values
                                                            where lookup_type = ''XXOYO_EINV_AR_DOCUMENT_TYPES''
                                                              and description = ''Document Type Code'' and enabled_flag = ''Y'')

                                        and rctla.interface_line_attribute6 = ool.line_id
                                        and ooh.header_id = ool.header_id

                                        and msib.inventory_item_id = ool.inventory_item_id
                                        and msib.organization_id = ool.ship_from_org_id

                                        and jaidet.trx_id = ooh.header_id
                                        AND jaidet.trx_line_id = ool.line_id
                                        and jaidet.entity_code = ''OE_ORDER_HEADERS''
                                        and jaidet.tax_type_id = jtype.tax_type_id

                                        and jtdfl.trx_id = ooh.header_id
                                        AND jtdfl.trx_line_id = ool.line_id
                                        and jtdfl.entity_code = ''OE_ORDER_HEADERS''
                                                          ';

    c_query_ar_inv varchar2(32767) := 'select /*+ leading(rcta) */
                                        ''AR INVOICE'' query_type
                                      ,null supplier_country
                                      ,xxoyo_einv_utility_pkg.get_gstin(jtdfl.organization_id) supplier_gstin
                                      ,(select name from xle_entity_profiles where legal_entity_id = rcta.legal_entity_id) supplier_legal_name
                                      ,xxoyo_einv_utility_pkg.get_location_id(jtdfl.organization_id, ''ORG_ID'') supplier_location_id
                                      ,''false'' SEZ_UNIT
                                      ,xxoyo_einv_utility_pkg.get_third_party_reg(ac.customer_id,rcta.bill_to_site_use_id,''GSTN'') recipient_gstin
                                      ,xxoyo_einv_utility_pkg.get_cust_location_id(rcta.bill_to_site_use_id, ''BILL_TO'') recipient_location_id
                                      ,ac.customer_number recipient_phone_num
                                      ,xxoyo_einv_utility_pkg.get_recipient_email(ac.customer_id) recipient_email --FACNT-1108
                                      ,ac.customer_name recipient_poc_name
                                      ,(select ac.customer_name from ar_customers where customer_id = rcta.ship_to_customer_id) ship_to_legal_name
                                      ,nvl(xxoyo_einv_utility_pkg.get_cust_location_id(rcta.ship_to_site_use_id, ''SHIP_TO'')
                                        ,xxoyo_einv_utility_pkg.get_cust_location_id(rcta.bill_to_site_use_id, ''BILL_TO'')) ship_to_location_id
                                      ,(select name from xle_entity_profiles where legal_entity_id = rcta.legal_entity_id) dispatch_from_legal_name
                                      ,xxoyo_einv_utility_pkg.get_location_id(jtdfl.organization_id, ''ORG_ID'') dispatch_location_id
                                      ,abs((nvl(rctla.quantity_invoiced,rctla.quantity_credited)*(rctla.UNIT_SELLING_PRICE))) item_base_amount
                                      ,abs(xxoyo_einv_utility_pkg.get_total_taxes_amount_AR(rcta.customer_trx_id, rctLa.customer_trx_LINE_id ,''TRANSACTIONS'')) total_tax_amount
                                      ,abs(((nvl(rctla.quantity_invoiced,rctla.quantity_credited)*(rctla.UNIT_SELLING_PRICE))+xxoyo_einv_utility_pkg.get_total_taxes_amount_AR(rcta.customer_trx_id, rctLa.customer_trx_LINE_id ,''TRANSACTIONS''))) ITEM_GROSS_AMOUNT
                                      ,nvl(msib.description, rctla.description) item_name
                                      ,msib.segment1 item_code
                                      ,abs(rctla.unit_selling_price) item_price
                                      ,nvl(rctla.quantity_invoiced,rctla.quantity_credited) item_quantity
                                      ,nvl(jtdfl.hsn_code,jtdfl.sac_code) hsn_or_sac_code
                                      ,rctla.inventory_item_id inventory_item_id
                                      , jtype.tax_type_name    tax_name
                                      , jaidet.tax_rate_percentage   tax_rate
                                      , abs(jaidet.unround_tax_amt_fun_curr) taxable_amount
                                      , rcta.TRX_NUMBER                 document_num
                                      , rcta.TRX_DATE                   document_date
                                      , rcta.created_by document_created_by
                                      , rctta.type document_type_code
                                      , rcta.customer_trx_id document_id
                                      ,upper(xxoyo_einv_utility_pkg.get_reporting_code(ac.customer_id,rcta.bill_to_site_use_id))  customer_type_code
                                      ,rcta.bill_to_customer_id cust_account_id
                                      ,(select cust_acct_site_id from hz_cust_site_uses_all where site_use_id = rcta.bill_to_site_use_id and site_use_code = ''BILL_TO'')  CUST_ACCT_SITE_ID
                                      ,rcta.bill_to_site_use_id cust_acct_site_bill_id
                                      ,rcta.ship_to_site_use_id cust_acct_site_ship_to
                                      ,null PARTY_ID
                                      ,null PARTY_SITE_ID
                                      ,jtdfl.original_tax_invoice_num preceding_doc_num
                                      ,jtdfl.original_tax_invoice_date preceding_doc_date
                                      ,(select customer_trx_id from ra_customer_trx_all where trx_number = jtdfl.original_tax_invoice_num) preceding_doc_id
                                      ,null preceding_line_id
                                      , null  is_service
                                      , rcta.shipment_id     shipment_id
                                      , null                 shipment_num
                                      , null                 pos_state_code_orig
                                      , null                 pos_state_orig
                                      , ''GST''                tax_scheme
                                      , rcta.batch_id        batch_id
                                      ,rcta.batch_source_id batch_source_id
                                      , rcta.org_id                 org_id
                                      , jtdfl.organization_id     owning_organization_id
                                      , null transfer_organization_id
                                      , jtdfl.trx_id               tax_factor_trx_id
                                      , jtdfl.trx_line_id          tax_factor_trx_line_id
                                      , jtdfl.entity_code          tax_factor_entity
                                      , jtdfl.det_factor_id        tax_factor_det_id
                                      , jaidet.tax_line_id         tax_lines_line_id
                                      , jaidet.det_factor_id       tax_lines_det_factor_id
                                      , jaidet.trx_id              tax_lines_trx_id
                                      , jaidet.trx_line_id         tax_lines_trx_line_id
                                      , jaidet.entity_code         tax_lines_entity
                                      , null                       transaction_id
                                      , jaidet.tax_type_id         tax_lines_tax_type_id

                                      FROM
                                      ar_customers ac,
                                      ra_customer_trx_all rcta,
                                      ra_customer_trx_lines_all rctla,
                                      jai_tax_det_fct_lines_v jtdfl,
                                      jai_tax_lines jaidet,
                                      ra_cust_trx_types_all rctta,
                                      mtl_system_items_b msib,
                                      jai_tax_types jtype
                                      WHERE     1 = 1
                                      and rcta.creation_date > sysdate - 90
                                      and rcta.creation_date >= to_date(''01-OCT-2020'',''DD-MON-RRRR'')
                                      AND rcta.complete_flag = ''Y''
                                      AND rcta.customer_trx_id = rctla.customer_trx_id
                                      and nvl(rcta.attribute12,''Y'') = ''Y''

                                      AND rcta.bill_to_customer_id = ac.customer_id

                                      and nvl(rctla.interface_line_context,''XXXXXX'') <> ''ORDER ENTRY''
                                      AND rctla.line_type = ''LINE''

                                      and rctla.inventory_item_id = msib.inventory_item_id(+)
                                      and rctla.warehouse_id = msib.organization_id (+)

                                      AND rcta.cust_trx_type_id = rctta.cust_trx_type_id
                                      AND rcta.org_id = rctta.org_id
                                      and rctta.type in (select lookup_code from fnd_lookup_values
                                                          where lookup_type = ''XXOYO_EINV_AR_DOCUMENT_TYPES''
                                                            and description = ''Document Type Code'' and enabled_flag = ''Y'')

                                      and jaidet.trx_id = rcta.customer_trx_id
                                      AND jaidet.trx_line_id = rctLa.customer_trx_LINE_id
                                      and jaidet.entity_code = ''TRANSACTIONS''
                                      and jaidet.tax_type_id = jtype.tax_type_id

                                      AND jtdfl.trx_id = rcta.customer_trx_id
                                      AND jtdfl.trx_line_id = rctLa.customer_trx_LINE_id
                                      AND jtdfl.org_id = rcta.org_id
                                      AND jtdfl.entity_code = ''TRANSACTIONS''
                                      ';

    c_query varchar2(32767);

    c_ref_cur sys_refcursor;

    v_query_type varchar2(20) ;

  begin

    for i in 1..2
      loop
        if i = 1 then
          v_query_type := 'OE-AR-Invoices';
          c_query := c_query_oe;
        elsif i = 2 then
          v_query_type := 'AR-Invoices';
          c_query := c_query_ar_inv;
        end if;
        /*
        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                          'Started extraction for ' || v_query_type
          , g_object);
        */

        open c_ref_cur for c_query;
        fetch c_ref_cur bulk collect into v_einv_tab;
        close c_ref_cur;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value3, g_debug_level,
                          'Fetched data for ' || v_query_type || ' : ' || v_einv_tab.count
          , g_object);

        if v_einv_tab.count > 0 then
          insert_in_gt_tbl(v_einv_tab);
        else
          xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'No records extracted for ' || v_query_type
            , g_object);

        end if;
      end loop;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in extract_ar_trx_data: ' || g_sql_errm
        , g_object);
      if g_generate_report = 'N' then
        xxoyo_einv_report_pkg.generate_report(p_request_id => g_request_id, p_mode => 'REQUEST_ID');
        g_generate_report := 'Y';
      end if;
      raise_application_error(-20903, 'Unexpected error in extract_ar_trx_data' || g_sql_errm, true);

  end extract_ar_trx_data;


  /******************************************************************************************************
  Component Type: Procedure
  Component Name: initialize
  ******************************************************************************************************/
  procedure initialize
  is

  begin

    select distinct master_organization_id into g_master_org_id
    from mtl_parameters;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in initialize: ' || g_sql_errm
        , g_object);
      raise_application_error(-20911, 'Unexpected error in initialize' || g_sql_errm, true);
  end initialize;


  /******************************************************************************************************
  Component Type: Procedure
  Component Name: main_proc
  ******************************************************************************************************/
  procedure main_proc(p_errbuff out varchar2,
                      p_retcode out varchar2,
                      p_mode in varchar2)
  is

  begin


    g_process_name := p_mode;

    if p_mode = g_mat_process then

      initialize;
      extract_mtl_trx_data;
      insert_in_hdr_tbl;
      reconcile_header_tbl;
      insert_in_lines_tbl;
      reconcile_mat_lines;
      insert_in_taxes_tbl;
      reconcile_mat_taxes;
      categorize_data;
      validate_mat_data;
      update_ind_mo_tbl;
      --archive_custom_tbl;
    elsif p_mode = g_ar_process then
      extract_ar_trx_data;
      insert_in_hdr_tbl;
      reconcile_header_tbl;
      insert_in_lines_tbl;
      insert_in_taxes_tbl;
      categorize_data;
      validate_ar_data;
      update_ra_customer_all_tbl;
    end if;

    gen_b2c_qr_code;
    set_completion_msg;
    p_errbuff := g_errbuff;
    p_retcode := g_retcode;

  exception
    when others then
      g_sql_errm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                        'Unexpected error in main_proc: ' || g_sql_errm
        , g_object);
      raise_application_error(-20901, 'Unexpected error in main_proc' || g_sql_errm, true);
  end main_proc;

end xxoyo_einv_trx_extract_pkg;
/
