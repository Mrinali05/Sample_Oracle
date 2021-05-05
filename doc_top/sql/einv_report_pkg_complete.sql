create or replace package body xxoyo_einv_report_pkg
as
/******************************************************************************************************

Component Type: Package
Component Name: xxoyo_einv_report_pkg
Description: This package inputs the parameters from the xml report,processes it and stores the data in
             GT table based on the parameters.

Version Matrix

  Version     Author                Date               Description
   1.0        Mrinali Verma         18th Sept '20      As per FACNT-959 OracleERP -OracleERP - PR/PO
                                                       Consumption timing; Closure & Expiry


******************************************************************************************************/

  g_ar_process varchar2(20)          := xxoyo_einv_utility_pkg.g_ar_process;
  g_mat_process varchar2(20)         := xxoyo_einv_utility_pkg.g_mat_process;
  g_debug_level         number       := nvl(to_number(fnd_profile.value('XXOYO_DEBUG_LEVEL')),0);
  g_debug_mode          varchar2(10) := 'FILE';
  g_debug_log           varchar2(10) := 'LOG';
  g_debug_output        varchar2(10) := 'OUTPUT';
  g_object              varchar2(50) := 'XXOYO_EINV_REPORT_PKG' || fnd_global.conc_request_id;
  g_debug_value0        number       := 0;
  g_debug_value1        number       := 1;
  g_debug_value2        number       := 2;
  g_debug_value3        number       := 3;
  g_sqlerrm             varchar2(240);
  g_file_name           varchar2(50) := 'XXOYO_EINV_GNRT_RPT_'||fnd_global.conc_request_id||'.pcl';
  g_file_type           utl_file.file_type:= null;
  g_warning_flag        varchar2(1) := 'N';
  g_ret_code            varchar2(1);
  g_footer              varchar2(2000);

  type invoice_tab_type is table of varchar2(30);


/******************************************************************************************************

Component Type: Function
Component Name: insert_mat_inv_params_gt
Description: This function inserts data in the GT table based on the parameters passed by the xml report

******************************************************************************************************/

  function insert_mat_inv_params_gt (p_process varchar2
                                    ,p_mode varchar2
                                    ,p_invoice_tab invoice_tab_type)
  return boolean

  is

    v_inserted_count number := 0;
    del_count number :=0;
    v_count number:=0;

  begin


    if p_invoice_tab.count <= 0 then
          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                            'Extracted count is less than or equal to 0'
                            ,g_object);
      return false;
    end if;

    for i in p_invoice_tab.first .. p_invoice_tab.last loop


      insert into xxoyo_einv_report_params_gt (invoice_process
                                              ,organization_id
                                              ,document_num
                                              ,batch_id
                                              ,transaction_id
                                              )
                                       select invoice_process
                                              ,org_id
                                              ,document_num
                                              ,batch_id
                                              ,transaction_id
                                              from xxoyo_einv_trx_hdr_tbl xetht
                                              where 1 = 1
                                                and (case
                                                       when p_mode like '%Invoice%' then
                                                         xetht.document_num
                                                       when p_mode like '%Batch%' then
                                                         to_char(xetht.batch_id)
                                                     end) = (case
                                                       when p_mode like '%Invoice%' then
                                                         p_invoice_tab(i)
                                                       when p_mode like '%Batch%' then
                                                         p_invoice_tab(i)
                                                     end)
                                                and xetht.status_code = 'SUCCESS'
                                                and xetht.irn_eligible_flag = 'Y'
                                                and xetht.invoice_process in (select meaning from fnd_lookup_values
                                                    where description=p_process and lookup_type='XXOYO_EINV_INVOICE_PROCESSES')

                                                ;

        v_inserted_count  := v_inserted_count + sql%rowcount;

    end loop;

    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                      'Header level records inserted in xxoyo_einv_report_params_gt: ' || v_inserted_count
                      ,g_object);

    if v_inserted_count = 0 then
      xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'No eligible records extracted for printing the invoice: ' || v_inserted_count
                        ,g_object);
      return false;

      else
      return true;
    end if;



  exception
    when others then
        g_sqlerrm := substr(sqlerrm, 1, 240);
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Unexpected error in insert_mat_inv_params_gt: ' || g_sqlerrm
                          ,g_object);
      return false;
  end insert_mat_inv_params_gt;


/******************************************************************************************************

Component Type: Function
Component Name: separate_string
Description: This function seperates the comma seperated parameters and saves it in a table type
             variable

******************************************************************************************************/


  function separate_string(p_string varchar2
                          ,p_invoice_tab out invoice_tab_type
                          )
  return boolean

  is

  begin

    select trim(regexp_substr(p_string, '[^,]+', 1, level)) bulk collect
      into p_invoice_tab
      from dual
   connect by level <= regexp_count(p_string, ',')+1;

    return true;

  exception
    when others then
        g_sqlerrm := substr(sqlerrm, 1, 240);
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Unexpected error in separate_string: ' || g_sqlerrm
                          ,g_object);

      return false;
  end separate_string;

/******************************************************************************************************

Component Type: Procedure
Component Name: print_error_details
Description: Print errored record details

******************************************************************************************************/

procedure print_error_details
is

  cursor error_details_cur is (select distinct document_num
                                 from xxoyo_einv_report_params_gt xerpg
                                where 1 = 1
                                  and not exists
                                     (select 1
                                        from xxoyo_einv_report_details_gt xerdg
                                       where 1 = 1
                                         and xerpg.document_num = xerdg.document_num
                                         and xerpg.batch_id = xerdg.batch_id
                                     )
                                     );

begin

    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                    'List of invoices for which line level extraction was not successful: '
                    ,g_object);

    for i in error_details_cur loop

      xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                      i.document_num
                      ,g_object);

    end loop;

exception
  when others then
    g_sqlerrm := substr(sqlerrm,1,240);
    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                    'Unexpected error in print_error_details '|| g_sqlerrm
                    ,g_object);

end print_error_details;

/******************************************************************************************************

Component Type: Procedure
Component Name: reconcile_data
Description: Recondile line level data for extracted header level documents

******************************************************************************************************/

procedure reconcile_data
is

    v_discrepancy_count number := 0;

begin

    select count(distinct document_num)
      into v_discrepancy_count
      from xxoyo_einv_report_params_gt xerpg
     where 1 = 1
       and not exists
          (select 1
             from xxoyo_einv_report_details_gt xerdg
            where 1 = 1
              and xerpg.document_num = xerdg.document_num
              and xerpg.batch_id = xerdg.batch_id
          )
          ;

    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                    'Documents for which line/taxes level data is not extracted fine: '|| v_discrepancy_count
                    ,g_object);

    if v_discrepancy_count > 0 then
      g_warning_flag := 'Y';
    end if;

exception
  when others then
    g_sqlerrm := substr(sqlerrm,1,240);
    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                    'Unexpected error in reconcile_data '|| g_sqlerrm
                    ,g_object);

end reconcile_data;

/******************************************************************************************************

Component Type: Function
Component Name: extract_einv_parameters
Description: This function inputs parameters from the xml report concurrent program ,validates it and
             passes it to the seperate_string function

******************************************************************************************************/


  --Function called by pre report trigger
  function extract_einv_parameters(p_process varchar2
                                  ,p_organization_id number
                                  ,p_mode varchar2
                                  ,p_invoice_no varchar2
                                  ,p_mult_invoices varchar2
                                  ,p_batch_id varchar2
                                  ,p_mult_batches varchar2
                                  )
  return boolean

  is

      v_string varchar2(4000);
      v_invoice_tab invoice_tab_type;

  begin

      if p_process = g_ar_process then
       g_footer := null;
       elsif p_process = g_mat_process then
       g_footer := 'Tax payable under reverse charge: NO'||chr(10)||
                   'Please note that this is only for stock transfer. The items specified are only for self consumptions and NOT FOR SALE';
      end if;

   --   if p_process = g_mat_process then

        if p_mode = 'Single Invoice' then

          v_string :=  p_invoice_no;

        elsif p_mode = 'Multiple Invoices' then

          v_string := p_mult_invoices;

          if  (regexp_like(v_string, '[^0-9a-z, ]','i' ))

              then
              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Invoice List is incorrect: ' || v_string
                          ,g_object);
              return false;

              else
              v_string := rtrim(replace(trim(v_string),' '),',');
              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Invoice List extracted: '|| v_string
                          ,g_object);

          end if;

        elsif p_mode = 'Single Batch' then

          v_string := p_batch_id;

        elsif p_mode = 'Multiple Batches' then

          v_string := p_mult_batches;

          if  (regexp_like(v_string, '[^0-9, ]' ))

              then
              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Batch list is incorrect: ' || v_string
                          ,g_object);
              return false;

              else
              v_string := rtrim(replace(trim(v_string),' '),',');
              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Batch List extracted: '||  v_string
                          ,g_object);

          end if;

        end if;

    --  end if;


      if not separate_string(v_string, v_invoice_tab) then
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Error in separate_string function'
                          ,g_object);
        return false;

      elsif not insert_mat_inv_params_gt(p_process,p_mode,v_invoice_tab) then
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Could not insert data in table xxoyo_einv_report_params_gt via function insert_mat_inv_params_gt'
                          ,g_object);
        return false;

      end if;

      if not extract_einv_details(p_process) then
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Error while extracing data using extract_einv_details'
                          ,g_object);
        return false;
      end if;

      return true;

  exception
    when others then
      g_sqlerrm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Error in extract_einv_parameters: ' || g_sqlerrm
                        ,g_object);
      return false;
  end extract_einv_parameters;


/******************************************************************************************************

Component Type: Function
Component Name: insert_mat_inv_details_gt
Description: This function extracts data and stores it in insert_mat_inv_details_gt table

******************************************************************************************************/


 function insert_mat_inv_details_gt
  return boolean

  is

    v_inserted_count number := 0;
    v_static_str varchar2(50) := 'data:image/png;base64,';

  begin

    insert into xxoyo_einv_report_details_gt
    select
           --xetht.signed_qr_code qr_code
           substr(replace(xetht.signed_qr_code,v_static_str),1,4000) qr_code1
          ,substr(replace(xetht.signed_qr_code,v_static_str),4001)   qr_code2
          ,xetht.irn
          ,xetht.ack_number
          ,xetht.ack_date

          ,xetht.document_num
          ,xetht.document_date
          ,xetht.shipment_id
          ,xetht.shipment_num

          ,xetht.document_type_code invoice_type
          ,(xetht.pos_state_upd||'('||xetht.pos_state_code_upd||')') place_of_supply
          ,xetht.eway_bill_number
          ,xetht.supply_type_code supply_type

          ,(xetht.supplier_legal_name||','||xetht.supplier_address1||','||xetht.supplier_place||','||xetht.supplier_state_en||','||xetht.supplier_pincode)  bill_from
          ,xetht.supplier_gstin
          ,xetht.supplier_state_code

          ,(xetht.recipient_legal_name||','||xetht.recipient_address1||','||xetht.recipient_place||','||xetht.recipient_pincode_upd) bill_to
          ,xetht.recipient_gstin
          ,xetht.recipient_state_code_upd

          ,(xetht.dispatch_from_legal_name||','||xetht.dispatch_from_address1||','||xetht.dispatch_from_place||','||xetht.dispatch_from_pincode) ship_from
          ,xetht.dispatch_from_state_code

          ,(xetht.ship_to_legal_name||','||xetht.ship_to_address1||','||xetht.ship_to_place||','||xetht.ship_to_pincode)ship_to
          ,xetht.recipient_poc_name
          ,xetht.recipient_phone_num
          ,xetht.recipient_email
          ,xetht.ship_to_state_code


          ,round(xetht.total_base_amount,2) base_amount
          ,round(xetht.total_tax_amount,2) tax_amount
          ,round(xetht.gross_amount,2) total_amount

          ,xetlt.item_code
          ,xetlt.item_name
          ,xetlt.hsn_or_sac_code hsn
          ,xetlt.item_quantity quantity

          ,round(xetlt.item_base_amount,2) rate
          ,round(xetlt.item_tax_amount,2)
          ,round(xetlt.item_gross_amount,2) amount

         ,(xettt.tax_name||'-'||xettt.tax_rate) tax_code
          ,xetht.batch_id
          ,g_footer invoice_process

          from
          xxoyo_einv_trx_hdr_tbl xetht,
          xxoyo_einv_trx_lines_tbl xetlt,
          xxoyo_einv_trx_taxes_tbl xettt,
          xxoyo_einv_report_params_gt xerpg
          where
          1=1
          and xetht.transaction_id = xetlt.transaction_id
          and xetlt.transaction_id = xettt.transaction_id
          and xetlt.line_number = xettt.item_line_number
        --  and xetht.batch_id = xerpg.batch_id
        --  and xetht.document_num = xerpg.document_num
          and xetht.transaction_id = xerpg.transaction_id
        --  xetht.document_num=''
          and xetht.status_code = 'SUCCESS'
          and xetht.irn_eligible_flag = 'Y'
          ;

    v_inserted_count :=  sql%rowcount;

    xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                    'Line and Tax level records inserted in xxoyo_einv_report_details_gt: '|| v_inserted_count
                    ,g_object);



    reconcile_data;

    if g_warning_flag = 'Y' then
      print_error_details;
    end if;

    return true;

  exception
    when others then
      g_sqlerrm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Error in insert_mat_inv_details_gt: ' || g_sqlerrm
                        ,g_object);
      return false;
  end insert_mat_inv_details_gt;


/******************************************************************************************************

Component Type: Function
Component Name: extract_einv_details
Description: This function checks which process is requested by the user

******************************************************************************************************/


  function extract_einv_details(p_process varchar2)
  return boolean

  is

  begin

  --  if p_process = g_ar_process then
  --    null;
  --  end if;

   -- if p_process = g_mat_process then
      if not insert_mat_inv_details_gt then
        return false;
      end if;
 --   end if;

  return true;

  exception
    when others then
      g_sqlerrm := substr(sqlerrm, 1, 240);
      xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Error in extract_einv_details: ' || g_sqlerrm
                        ,g_object);
      return false;
  end extract_einv_details;

/******************************************************************************************************

Component Type: Procedure
Component Name: generate_report
Description: This procedure generate report depending on the parama passed

******************************************************************************************************/

     procedure generate_report(
                              p_from_date  date default null
                             ,p_to_date date default null
                             ,p_status  varchar2 default null
                             ,p_request_id number default null
                             ,p_mode varchar2
                             ,p_type varchar2 default null
                              )
    is

        v_to_date               date ;
        v_from_date             date ;
        v_delim                 varchar(1) := '~';

        v_mail_sent_status      boolean;

        v_date_format           varchar2(30):= 'DD-MON-RRRR HH24:MI:SS';


        type report_data_type is record (transaction_id number,invoice_process varchar2(50),supplier_legal_name varchar2(240),ou_name VARCHAR2(240),owning_organization_name varchar(240),supplier_gstin varchar2(100),
        supplier_state_en varchar(100),batch_id number,document_date varchar2(30),document_num varchar2(50),gross_amount number,creation_date varchar2(30),
        created_by varchar2(50),recipient_legal_name varchar2(240),transfer_organization_name varchar(240),recipient_address1 varchar2(240),recipient_state_en varchar2(100)
        ,recipient_gstin varchar2(100),status_code varchar2(30),status_message varchar2(4000),irp_error_date varchar2(2000),irp_error_message varchar2(4000),irn varchar2(240)
        ,irn_creation_date varchar2(30),supply_type_code varchar2(50),ack_number varchar2(50),ack_date varchar2(50),request_id number);

        type report_tab is table of report_data_type index by binary_integer;

        v_report_tab report_tab;

        report_ref_cur sys_refcursor;

        v_query varchar2(32767);

        v_status varchar2(20);

        v_select varchar2(32767) := 'select
                                  transaction_id
                                  ,invoice_process
                                  ,supplier_legal_name
                                  ,ou_name
                                  ,owning_organization_name
                                  ,supplier_gstin
                                  ,supplier_state_en
                                  ,batch_id
                                  ,document_date
                                  ,document_num
                                  ,gross_amount
                                  ,creation_date
                                  ,created_by
                                  ,recipient_legal_name
                                  ,transfer_organization_name
                                  ,recipient_address1
                                  ,recipient_state_en
                                  ,recipient_gstin
                                  ,status_code
                                  ,status_message
                                  ,case
                                      when status_code like ''SUCCESS%'' then
                                        ''-''
                                      when status_code = ''PENDING'' then
                                        ''-''
                                      when status_code in (''RUNNING'',''ERROR'') then
                          substr( replace(  replace(listagg((error_creation_date),''####'')
                                                      within group (order by transaction_id, creation_date)
                                                ,chr(10)
                                              )
                                ,chr(13)
                              )
                              ,1,4000)
                                   end as IRP_error_date
                                  ,case
                                      when status_code like ''SUCCESS%'' then
                                        ''-''
                                      when status_code = ''PENDING'' then
                                        ''-''
                                      when status_code in (''RUNNING'',''ERROR'') then
                        substr(   replace(  replace(listagg((phase||'' - ''||error_messages),''####'')
                                                      within group (order by transaction_id, creation_date)
                                                ,chr(10)
                                                )
                                ,chr(13)
                                )
                            ,1,4000)
                                   end as IRP_error
                                  ,irn
                                  ,irn_created_at
                                  ,supply_type_code
                                  ,ack_number
                                  ,ack_date
                                  ,request_id
                                  from
                                  (select
                                          xetht.transaction_id
                                         ,xetht.invoice_process
                                         ,xetht.supplier_legal_name
                                         ,(select name from hr_operating_units where organization_id = xetht.org_id) ou_name
                                         ,(select organization_name from org_organization_definitions where organization_id = xetht.owning_organization_id) owning_organization_name
                                         ,xetht.supplier_gstin
                                         ,xetht.supplier_state_en
                                         ,xetht.batch_id
                                         ,xetht.document_date
                                         ,xetht.document_num
                                         ,xetht.gross_amount
                                         ,to_char(xetht.creation_date,''DD-MON-RRRR HH24:MI:SS'') creation_date
                                         ,(select user_name from fnd_user where user_id = xetht.document_created_by) created_by
                                         ,xetht.recipient_legal_name
                                         ,(select organization_name from org_organization_definitions where organization_id = xetht.transfer_organization_id) transfer_organization_name
                                         ,xetht.recipient_address1
                                         ,xetht.recipient_state_en
                                         ,xetht.recipient_gstin
                                         ,xetht.status_code
                                         ,xetht.status_message
                                         ,xetet.phase
                                         ,case
                                            when xetet.error_type like ''Unexpected Error%'' then
                                              ''Unexpected Error - Contact tech support''
                                            else
                                              substr(xetet.error_message,1,100)
                                          end error_messages
                                         ,to_char(xetht.last_update_date,''DD-MON-RRRR HH24:MI:SS'')  error_creation_date
                                         ,xetht.irn
                                         ,xetht.irn_created_at
                                         ,xetht.supply_type_code
                                         ,xetht.ack_number
                                         ,xetht.ack_date
                                         ,xetht.request_id
                                   from xxoyo_einv_trx_hdr_tbl xetht
                                        ,(select distinct phase, error_message,transaction_id, error_type from xxoyo_einv_trx_err_tbl) xetet
                                   where 1=1
                                     and xetht.transaction_id = xetet.transaction_id(+)
                                     --and xetht.request_id = xetet.request_id(+)
                                     and xetht.status_code <> ''RE-EXTRACTED''
                                     '
                                     ;

        v_where_request varchar2(1000) := 'and xetht.request_id = :p_request_id
                                          ';

        v_where_status varchar2(1000) := 'and xetht.status_code = nvl(:p_status,xetht.status_code)
                                          ';

        v_where_ar_invoice_process varchar(1000) := 'and upper(xetht.invoice_process) in (select lookup_code
                                                    from fnd_lookup_values
                                                    where 1=1
                                                    and description = :g_ar_process
                                                    and lookup_type = ''XXOYO_EINV_INVOICE_PROCESSES''
                                                    and enabled_flag = ''Y'')'   ;

        v_where_mat_invoice_process varchar(1000) := 'and upper(xetht.invoice_process) in (select lookup_code
                                                    from fnd_lookup_values
                                                    where 1=1
                                                    and description = :g_mat_process
                                                    and lookup_type = ''XXOYO_EINV_INVOICE_PROCESSES''
                                                    and enabled_flag = ''Y'')'    ;

        v_where_error_alert varchar2(1000) := 'and xetht.mail_sent = ''N'' ';

        v_where_date varchar2(1000) := 'and xetht.last_update_date between :v_from_date and :v_to_date
                                        ';

        v_group_by varchar2(4000)  :=   ')group by
                                        ( transaction_id
                                          ,invoice_process
                                          ,supplier_legal_name
                                          ,ou_name
                                          ,owning_organization_name
                                          ,supplier_gstin
                                          ,supplier_state_en
                                          ,batch_id
                                          ,document_date
                                          ,document_num
                                          ,gross_amount
                                          ,creation_date
                                          ,created_by
                                          ,recipient_legal_name
                                          ,transfer_organization_name
                                          ,recipient_address1
                                          ,recipient_state_en
                                          ,recipient_gstin
                                          ,status_code
                                          ,status_message
                                          ,irn
                                          ,irn_created_at
                                          ,supply_type_code
                                          ,ack_number
                                          ,ack_date
                                          ,request_id)
                                        ';

    begin

        v_from_date :=  to_date(to_char(nvl(p_from_date, sysdate - 1), 'DD-MON-RRRR') || '00:00:00', v_date_format );
        v_to_date := to_date(to_char(nvl(p_to_date, sysdate), 'DD-MON-RRRR') || '23:59:59', v_date_format );

        if p_mode = 'ALERT' then

           xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value2,g_debug_level,
                        'Generating Alert for the status '
                        ||nvl(p_status,'All')
                        ||' and type '
                        ||p_type
                        ,g_object);

          if p_type = g_mat_process then

            v_query := v_select||v_where_mat_invoice_process||v_where_error_alert||v_where_status||v_group_by;
            open report_ref_cur for v_query using g_mat_process, p_status;
               fetch report_ref_cur bulk collect into v_report_tab;
            close report_ref_cur;
          else
            v_query := v_select||v_where_ar_invoice_process||v_where_error_alert||v_where_status||v_group_by;

            open report_ref_cur for v_query using g_ar_process, p_status;
               fetch report_ref_cur bulk collect into v_report_tab;
            close report_ref_cur;
          end if;

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
                                    'Query Used for mode ' || p_mode ||' is '|| v_query
                                    ,g_object);

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                    'Query execution for Alert Mode Completed'
                                    ,g_object);

        elsif p_mode = 'DATE' then

           xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value2,g_debug_level,
                        'Generating report for the status '
                        ||nvl(p_status,'All')
                        ||' and from date '
                        || to_char(v_from_date,v_date_format)
                        ||' and to date '
                        ||to_char(v_to_date,v_date_format)
                        ,g_object);

            if p_type = g_mat_process then

                v_query := v_select||v_where_mat_invoice_process||v_where_status||v_where_date||v_group_by;
                open report_ref_cur for v_query using g_mat_process,p_status, v_from_date,v_to_date;
                 fetch report_ref_cur bulk collect into v_report_tab;
                close report_ref_cur;
            else
                v_query := v_select||v_where_ar_invoice_process||v_where_status||v_where_date||v_group_by;
                open report_ref_cur for v_query using g_ar_process,p_status, v_from_date,v_to_date;
                 fetch report_ref_cur bulk collect into v_report_tab;
                close report_ref_cur;
            end if;

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                    'Query execution for Date Mode Completed'
                                    ,g_object);

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
                                'Query Used for mode ' || p_mode ||' is '|| v_query
                                ,g_object);

        elsif p_mode = 'REQUEST_ID' then

           xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value2,g_debug_level,
                        'Generating report for the request_id ' ||p_request_id
                        ,g_object);

          v_query := v_select||v_where_request||v_group_by;

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
                                    'Query Used for mode ' || p_mode ||' is '|| v_query
                                    ,g_object);

          open report_ref_cur for v_query using p_request_id;
               fetch report_ref_cur bulk collect into v_report_tab;
          close report_ref_cur;

          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                    'Query execution for REQUEST_ID Mode Completed'
                                    ,g_object);

        end if;


        if v_report_tab.count > 0 then
          xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
              'Number of records fetched : '||v_report_tab.count
              ,g_object);

          xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (    g_file_name,'Batch ID'
                                                ||v_delim||'Source'
                                                ||v_delim||'Legal Entity'
                                                ||v_delim||'Operating Unit Name'
                                                ||v_delim||'Inventory Organization'
                                                ||v_delim||'Supplier GST Number'
                                                ||v_delim||'Supplier GST State'
                                                ||v_delim||'Recipient Name '
                                                ||v_delim||'Inventory Organization'
                                                ||v_delim||'Recipient Address'
                                                ||v_delim||'Recipient State'
                                                ||v_delim||'Recipient GST Number'
                                                ||v_delim||'Transaction Number'
                                                ||v_delim||'Transaction Date'
                                                ||v_delim||'Transaction Amount'
                                                ||v_delim||'Transaction Creation Date'
                                                ||v_delim||'Transaction Created By'
                                                ||v_delim||'Internal status code'
                                                ||v_delim||'Internal Validation Failure Reason'
                                                ||v_delim||'IRP Validation Failure Date'
                                                ||v_delim||'IRP Validation Failure Reason'
                                                ||v_delim||'IRN'
                                                ||v_delim||'IRN Date'
                                                ||v_delim||'Customer Type'
                                                ||v_delim||'Ack Number'
                                                ||v_delim||'Ack Date'
                                                ||v_delim||'Request ID'
                                                ,g_file_type
                                                ,'Y'
                                                ,'OPEN'
                                    );
          for i in v_report_tab.first .. v_report_tab.last loop

            xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name
                                        ,v_report_tab(i).batch_id--'Batch ID'
                                        ||v_delim|| v_report_tab(i).invoice_process--'Source'
                                        ||v_delim|| v_report_tab(i).supplier_legal_name--'Legal Entity'
                                        ||v_delim|| v_report_tab(i).ou_name--'Operating Unit Name'
                                        ||v_delim|| v_report_tab(i).owning_organization_name--'Inventory Organization'
                                        ||v_delim|| v_report_tab(i).supplier_gstin--'Supplier GST Number'
                                        ||v_delim|| v_report_tab(i).supplier_state_en--'Supplier GST State'
                                        ||v_delim|| v_report_tab(i).recipient_legal_name--'Recipient Name '
                                        ||v_delim|| v_report_tab(i).transfer_organization_name--'Inventory Organization'
                                        ||v_delim|| v_report_tab(i).recipient_address1--'Recipient Address'
                                        ||v_delim|| v_report_tab(i).recipient_state_en--'Recipient State'
                                        ||v_delim|| v_report_tab(i).recipient_gstin--'Recipient GST Number'
                                        ||v_delim|| v_report_tab(i).document_num--'Transaction Number'
                                        ||v_delim|| v_report_tab(i).document_date--'Transaction Date'
                                        ||v_delim|| v_report_tab(i).gross_amount--'Transaction Amount'
                                        ||v_delim|| v_report_tab(i).creation_date--'Transaction Creation Date'
                                        ||v_delim|| v_report_tab(i).created_by--'Transaction Created By'
                                        ||v_delim|| v_report_tab(i).status_code--'Internal status code'
                                        ||v_delim|| v_report_tab(i).status_message--'Internal Validation Failure Reason'
                                        ||v_delim|| v_report_tab(i).irp_error_date--'IRP Validation Failure Date'
                                        ||v_delim|| v_report_tab(i).irp_error_message--'IRP Validation Failure Reason'
                                        ||v_delim|| v_report_tab(i).irn--'IRN'
                                        ||v_delim|| v_report_tab(i).irn_creation_date--'IRN Date'
                                        ||v_delim|| v_report_tab(i).supply_type_code--'Customer Type'
                                        ||v_delim|| v_report_tab(i).ack_number--'Ack Number'
                                        ||v_delim|| v_report_tab(i).ack_date--'Ack Date'
                                        ||v_delim|| v_report_tab(i).request_id--'Request id'
                                        ,g_file_type
                                        ,'Y'
                                        ,'OPEN'
                                        );
          end loop;

          xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name
                                       ,null
                                       ,g_file_type
                                       ,'Y'
                                       ,'CLOSE'
                                       );

          if p_mode = 'ALERT' then

           if p_type = g_mat_process then
                xxoyo_send_mail_att_prc('XXOYO_EINV_STATUS_RPT_ERROR_ALERT_RPT',g_file_name,null,v_mail_sent_status);
           else
                xxoyo_send_mail_att_prc('XXOYO_EINV_ERROR_ALERT_RPT_AR',g_file_name,null,v_mail_sent_status);
           end if;

            if not v_mail_sent_status then
              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                    'Unable to sent the alert mail'
                                    ,g_object);
              g_ret_code := 1;
              return;

            end if;

            forall i in v_report_tab.first .. v_report_tab.last
                update xxoyo_einv_trx_hdr_tbl
                   set mail_sent = 'Y'
                 where 1 = 1
                   and transaction_id = v_report_tab(i).transaction_id
                   ;
          end if;

        else
            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                            'No records extracted for the report'
                            ,g_object);

        end if;

    exception
      when others then
        g_sqlerrm := substr(sqlerrm, 1, 240);
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                          'Error in generate_report: ' || g_sqlerrm
                          ,g_object);
        raise_application_error(-20901, 'Unexpected error in generate_report'||g_sqlerrm, true);
    end generate_report;

/******************************************************************************************************

Component Type: Procedure
Component Name: send_errors_alert
Description: This procedure generate report depending on the parama passed

******************************************************************************************************/

    procedure send_errors_alert(
       p_err_buff out varchar2
       ,p_ret_code out varchar2
       ,p_status in varchar2
    )is

      v_status_eligible       varchar(1) := 'N';

    begin

        if p_status is not null then


            select 'Y'
              into v_status_eligible
              from fnd_lookup_values
             where 1 = 1
               and lookup_type like 'XXOYO_CONCURRENT_PROGRAM_PARAM'
               and description = 'XXOYO_EINV_STATUS_RPT'
               and enabled_flag = 'Y'
               and lookup_code = p_status
               ;

            if(v_status_eligible = 'N') then
               xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Status Code is not correct'
                        ,g_object);
               raise_application_error(-20903, 'Status Code is not correct ', true);
            end if;

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value2,g_debug_level,
                            'Calling generate_report for status '||p_status
                            ,g_object);

            generate_report(p_status    =>  p_status
                            ,p_mode     =>  'ALERT'
                            ,p_type     => g_mat_process);

            generate_report(p_status    =>  p_status
                            ,p_mode     =>  'ALERT'
                            ,p_type     =>  g_ar_process);

        else
            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                            'Status Code is null'
                            ,g_object);

        end if;

    exception
        when others then
        g_sqlerrm := substr(sqlerrm, 1, 240);
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Error in send_errors_alert: ' || g_sqlerrm
                        ,g_object);
        raise_application_error(-20902, 'Unexpected error in send_errors_alert: ' || g_sqlerrm, true);
    end send_errors_alert;


/******************************************************************************************************

Component Type: Procedure
Component Name: generate_status_report
Description: This procedure generate report depending on the parama passed

******************************************************************************************************/

    procedure generate_status_report(
        p_err_buff      out varchar2
       ,p_ret_code      out varchar2
       ,p_type          varchar2
       ,p_from_date     date
       ,p_to_date       date
       ,p_status        varchar2  default null
    )
    is
    begin
        generate_report(p_from_date =>  p_from_date
                        ,p_to_date  =>  p_to_date
                        ,p_status   =>  p_status
                        ,p_mode     =>  'DATE'
                        ,p_type     => p_type
                        );


        g_ret_code := p_ret_code;

    exception
        when others then
        g_sqlerrm := substr(sqlerrm, 1, 240);

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                        'Error in generate_status_report: ' || g_sqlerrm
                        ,g_object);

        raise_application_error(-20901, 'Unexpected error in generate_status_report'||g_sqlerrm, true);
    end generate_status_report;


end xxoyo_einv_report_pkg;