create or replace package body xxoyo_po_pr_closure_report_pkg
as

/******************************************************************************************************

Component Type: Package
Component Name: XXOYO_PO_PR_CLOSURE_REPORT_PKG
Description:

Version Matrix

Version     Author                Date                  Description
  1.0       Mrinali Verma         16th Dec '20          As per FACNT-959 OracleERP - PR/PO
                                                        Consumption timing; Closure and Expiry

  1.1       Mrinali Verma         21st Dec '20          As per oyo_seek ticket:7958323388,adding
                                                        some logs to find the error in bcc_emails

  1.2       Mrinali Verma         19th Jan '20          As per the ticket:3982495902 few changes 
                                                        had to be done and a bug where POs without PR
                                                        were not getting picked had to be fixed

******************************************************************************************************/

  g_debug_level number := nvl(to_number(fnd_profile.value('XXOYO_DEBUG_LEVEL')), 3);
  g_debug_mode varchar2(10) := 'FILE';
  g_debug_log varchar2(10) := 'LOG';
  g_debug_output varchar2(10) := 'OUTPUT';
  g_object varchar2(50) := 'XXOYO_PO_PR_CLOSURE_REPORT_PKG: ' || fnd_global.conc_request_id;
  g_debug_value0 number := 0;
  g_debug_value1 number := 1;
  g_debug_value2 number := 2;
  g_debug_value3 number := 3;
  g_user_id number := fnd_global.user_id;
  g_request_id number := fnd_global.conc_request_id;
  g_sql_errm varchar2(300);
  g_file_name varchar2(50) := 'Oracle_PO_PR_Report_'||to_char(sysdate,'DD-MON-RRRR')||'.pcl';
  g_file_type utl_file.file_type:= null;
  g_errbuff varchar2(500);
  g_retcode varchar2(1);

/**********************************************************************q********************************
Component Type: Procedure
Component Name: po_pr_report
Description: This procedure generates report based on various parameters
******************************************************************************************************/

    procedure po_pr_report(p_from_date      date default null
                          ,p_to_date        date default null
                          ,p_status         varchar2 default null
                          ,p_request_id     number default null
                          ,p_mode           varchar2
                          ,p_type           varchar2 default null)
    is

        type report_record_datatype is record(document_id           number
                                              ,document_num         varchar2(50)
                                              ,creation_date        varchar2(50)
                                              ,requestor_id         number
                                              ,requestor_name       varchar2(100)
                                              ,document_subtype     varchar2(50)
                                              ,document_type_code   varchar2(50)
                                              ,closure_status       varchar2(25)
                                              ,closure_status_date  varchar2(50)
                                              ,status_code          varchar2(25)
                                              ,operating_unit_name  varchar2(240)
                                              ,authorization_status varchar2(25)
                                              ,amount               number
                                              ,supplier_name        varchar(240));

        type report_tab is table of report_record_datatype index by binary_integer;

        v_report_tab report_tab;
        v_from_date date;
        v_to_date date;
        v_eligible_flag varchar2(5);
        v_date_format   varchar2(30):= 'DD-MON-RRRR HH24:MI:SS';

        v_status_code varchar2(20);
        v_type varchar2(20);
        v_delim varchar(1) := '~';
        v_query_select varchar2(4000) := ' select
                                           document_id
                                           ,document_num
                                           ,to_char(creation_date,''DD-MON-RRRR HH24:MI:SS'')creation_date
                                           ,requestor_id
                                           ,requestor_name
                                           ,document_subtype
                                           ,decode(document_type_code,
                                                    ''PO'',''Purchase Order'',
                                                    ''REQUISITION'',''Purchase Requisition'') document_type
                                           ,closure_status
                                           ,to_char(closure_status_date,''DD-MON-RRRR HH24:MI:SS'')closure_status_date
                                           ,status_code
                                           ,operating_unit_name
                                           ,authorization_status
                                           ,amount
                                           ,supplier_name
                                           from
                                           xxoyo_po_pr_closure_tbl xppct
                                           where
                                           xppct.status_code <> ''RE-EXTRACTED''
                                           ';

        v_date_query varchar2(300)      := '
                                            and xppct.creation_date between :p_from_date and :p_to_date' ;

        v_status_query varchar2(200)    :=  '
                                            and xppct.status_code= nvl(:p_status,xppct.status_code)' ;

        v_doc_code_query varchar2(700)  :=  '
                                            and decode(xppct.document_type_code,''PO'',''Purchase Order'',
                                            ''REQUISITION'',''Purchase Requisition'')=nvl(:p_type,decode(xppct.document_type_code,''PO'',''Purchase Order'',
                                                                                                    ''REQUISITION'',''Purchase Requisition''))' ;

        v_request_id_query varchar2(200):= '
                                             and xppct.request_id=:p_request_id';
        v_final_query varchar2(4000);

        v_ref_cur sys_refcursor;

    begin
        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
        'Entering po_pr_report procedure ',g_object);


        v_from_date :=  to_date(to_char(nvl(p_from_date, sysdate - 1), 'DD-MON-RRRR') || '00:00:00', v_date_format );
        v_to_date := to_date(to_char(nvl(p_to_date, sysdate), 'DD-MON-RRRR') || '23:59:59', v_date_format );

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
        'Mode: '||p_mode||' Type: '||p_type ||' Status: '||p_status||' From Date: '||to_char(v_from_date,v_date_format)||' To Date: '||to_char(v_to_date,v_date_format) ,g_object);

        if p_mode = 'DATE' then
            v_final_query :=v_query_select||v_date_query||v_doc_code_query||v_status_query;
            open v_ref_cur for v_final_query using v_from_date,v_to_date,p_type,p_status;
            fetch v_ref_cur bulk collect into v_report_tab;
            close v_ref_cur;

        elsif p_mode = 'REQUEST_ID' then
            v_final_query := v_query_select||v_status_query||v_doc_code_query||v_request_id_query;
            open v_ref_cur for v_final_query using p_status,p_type,p_request_id;
            fetch v_ref_cur bulk collect into v_report_tab;
            close v_ref_cur;

        end if;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                                'Generating report on the basis of: ' || p_mode
                , g_object);

        if v_report_tab.count>0 then

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
            'Number of records fetched for report: '||v_report_tab.count
            ,g_object);


            xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name,'Operating_unit'
                                                            ||v_delim||'Document_type_code'
                                                            ||v_delim||'Document_id'
                                                            ||v_delim||'Document_num'
                                                            ||v_delim||'Creation_date'
                                                            ||v_delim||'Requestor_id'
                                                            ||v_delim||'Requestor_name'
                                                            ||v_delim||'Document_subtype'
                                                            ||v_delim||'Status_code'
                                                            ||v_delim||'Authorization_status'
                                                            ||v_delim||'Amount'
                                                            ||v_delim||'Vendor_name'
                                                            ||v_delim||'Closure_status'
                                                            ||v_delim||'Closure_status_date'
                                                            ,g_file_type
                                                            ,'Y'
                                                            ,'OPEN' );
            for  i in v_report_tab.first ..v_report_tab.last

            loop

                xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name,v_report_tab(i).operating_unit_name
                                                                ||v_delim||v_report_tab(i).document_type_code
                                                                ||v_delim||v_report_tab(i).document_id
                                                                ||v_delim||v_report_tab(i).document_num
                                                                ||v_delim||v_report_tab(i).creation_date
                                                                ||v_delim||v_report_tab(i).requestor_id
                                                                ||v_delim||v_report_tab(i).requestor_name
                                                                ||v_delim||v_report_tab(i).document_subtype
                                                                ||v_delim||v_report_tab(i).status_code
                                                                ||v_delim||v_report_tab(i).authorization_status
                                                                ||v_delim||v_report_tab(i).amount
                                                                ||v_delim||v_report_tab(i).supplier_name
                                                                ||v_delim||v_report_tab(i).closure_status
                                                                ||v_delim||v_report_tab(i).closure_status_date
                                                                ,g_file_type
                                                                ,'Y'
                                                                ,'OPEN' );
            end loop;

            xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name
                                                   ,null
                                                   ,g_file_type
                                                   ,'Y'
                                                   ,'CLOSE'
                                                   );
        else

            xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
            'No record extracted for mode: ' || p_mode, g_object);
        end if;

        exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 2400);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'Unexpected error in po_pr_report: ' || g_sql_errm, g_object);

                raise_application_error(-20903, 'Unexpected error in po_pr_report: '|| g_sql_errm, true);


    end po_pr_report;

/******************************************************************************************************
Component Type: Procedure
Component Name: call_email_prc
Description: This procedure sends mail
******************************************************************************************************/


procedure call_email_prc(p_process_name varchar2
                        ,p_status_code varchar2
                        ,p_doc_code varchar2)

is
v_mail_sent_status boolean := false;
begin
     if p_process_name is null then

              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                'Unable to extract email process',g_object);
              raise_application_error(-20910, 'Unable to extract email process', true);

            else

              xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                'Mail Process Extracted is: ' || p_process_name, g_object);

            end if;

            xxoyo_send_mail_att_prc(p_process_name,g_file_name,null,v_mail_sent_status);

            if not v_mail_sent_status and p_status_code = 'PENDING' then

                xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                  'Error while sending mail post extraction',g_object);

                raise_application_error(-20911, 'Error while sending mail post extraction', true);

            elsif not v_mail_sent_status and p_status_code = 'SUCCESS' then

                xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                  'Error while sending mail post action taken',g_object);

                update xxoyo_po_pr_closure_tbl xppct
                   set mail_sent_flag = 'N'
                      ,status_message = 'Mail not sent'
                where 1 = 1
                  and request_id = g_request_id;

                xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                  'Records updated in custom table for mail sent as N: '|| sql%rowcount,g_object);

                commit;

                raise_application_error(-20911, 'Error while sending mail post action taken', true);

            elsif p_status_code = 'PENDING' then

                update xxoyo_po_pr_closure_tbl xppct
                   set status_code = 'RUNNING'
                      ,status_message = 'Updating the status'
                 where 1 = 1
                   and request_id = g_request_id
                   and document_type_code= p_doc_code;

                xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                  'Records updated in custom table for status as RUNNING post sending mail: '|| sql%rowcount,g_object);

            elsif p_status_code = 'SUCCESS' then

                update xxoyo_po_pr_closure_tbl xppct
                   set mail_sent_flag = 'Y'
                      ,status_message = 'Mail successfully sent'
                 where 1 = 1
                   and request_id = g_request_id
                   and document_type_code=p_doc_code
                   and status_code='SUCCESS';

                xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
                                  'Records updated in custom table for mail sent as Y: '|| sql%rowcount,g_object);

            end if;
         exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 240);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'Unexpected error in call_email_prc : ' || g_sql_errm, g_object);


               raise_application_error(-20903, 'Unexpected error in call_email_prc: '|| g_sql_errm, true);


end call_email_prc;

/******************************************************************************************************
Component Type: Procedure
Component Name: update_and_send_mail
Description: This procedure updates the bcc_email column in xxoyo_mail_processes_tbl
******************************************************************************************************/


    procedure update_and_send_mail(p_status_code varchar2
                                  ,p_doc_code varchar2
                                  ,p_bcc_email varchar2
                                  ,p_process_name out varchar2)

    is
    pragma autonomous_transaction;

    v_process_name varchar2(200);
    v_mail_sent_status boolean := false;
    begin


        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering update_and_send_mail procedure with parameters: '
          ||p_status_code||p_doc_code||'####'||p_bcc_email ,g_object);

        if p_status_code = 'PENDING' and p_doc_code = 'PO' then

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering the update block of xxoyo_mail_processes_tbl
            with process name XXOYO_PO_EXTRACTION',g_object);

            update xxoyo_mail_processes_tbl
            set bcc_email = trim(p_bcc_email)
            where process_name = 'XXOYO_PO_EXTRACTION';

            v_process_name := 'XXOYO_PO_EXTRACTION';

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
            'Records updated in mail process table for PENDING POs: '|| sql%rowcount, g_object);

        elsif p_status_code = 'PENDING' and p_doc_code = 'REQUISITION' then

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering the update block of xxoyo_mail_processes_tbl
            with process name XXOYO_PR_EXTRACTION',g_object);

            update xxoyo_mail_processes_tbl
            set bcc_email = trim(p_bcc_email)
            where process_name='XXOYO_PR_EXTRACTION';

            v_process_name := 'XXOYO_PR_EXTRACTION';

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
            'Records updated in mail process table for PENDING PRs: '|| sql%rowcount, g_object);

        elsif p_status_code = 'SUCCESS' and p_doc_code = 'PO' then

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering the update block of xxoyo_mail_processes_tbl
            with process name XXOYO_PO_ACTION',g_object);

            update xxoyo_mail_processes_tbl
            set bcc_email = trim(p_bcc_email)
            where process_name = 'XXOYO_PO_ACTION';

            v_process_name := 'XXOYO_PO_ACTION';

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
            'Records updated in mail process table for SUCCESS POs: '|| sql%rowcount, g_object);

        elsif p_status_code = 'SUCCESS' and p_doc_code = 'REQUISITION' then

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering the update block of xxoyo_mail_processes_tbl
            with process name XXOYO_PR_ACTION',g_object);

            update xxoyo_mail_processes_tbl
            set bcc_email = trim(p_bcc_email)
            where process_name = 'XXOYO_PR_ACTION';

            v_process_name := 'XXOYO_PR_ACTION';

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
            'Records updated in mail process table for SUCCESS PRs: '|| sql%rowcount, g_object);

        end if;

        p_process_name := v_process_name;

        commit;

        --call_email_prc(v_process_name ,p_status_code,p_doc_code);

        exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 240);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'Unexpected error in update_and_send_mail : ' || g_sql_errm, g_object);


               raise_application_error(-20903, 'Unexpected error in update_and_send_mail: '|| g_sql_errm, true);

    end;

 /******************************************************************************************************

  Component Type: Procedure
  Component Name: update_bcc_list
  Description: This procedure updates the bcc list and sends the mail

  ******************************************************************************************************/

    procedure update_bcc_list(p_status_code IN varchar2
                              ,p_doc_code   IN varchar2
                             -- ,p_process_name out varchar2
                              )
    is


        v_status_code varchar2(50);

        type bcc_email_list is table of xxoyo_po_pr_closure_tbl.requestor_email_address%type index by binary_integer;

        v_bcc_list bcc_email_list;

        v_bcc_query varchar2(1000) := 'select distinct requestor_email_address
                                      from xxoyo_po_pr_closure_tbl
                                      where eligible_flag = ''Y''
                                      and document_type_code = :p_doc_code
                                      and status_code = :p_status_code
                                      order by requestor_email_address';

        bcc_ref_cur SYS_REFCURSOR;

        v_count number := 0;

        v_bcc_email_1 varchar2(4000);
        v_bcc_email_2 varchar2(4000);
        v_bcc_email_3 varchar2(4000);
        v_bcc_email_4 varchar2(4000);
        v_bcc_email_5 varchar2(4000);
        v_mail_sent_status boolean := false;
        v_process_name varchar2(100);

        v_max_length number := 2500;    --#ticket:3982495902

    begin

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Entering update_bcc_list procedure with status: '
        ||p_status_code||' and doc_code: '||p_doc_code,g_object);

        open bcc_ref_cur for v_bcc_query using p_doc_code,p_status_code;
        fetch bcc_ref_cur bulk collect into v_bcc_list;
        close bcc_ref_cur;

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'BCC_LIST count: '||v_bcc_list.count,g_object);

        for i in v_bcc_list.first .. v_bcc_list.last
        loop

         /* v_count := v_count + 1;

          if v_count = 1 then
            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
              'Exiting BCC loop', g_object);
             v_count :=0;
            exit;
          end if;*/

            if length(v_bcc_email_1) < v_max_length or v_bcc_email_1 is null then
                v_bcc_email_1 := v_bcc_email_1 || ' ' || v_bcc_list(i);

            elsif length(v_bcc_email_2) < v_max_length or v_bcc_email_2 is null then
                v_bcc_email_2 := v_bcc_email_2 || ' ' || v_bcc_list(i);

            elsif length(v_bcc_email_3) < v_max_length  or v_bcc_email_3 is null then
                v_bcc_email_3 := v_bcc_email_3 || ' ' || v_bcc_list(i);

            elsif length(v_bcc_email_4) < v_max_length  or v_bcc_email_4 is null then   --#ticket:3982495902
                v_bcc_email_4 := v_bcc_email_4 || ' ' || v_bcc_list(i);   

            elsif length(v_bcc_email_5) < v_max_length  or v_bcc_email_5 is null then   --#ticket:3982495902
                v_bcc_email_5 := v_bcc_email_5 || ' ' || v_bcc_list(i);                              

            end if;

        end loop;

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Exiting for loop of bcc_list',g_object); ----#ticket_number  :7958323388


        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,'Email list:: '
          ||v_bcc_email_1||chr(13)||v_bcc_email_2||chr(13)||v_bcc_email_2 ,g_object);


        for i in 1 .. 5
        loop

          if (i = 1 and v_bcc_email_1 is not null) then
            update_and_send_mail(p_status_code
                                ,p_doc_code
                                ,v_bcc_email_1
                                ,v_process_name);
            call_email_prc(v_process_name ,p_status_code,p_doc_code);

          elsif (i = 2 and v_bcc_email_2 is not null) then
            update_and_send_mail(p_status_code
                                ,p_doc_code
                                ,v_bcc_email_2
                                ,v_process_name);
            call_email_prc(v_process_name ,p_status_code,p_doc_code);

          elsif (i = 3 and v_bcc_email_3 is not null) then
            update_and_send_mail(p_status_code
                                ,p_doc_code
                                ,v_bcc_email_3
                                ,v_process_name);
            call_email_prc(v_process_name ,p_status_code,p_doc_code);

          elsif (i = 4 and v_bcc_email_4 is not null) then      --#ticket:3982495902
            update_and_send_mail(p_status_code
                                ,p_doc_code
                                ,v_bcc_email_4
                                ,v_process_name);
            call_email_prc(v_process_name ,p_status_code,p_doc_code);   

          elsif (i = 5 and v_bcc_email_5 is not null) then        --#ticket:3982495902
            update_and_send_mail(p_status_code
                                ,p_doc_code
                                ,v_bcc_email_5
                                ,v_process_name);
            call_email_prc(v_process_name ,p_status_code,p_doc_code);                      

          end if;

        end loop;

        xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                            'Exiting update_bcc_list ', g_object);



        exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 240);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'Unexpected error in update_bcc_list: ' || g_sql_errm, g_object);

                raise_application_error(-20903, 'Unexpected error in update_bcc_list: '|| g_sql_errm, true);

    end update_bcc_list;

/******************************************************************************************************

Component Type: Procedure
Component Name: send_mail
Description: This procedure collects data and makes a file

******************************************************************************************************/


    procedure send_mail(p_status_code varchar2)

    is

        v_doc_code varchar2(50);
        v_delim varchar(1) := '~';
        v_count number;
        --v_stage number := 0;
        v_mail_sent_status      boolean;
        v_process_name varchar2(200);
        --v_status_code varchar2(50);
        type email_data_type is record(document_id number,document_num varchar2(20),creation_date varchar2(50)
                                      ,approval_date varchar2(50),requestor_name varchar(100)
                                      ,requestor_email_address varchar2(500),document_type varchar2(50)
                                      ,transaction_id varchar2(50),operating_unit_name varchar2(240)
                                      ,authorization_status varchar2(25),supplier_name varchar2(240),amount number);
        type email_tab is  table of email_data_type index by binary_integer;
        v_email_tab email_tab;
        email_ref_cur sys_refcursor;
           --rajinder, use request id check as well  in the query where clause
        v_query varchar2(3000) := 'select
                                    document_id
                                   ,document_num
                                   ,to_char(purchase_creation_date,''DD-MON-RRRR HH24:MI:SS'') creation_date
                                   ,to_char(approval_date,''DD-MON-RRRR HH24:MI:SS'')
                                   ,requestor_name
                                   ,requestor_email_address
                                   ,decode(document_type_code,
                                          ''PO'',''Purchase Order'',
                                          ''REQUISITION'',''Purchase Requisition'') document_type
                                   ,transaction_id
                                   ,operating_unit_name
                                   ,authorization_status
                                   ,supplier_name
                                   ,amount
                                    from xxoyo_po_pr_closure_tbl
                                    where 1=1
                                    and request_id = :g_request_id
                                    and status_code = :p_status_code
                                    and eligible_flag =''Y''
                                    and document_type_code = :v_doc_code
                                   ';

    begin

        xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value3,g_debug_level,
                'Inside send_mail with status_code: '|| p_status_code ,g_object);

        for i in 1 .. 2
        loop

            if i = 1 then

              v_doc_code := 'PO';
              g_file_name:= 'Oracle_PO_To_Be_Closed_List_'||to_char(sysdate,'DD-MON-RRRR')||'.pcl';

            elsif i = 2 then

              v_doc_code := 'REQUISITION';
              g_file_name:= 'Oracle_PR_To_Be_Closed_List_'||to_char(sysdate,'DD-MON-RRRR')||'.pcl';

            end if;

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
            'Opening cursor email_ref_cur' ,g_object);

            open  email_ref_cur for v_query using g_request_id,p_status_code,v_doc_code;
            fetch email_ref_cur bulk collect into v_email_tab;
            close email_ref_cur;


            if v_email_tab.count = 0 then

                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'No data fetched for '||v_doc_code||' for sending mail', g_object);

                goto skip_sending_mail;

            end if;

            xxoyo_log_message(g_debug_mode, g_debug_log,g_debug_value0,g_debug_level,
            'Number of records fetched for sending email for '|| v_doc_code|| ' are: '||v_email_tab.count ,g_object);

            xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name,'Operating_unit'
                                                  ||v_delim||'Document_type'
                                                  ||v_delim||'Document_id'
                                                  ||v_delim||'Document_num'
                                                  ||v_delim||'Creation_date'
                                                  ||v_delim||'Approval_date'
                                                  ||v_delim||'Authorization_status'
                                                  ||v_delim||'Requestor_name'
                                                  ||v_delim||'Requestor_email_address'
                                                  ||v_delim||'Amount'
                                                  ||v_delim||'Vendor_name'

                                                  ,g_file_type
                                                  ,'Y'
                                                  ,'OPEN' );

            for  i in v_email_tab.first ..v_email_tab.last
            loop

                xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name,v_email_tab(i).operating_unit_name
                                                  ||v_delim||v_email_tab(i).document_type
                                                  ||v_delim||v_email_tab(i).document_id
                                                  ||v_delim||v_email_tab(i).document_num
                                                  ||v_delim||v_email_tab(i).creation_date
                                                  ||v_delim||v_email_tab(i).approval_date
                                                  ||v_delim||v_email_tab(i).authorization_status
                                                  ||v_delim||v_email_tab(i).requestor_name
                                                  ||v_delim||v_email_tab(i).requestor_email_address
                                                  ||v_delim||v_email_tab(i).amount
                                                  ||v_delim||v_email_tab(i).supplier_name
                                                  ,g_file_type
                                                  ,'Y'
                                                  ,'OPEN' );
            end loop;

            xxoyo_einv_utility_pkg.xxoyo_write_utl_file_output (g_file_name
                                         ,null
                                         ,g_file_type
                                         ,'Y'
                                         ,'CLOSE'
                                         );

            update_bcc_list(p_status_code
                            ,v_doc_code
                          --  ,v_process_name
                            );


            <<skip_sending_mail>>

            null;

        end loop;

        exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 240);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                'Unexpected error in send_mail : ' || g_sql_errm , g_object);

                raise_application_error(-20903, 'Unexpected error in send_mail', true);

    end send_mail;

/******************************************************************************************************

Component Type: Procedure
Component Name: generate_report
Description: This procedure calls the po_pr_report procedure based on the parameters set by user

******************************************************************************************************/

    procedure generate_report(
                                 p_errbuff out varchar2
                                ,p_retcode out varchar2
                                ,p_type varchar2
                                ,p_from_date date
                                ,p_to_date date
                                ,p_status  varchar2
                                )
    is

    begin

          po_pr_report(p_from_date =>  p_from_date
                        ,p_to_date  =>  p_to_date
                        ,p_status   =>  p_status
                        ,p_mode     =>  'DATE'
                        ,p_type     => p_type
                        );


        g_errbuff := p_errbuff;
        g_retcode := p_retcode;

        exception
            when others then
                g_sql_errm := substr(sqlerrm, 1, 240);
                xxoyo_log_message(g_debug_mode, g_debug_log, g_debug_value0, g_debug_level,
                                  'Unexpected error in generate_report: ' || g_sql_errm
                , g_object);
                raise_application_error(-20901, 'Unexpected error in generate_report: ' || g_sql_errm, true);
    end generate_report;

end xxoyo_po_pr_closure_report_pkg;
/