/*****************************************************************************************************

Component Type: Procedure
Component Name: xxoyo_wf_notif_reassign_prc
Description: This procedure reassigns a PO/PR/AP from one user to another
Version Matrix
  Version     Author                Date               Description
   1.0        Mrinali Verma         10th March '21     As per BF-1243 procedure reassigns a PO/PR/AP 
                                                       from one user to another
                                                       Ticket number :2313807357



******************************************************************************************************/
create or replace PROCEDURE      xxoyo_wf_notif_reassign_prc (
   errbuff       OUT VARCHAR2,
   retcode       OUT VARCHAR2,
   p_doc_type        VARCHAR2,
   p_item_key        VARCHAR2,
   p_from_user       VARCHAR2,
   p_to_user         VARCHAR2)
AS
   CURSOR c_pr_rec
   IS
      SELECT DISTINCT
             ntf.notification_id,
             item_key,
             ntf.priority * -1 AS priority,
             DECODE (
                ntf.more_info_role,
                NULL, ntf.subject,
                   fnd_message.get_string ('FND', 'FND_MORE_INFO_REQUESTED')
                || ' '
                || ntf.subject)
                AS subject,
             ntf.language,
             NVL (ntf.sent_date, ntf.begin_date) begin_date,
             ntf.due_date,
             'P' AS priority_f,
             ntf.status,
             ntf.from_user,
             wit.display_name AS TYPE,
             ntf.more_info_role,
             ntf.from_role,
             ntf.recipient_role,
             DECODE (ntf.more_info_role,
                     NULL, ntf.to_user,
                     wf_directory.getroledisplayname (ntf.more_info_role))
                AS to_user,
             ntf.end_date,
             ntf.MESSAGE_TYPE,
             ntf.message_name,
             ntf.mail_status,
             ntf.original_recipient
        FROM wf_notifications ntf, wf_item_types_tl wit, wf_lookups_tl wl
       WHERE     ntf.status = 'OPEN'
             --       AND Ntf.from_role = 'SYSADMIN'
             --       AND notification_id = 323071
             AND ntf.MESSAGE_TYPE = wit.name
             AND wit.language = USERENV ('LANG')
             AND wl.lookup_type = 'WF_NOTIFICATION_STATUS'
             AND ntf.status = wl.lookup_code
             AND wl.language = USERENV ('LANG')
             AND ntf.recipient_role = p_from_user
             --       AND TO_CHAR (user_key) = TO_CHAR (100172)
             AND item_key LIKE
                       '%'
                    || SUBSTR (p_item_key || '-',
                               1,
                               INSTR (p_item_key || '-', '-') - 1)
                    || '%'
             AND MESSAGE_TYPE = 'REQAPPRV';
   CURSOR c_po_rec
   IS
      SELECT DISTINCT
             ntf.notification_id,
             item_key,
             ntf.priority * -1 AS priority,
             DECODE (
                ntf.more_info_role,
                NULL, ntf.subject,
                   fnd_message.get_string ('FND', 'FND_MORE_INFO_REQUESTED')
                || ' '
                || ntf.subject)
                AS subject,
             ntf.language,
             NVL (ntf.sent_date, ntf.begin_date) begin_date,
             ntf.due_date,
             'P' AS priority_f,
             ntf.status,
             ntf.from_user,
             wit.display_name AS TYPE,
             ntf.more_info_role,
             ntf.from_role,
             ntf.recipient_role,
             DECODE (ntf.more_info_role,
                     NULL, ntf.to_user,
                     wf_directory.getroledisplayname (ntf.more_info_role))
                AS to_user,
             ntf.end_date,
             ntf.MESSAGE_TYPE,
             ntf.message_name,
             ntf.mail_status,
             ntf.original_recipient
        FROM wf_notifications ntf, wf_item_types_tl wit, wf_lookups_tl wl
       WHERE     ntf.status = 'OPEN'
             --       AND Ntf.from_role = 'SYSADMIN'
             --       AND notification_id = 323071
             AND ntf.MESSAGE_TYPE = wit.name
             AND wit.language = USERENV ('LANG')
             AND wl.lookup_type = 'WF_NOTIFICATION_STATUS'
             AND ntf.status = wl.lookup_code
             AND wl.language = USERENV ('LANG')
             AND ntf.recipient_role = p_from_user
             --       AND TO_CHAR (user_key) = TO_CHAR (100172)
             AND item_key LIKE
                       '%'
                    || SUBSTR (p_item_key || '-',
                               1,
                               INSTR (p_item_key || '-', '-') - 1)
                    || '%'
             AND MESSAGE_TYPE = 'POAPPRV';
             
 --#ticket number:2313807357
   CURSOR c_ap_inv_rec
   IS
      SELECT DISTINCT
             ntf.notification_id,
             item_key,
             ntf.priority * -1 AS priority,
             DECODE (
                ntf.more_info_role,
                NULL, ntf.subject,
                   fnd_message.get_string ('FND', 'FND_MORE_INFO_REQUESTED')
                || ' '
                || ntf.subject)
                AS subject,
             ntf.language,
             NVL (ntf.sent_date, ntf.begin_date) begin_date,
             ntf.due_date,
             'P' AS priority_f,
             ntf.status,
             ntf.from_user,
             wit.display_name AS TYPE,
             ntf.more_info_role,
             ntf.from_role,
             ntf.recipient_role,
             DECODE (ntf.more_info_role,
                     NULL, ntf.to_user,
                     wf_directory.getroledisplayname (ntf.more_info_role))
                AS to_user,
             ntf.end_date,
             ntf.MESSAGE_TYPE,
             ntf.message_name,
             ntf.mail_status,
             ntf.original_recipient
        FROM wf_notifications ntf, wf_item_types_tl wit, wf_lookups_tl wl
       WHERE     ntf.status = 'OPEN'
             --       AND Ntf.from_role = 'SYSADMIN'
             --       AND notification_id = 323071
             AND ntf.MESSAGE_TYPE = wit.name
             AND wit.language = USERENV ('LANG')
             AND wl.lookup_type = 'WF_NOTIFICATION_STATUS'
             AND ntf.status = wl.lookup_code
             AND wl.language = USERENV ('LANG')
             AND ntf.recipient_role = p_from_user
             --       AND TO_CHAR (user_key) = TO_CHAR (100172)
            AND (case
             when p_item_key is not null and (item_key LIKE p_item_key ||'%')
             then 1
             when p_item_key is  null  then 1    
             else 0
             end = 1
             )   
             AND MESSAGE_TYPE = 'APINVAPR';  
BEGIN
   fnd_file.put_line (fnd_file.LOG, 'Start of xxoyo_wf_notif_reassign_prc');
   IF p_doc_type = 'PR'
   THEN
      fnd_file.put_line (fnd_file.LOG, 'Start of WF Reassign for PR');
      FOR i IN c_pr_rec
      LOOP
         fnd_file.put_line (fnd_file.LOG, 'Item Key for PR : ' || i.item_key);
         wf_notification.forward (i.notification_id,
                                  p_to_user,
                                  NULL,
                                  p_from_user,
                                  1,
                                  'RULE');
      END LOOP;
   END IF;
   IF p_doc_type = 'PO'
   THEN
      fnd_file.put_line (fnd_file.LOG, 'Start of WF Reassign for PO');
      FOR i IN c_po_rec
      LOOP
         fnd_file.put_line (fnd_file.LOG, 'Item Key for PO : ' || i.item_key);
         wf_notification.forward (i.notification_id,
                                  p_to_user,
                                  NULL,
                                  p_from_user,
                                  1,
                                  'RULE');
      END LOOP;
   END IF;

   --#ticket number:2313807357
   IF p_doc_type = 'AP'
   THEN
      fnd_file.put_line (fnd_file.LOG, 'Start of WF Reassign for Invoice');
      FOR i IN c_ap_inv_rec
      LOOP
         fnd_file.put_line (fnd_file.LOG, 'Item Key for Invoice : ' || i.item_key);
         wf_notification.forward (i.notification_id,
                                  p_to_user,
                                  NULL,
                                  p_from_user,
                                  1,
                                  'RULE');
      END LOOP;
   END IF;
   COMMIT;
   fnd_file.put_line (fnd_file.LOG, 'End of xxoyo_wf_notif_reassign_prc');
   fnd_file.put_line (fnd_file.LOG,
                      '**************End of the Report****************');
EXCEPTION
   WHEN OTHERS
   THEN
      ROLLBACK;
      retcode := 1;
      errbuff := SQLERRM;
      fnd_file.put_line (
         fnd_file.LOG,
         'Error in xxoyo_wf_notif_reassign_prc : ' || errbuff);
END;