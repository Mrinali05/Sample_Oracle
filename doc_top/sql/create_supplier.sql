create or replace PACKAGE BODY      XXOYO_EMP_AS_VENDOR_CREATE_PKG
/* $Header: XXOYO_EMP_AS_VENDOR_CREATE_PKG_BODY.sql 1.00 2019/11/01 pwc@pwc.com$*/
-- +=========================================================================+
-- |                                 OYO                                     |
-- +=========================================================================+
-- | FILENAME                                                                |
-- |     XXOYO_EMP_AS_VEN_CREATE_PKG_BODY.sql                                |
-- |                                                                         |
-- | DESCRIPTION                                                             |
-- |     This is the custom package used to create an Employee as Supplier   |
-- |                                                                         |
-- | HISTORY                                                                 |
-- |  Version  Date         Author        Comments                           |
-- |  -------  -----------  ----------    ---------------------------------  |
-- |  V1.0     01-Nov-2019  PwC-Pranjul   Initial Version                    |
-- +=========================================================================+
AS
   PROCEDURE XXOYO_MAIN_PRC (p_out_ret_msg          OUT VARCHAR2,
                             p_out_ret_code         OUT NUMBER,
                             p_in_process_mode   IN     VARCHAR2)
   IS
      CURSOR get_emp_cur (p_request_id NUMBER)
      IS
           SELECT *
             FROM XXOYO_EMP_AS_VENDOR_TBL
            WHERE process_flag = 'N' AND request_id = p_request_id
         ORDER BY Employee_id, org_id, VENDOR_SITE_CODE;



      l_org_id                 NUMBER;     -- := fnd_profile.VALUE ('ORG_ID');
      l_load_count             NUMBER;
      l_error_count            NUMBER;
      --   l_bg_flag                NUMBER;
      --   l_bg_id                  NUMBER;
      -----Standard API Parameters-------------------------
      l_api_version            NUMBER := 1.0;
      l_return_status          VARCHAR2 (200);
      l_msg_count              NUMBER;
      l_msg_data               VARCHAR2 (200);
      -----Vendor API Parameters-------------------------
      l_vendor_rec             apps.ap_vendor_pub_pkg.r_vendor_rec_type;
      l_vendor_id              NUMBER;
      l_party_id               NUMBER;
      l_segment1               VARCHAR2 (40);
      -----Vendor Site API Parameters-------------------------
      l_vendor_site_rec        apps.ap_vendor_pub_pkg.r_vendor_site_rec_type;
      l_vendor_site_id         NUMBER;
      l_party_site_id          NUMBER;
      l_location_id            NUMBER;
      l_liab_accts             VARCHAR2 (100);
      l_prepay_accts           VARCHAR2 (100);
      l_coa_id                 NUMBER;
      --------------------------------------------
      l_api_message            VARCHAR2 (4000);
      e_api_error              EXCEPTION;
      l_next_supplier_number   ap_product_setup.next_auto_supplier_num%TYPE;

      l_user_id                NUMBER := fnd_global.user_id;
      l_request_id             NUMBER := fnd_global.conc_request_id;
      l_err_code               NUMBER;
      l_err_msg                VARCHAR2 (4000);
      l_exception              EXCEPTION;
      l_ret_code               NUMBER;
      l_ret_msg                VARCHAR2 (2000);
      l_count                  NUMBER;

      l_resp_id                NUMBER;
      l_app_id                 NUMBER;
      l_default_ou             hr_operating_units.name%TYPE := 'OHHPL_SMART';
      l_term_id                ap_terms.term_id%TYPE;

      ------------------------------------------
      PROCEDURE write_log (p_text IN VARCHAR2)
      IS
      BEGIN
         fnd_file.put_line (fnd_file.LOG, p_text);
      -- DBMS_OUTPUT.put_line (p_text);
      END write_log;
   BEGIN
      BEGIN
         SELECT RESPONSIBILITY_ID, APPLICATION_ID
           INTO l_resp_id, l_app_id
           FROM fnd_responsibility_tl
          WHERE responsibility_name = 'India Local Purchasing';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_resp_id := NULL;
            l_app_id := NULL;
      END;


      fnd_global.apps_initialize (user_id        => l_user_id,
                                  resp_id        => l_resp_id,
                                  resp_appl_id   => l_app_id);

      --     MO_GLOBAL.SET_POLICY_CONTEXT('S',102)
      mo_global.init ('JA');

      write_log (
            '******Program starts to create suppliers for an employee...'
         || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

      INSERT INTO XXOYO_EMP_AS_VENDOR_TBL (REQUEST_ID,
                                           EMPLOYEE_ID,
                                           EMPLOYEE_NUMBER,
                                           EMPLOYEE_NAME,
                                           LEGAL_ENTITY,
                                           CURRENT_EMPLOYEE_FLAG,
                                           OPERATING_UNIT,
                                           ORG_ID,
                                           VENDOR_NAME,
                                           VENDOR_NAME_ALT,
                                           VENDOR_CODE,
                                           VENDOR_SITE_CODE,
                                           EMAIL_ADDRESS,
                                           PAN,
                                           VENDOR_ID,
                                           PARTY_ID,
                                           VENDOR_SITE_ID,
                                           PROCESS_FLAG,
                                           ERROR_MSG,
                                           ATTRIBUTE1,
                                           ATTRIBUTE2,
                                           ATTRIBUTE3,
                                           ATTRIBUTE4,
                                           ATTRIBUTE5,
                                           CREATION_DATE,
                                           CREATED_BY,
                                           LAST_UPDATE_DATE,
                                           LAST_UPDATED_BY)
         (SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'OFFICE' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY
            -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --   AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = flv.attribute3
                 AND hru.name <> l_default_ou -- This is to bypass creating SITE in Default OU here
                 AND flv.attribute4 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y'
          UNION
          SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'HOME' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY                -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --  AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = flv.attribute3
                 AND hru.name <> l_default_ou -- This is to bypass creating SITE in Default OU here
                 AND flv.attribute6 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y'
          UNION
          SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'PROVISIONAL' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY                -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --   AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = flv.attribute3
                 AND hru.name <> l_default_ou -- This is to bypass creating SITE in Default OU here
                 AND flv.attribute8 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y'
          UNION
          --- 3 sites are to be created under OHHPL_SMART by default, irrespective of its mapping in Lookup against Employee's Legal Entity
          SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'OFFICE' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY
            -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --    AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = l_default_ou                  --flv.attribute3
                 AND flv.attribute4 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y'
          UNION
          SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'HOME' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY                -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --    AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = l_default_ou                  --flv.attribute3
                 AND flv.attribute6 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y'
          UNION
          SELECT l_request_id,
                 papf.person_id,
                 papf.employee_number,
                 papf.full_name,
                 paaf.ass_attribute2,
                 papf.current_employee_flag --NVL (papf.current_employee_flag, papf.current_npw_flag)
                                           employee_flag,
                 hru.name,
                 hru.organization_id,
                 (papf.employee_number --  NVL (papf.employee_number, papf.npw_number)
                                      || '-' || papf.full_name) vendor_name,
                 papf.full_name vendor_name_alt,
                 'E' || papf.employee_number -- NVL (papf.employee_number, papf.npw_number)
                                            Vendor_code,
                 'PROVISIONAL' vendor_site_code,
                 papf.email_address,
                 papf.per_information4 PAN,
                 NULL VENDOR_ID,
                 NULL PARTY_ID,
                 NULL VENDOR_SITE_ID,
                 'N' process_flag,
                 NULL ERROR_MSG,
                 NULL ATTRIBUTE1,
                 NULL ATTRIBUTE2,
                 NULL ATTRIBUTE3,
                 NULL ATTRIBUTE4,
                 NULL ATTRIBUTE5,
                 SYSDATE CREATION_DATE,
                 l_user_id CREATED_BY,
                 SYSDATE LAST_UPDATE_DATE,
                 l_user_id LAST_UPDATED_BY                -- pap.NAME POSITION
            FROM per_all_people_f papf,
                 per_all_assignments_f paaf,
                 fnd_lookup_values flv,
                 hr_operating_units hru
           -- ,per_all_positions pap
           WHERE     papf.person_id = paaf.person_id
                 --    AND papf.person_id IN (66)
                 -- AND paaf.organization_id = 102
                 AND paaf.ass_attribute2 IS NOT NULL        --HCM Legal Entity
                 AND papf.npw_number IS NULL
                 AND papf.employee_number IS NOT NULL -- Only Employees are to be considered
                 AND flv.lookup_type = 'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                 AND flv.enabled_flag = 'Y'
                 AND NVL (flv.end_date_active, SYSDATE + 1) >= SYSDATE
                 AND flv.attribute1 = paaf.ass_attribute2
                 AND hru.name = l_default_ou                  --flv.attribute3
                 AND flv.attribute8 IS NOT NULL
                 --  AND paaf.position_id = pap.position_id(+)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM ap_suppliers asup
                          WHERE asup.employee_id = papf.person_id)
                 AND NOT EXISTS
                        (SELECT 1
                           FROM XXOYO_EMP_AS_VENDOR_TBL est
                          WHERE est.employee_id = papf.person_id)
                 AND TRUNC (SYSDATE) BETWEEN papf.effective_start_date
                                         AND NVL (papf.effective_end_date,
                                                  SYSDATE)
                 AND TRUNC (SYSDATE) BETWEEN paaf.effective_start_date
                                         AND NVL (paaf.effective_end_date,
                                                  SYSDATE)
                 AND papf.current_employee_flag = --NVL (papf.current_employee_flag, papf.current_npw_flag) =
                                                 'Y');

      IF p_in_process_mode = 'Reprocess'
      THEN
         UPDATE XXOYO_EMP_AS_VENDOR_TBL
            SET process_flag = 'N',
                request_id = l_request_id,
                error_msg = NULL,
                last_update_date = SYSDATE,
                LAST_UPDATED_BY = l_user_id
          WHERE process_flag = 'E';
      END IF;

      COMMIT;


      BEGIN
         SELECT next_auto_supplier_num
           INTO l_next_supplier_number
           FROM ap_product_setup;

         UPDATE ap_product_setup
            SET supplier_numbering_method = 'MANUAL',
                next_auto_supplier_num = NULL,
                supplier_num_type = 'ALPHANUMERIC';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_next_supplier_number := NULL;
      END;

      BEGIN
         SELECT term_id
           INTO l_term_id
           FROM ap_terms
          WHERE name = 'Immediate';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_term_id := 10000;
      END;

      -- COMMIT;
      l_count := 0;

      FOR rec_emp IN get_emp_cur (l_request_id)
      LOOP
         l_org_id := NULL;
         l_vendor_id := NULL;
         l_err_code := 0;
         l_err_msg := NULL;
         l_ret_code := 0;
         l_ret_msg := NULL;
         l_org_id := rec_emp.org_id;

         l_count := l_count + 1;

         write_log ('');
         write_log (
               '****** Starting supplier interface load for employee# : '
            || rec_emp.employee_number
            || ' - '
            || rec_emp.employee_name);

         BEGIN
            IF rec_emp.vendor_id IS NULL AND rec_emp.vendor_site_id IS NULL
            THEN
               /*          BEGIN
                            SELECT hru.organization_id
                              INTO l_org_id
                              FROM hr_operating_units hru,
                                   per_all_assignments_f paaf,
                                   per_periods_of_service_v2 ppos
                             WHERE     hru.organization_id = paaf.organization_id
                                   AND paaf.assignment_id = ppos.assignment_id
                                   AND ppos.person_id = rec_emp.employee_id
                                   AND TRUNC (SYSDATE) BETWEEN TRUNC (ppos.date_start)
                                                           AND TRUNC (
                                                                  NVL (
                                                                     ppos.actual_termination_date,
                                                                     SYSDATE));
                         EXCEPTION
                            WHEN OTHERS
                            THEN
                               l_org_id := NULL;
                         END;

                         IF l_org_id IS NULL
                         THEN
                            l_err_code := 1;
                            l_err_msg :=
                               l_err_msg || 'Organization id could not be fetched; ';
                         END IF;

                         BEGIN
                            SELECT xxoyo.xxoyo_emp_vendor_num_seq.NEXTVAL
                              INTO l_vendor_rec.segment1
                              FROM DUAL;
                         EXCEPTION
                            WHEN OTHERS
                            THEN
                               l_err_code := 1;
                               l_err_msg :=
                                     l_err_msg
                                  || 'Sequence value could not be fetched for Supplier Number, '
                                  || SUBSTR (SQLERRM, 1, 100)
                                  || '; ';
                         END;
             */



               /*         IF l_err_code = 1
                        THEN
                           UPDATE XXOYO_EMP_AS_VENDOR_TBL
                              SET process_flag = 'E', ERROR_MSG = l_err_msg
                            WHERE     employee_id = rec_emp.employee_id
                                  AND process_flag = 'N'
                                  AND request_id = l_request_id;
                        ELSE
               */
               BEGIN
                  SELECT VENDOR_ID
                    INTO l_vendor_id
                    FROM ap_suppliers
                   WHERE     segment1 = rec_emp.vendor_code
                         AND VENDOR_NAME = rec_emp.vendor_name;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_vendor_id := NULL;
               END;

               IF l_vendor_id IS NULL
               THEN
                  l_vendor_rec.segment1 := rec_emp.vendor_code;
                  l_vendor_rec.vendor_name := rec_emp.vendor_name;
                  l_vendor_rec.VENDOR_NAME_ALT := rec_emp.vendor_name_alt;
                  l_vendor_rec.summary_flag := 'N';
                  l_vendor_rec.enabled_flag := 'Y';
                  l_vendor_rec.vendor_type_lookup_code := 'EMPLOYEE';
                  l_vendor_rec.terms_id := l_term_id;
                  l_vendor_rec.set_of_books_id :=
                     fnd_profile.VALUE ('GL_SET_OF_BKS_ID');
                  l_vendor_rec.hold_all_payments_flag := 'N';
                  l_vendor_rec.hold_future_payments_flag := 'N';
                  l_vendor_rec.start_date_active := SYSDATE;
                  l_vendor_rec.terms_date_basis := 'Invoice';
                  l_vendor_rec.employee_id := rec_emp.employee_id;
                  l_vendor_rec.ext_payee_rec.default_pmt_method := 'EFT';
                  apps.ap_vendor_pub_pkg.create_vendor (
                     p_api_version        => l_api_version,
                     p_init_msg_list      => fnd_api.g_true,
                     p_commit             => fnd_api.g_false,
                     p_validation_level   => fnd_api.g_valid_level_full,
                     x_return_status      => l_return_status,
                     x_msg_count          => l_msg_count,
                     x_msg_data           => l_msg_data,
                     p_vendor_rec         => l_vendor_rec,
                     x_vendor_id          => l_vendor_id,
                     x_party_id           => l_party_id);
               ELSE
                  l_return_status := 'S';
               END IF;

               IF NVL (l_return_status, 'E') != 'S'
               THEN
                  write_log (
                        'Error in Vendor API for Employee '
                     || rec_emp.employee_name
                     || ' Employee No:- '
                     || rec_emp.employee_number);

                  FOR i IN 1 .. l_msg_count
                  LOOP
                     l_api_message :=
                           i
                        || '. '
                        || fnd_msg_pub.get (p_encoded => fnd_api.g_false)
                        || CHR (10);
                     fnd_file.put (fnd_file.LOG, l_api_message);
                  END LOOP;

                  write_log ('***************************');
                  write_log ('Output information ....');
                  write_log ('x_return_status: ' || l_return_status);
                  write_log ('x_msg_count: ' || l_msg_count);
                  write_log ('x_msg_data: ' || l_api_message);
                  write_log ('x_msg_data: ' || l_msg_data);
                  write_log ('***************************');

                  UPDATE XXOYO_EMP_AS_VENDOR_TBL
                     SET process_flag = 'E',
                         ERROR_MSG =
                            'Error while creating Vendor, ' || l_msg_data,
                         last_update_date = SYSDATE,
                         last_updated_by = l_user_id
                   WHERE     employee_id = rec_emp.employee_id
                         AND vendor_code = rec_emp.vendor_code
                         AND process_flag = 'N'
                         AND request_id = l_request_id;
               ELSE
                  write_log (
                        'Vendor for Employee '
                     || rec_emp.employee_name
                     || ' Employee No:- '
                     || rec_emp.employee_number);
                  write_log (
                        'Vendor ID = '
                     || l_vendor_id
                     || ' Party ID = '
                     || l_party_id);

                  write_log (
                        'Calling Vendor Site API for Employee '
                     || rec_emp.employee_name
                     || ' Employee No:- '
                     || rec_emp.employee_number);

                  BEGIN
                     SELECT address_line_1,
                            address_line_2,
                            address_line_3,
                            town_or_city,
                            postal_code,
                            country
                       INTO l_vendor_site_rec.address_line1,
                            l_vendor_site_rec.address_line2,
                            l_vendor_site_rec.address_line3,
                            l_vendor_site_rec.city,
                            l_vendor_site_rec.zip,
                            l_vendor_site_rec.country
                       FROM hr_locations_all a, hr_all_organization_units b
                      WHERE     b.organization_id = l_org_id
                            AND a.location_id = b.location_id;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        NULL;
                  END;

                  l_vendor_site_rec.vendor_id := l_vendor_id;
                  l_vendor_site_rec.vendor_site_code :=
                     rec_emp.vendor_site_code;                     --'OFFICE';
                  l_vendor_site_rec.purchasing_site_flag := NULL;
                  l_vendor_site_rec.rfq_only_site_flag := 'N';
                  l_vendor_site_rec.pay_site_flag := 'Y';
                  l_vendor_site_rec.attention_ar_flag := 'N';
                  l_vendor_site_rec.terms_date_basis := 'Invoice';

                  BEGIN
                     BEGIN
                        SELECT    gcc.SEGMENT1
                               || '.'
                               || gcc.SEGMENT2
                               || '.'
                               || DECODE (rec_emp.vendor_site_code,
                                          'OFFICE', flv.attribute5,
                                          'HOME', flv.attribute7,
                                          'PROVISIONAL', flv.attribute9)
                               || '.'
                               || gcc.SEGMENT4
                               || '.'
                               || gcc.SEGMENT5
                               || '.'
                               || gcc.SEGMENT6
                               || '.'
                               || gcc.SEGMENT7
                               || '.'
                               || gcc.segment8
                               || '.'
                               || gcc.segment9
                          INTO l_liab_accts
                          FROM financials_system_params_all fsp,
                               gl_code_combinations_kfv gcc,
                               fnd_lookup_values flv,
                               hr_operating_units hru
                         WHERE     1 = 1
                               AND fsp.org_id = l_org_id
                               AND fsp.ACCTS_PAY_CODE_COMBINATION_ID =
                                      gcc.CODE_COMBINATION_ID
                               AND hru.organization_id = fsp.org_id
                               AND lookup_type =
                                      'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                               AND flv.attribute3 = hru.name
                               AND flv.attribute1 = rec_emp.legal_entity;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           -- For Default OU case, where Default OU is not mapped against Employee's Legal ENTITY
                           --in lookup
                           SELECT    gcc.SEGMENT1
                                  || '.'
                                  || gcc.SEGMENT2
                                  || '.'
                                  || DECODE (rec_emp.vendor_site_code,
                                             'OFFICE', flv.attribute5,
                                             'HOME', flv.attribute7,
                                             'PROVISIONAL', flv.attribute9)
                                  || '.'
                                  || gcc.SEGMENT4
                                  || '.'
                                  || gcc.SEGMENT5
                                  || '.'
                                  || gcc.SEGMENT6
                                  || '.'
                                  || gcc.SEGMENT7
                                  || '.'
                                  || gcc.segment8
                                  || '.'
                                  || gcc.segment9
                             INTO l_liab_accts
                             FROM financials_system_params_all fsp,
                                  gl_code_combinations_kfv gcc,
                                  fnd_lookup_values flv,
                                  hr_operating_units hru
                            WHERE     1 = 1
                                  AND fsp.org_id = l_org_id
                                  AND fsp.ACCTS_PAY_CODE_COMBINATION_ID =
                                         gcc.CODE_COMBINATION_ID
                                  AND hru.organization_id = fsp.org_id
                                  AND lookup_type =
                                         'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                                  AND flv.attribute3 = hru.name
                                  AND hru.name = l_default_ou
                                  AND ROWNUM = 1;
                     --AND flv.attribute1 = rec_emp.legal_entity;
                     END;

                     BEGIN
                        SELECT    gcc.SEGMENT1
                               || '.'
                               || gcc.SEGMENT2
                               || '.'
                               || DECODE (rec_emp.vendor_site_code,
                                          'OFFICE', flv.attribute4,
                                          'HOME', flv.attribute6,
                                          'PROVISIONAL', flv.attribute8)
                               || '.'
                               || gcc.SEGMENT4
                               || '.'
                               || gcc.SEGMENT5
                               || '.'
                               || gcc.SEGMENT6
                               || '.'
                               || gcc.SEGMENT7
                               || '.'
                               || gcc.segment8
                               || '.'
                               || gcc.segment9
                          INTO l_prepay_accts
                          FROM financials_system_params_all fsp,
                               gl_code_combinations_kfv gcc,
                               fnd_lookup_values flv,
                               hr_operating_units hru
                         WHERE     1 = 1
                               AND fsp.org_id = l_org_id
                               AND fsp.PREPAY_CODE_COMBINATION_ID =
                                      gcc.CODE_COMBINATION_ID
                               AND hru.organization_id = fsp.org_id
                               AND lookup_type =
                                      'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                               AND flv.attribute3 = hru.name
                               AND flv.attribute1 = rec_emp.legal_entity;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           SELECT    gcc.SEGMENT1
                                  || '.'
                                  || gcc.SEGMENT2
                                  || '.'
                                  || DECODE (rec_emp.vendor_site_code,
                                             'OFFICE', flv.attribute4,
                                             'HOME', flv.attribute6,
                                             'PROVISIONAL', flv.attribute8)
                                  || '.'
                                  || gcc.SEGMENT4
                                  || '.'
                                  || gcc.SEGMENT5
                                  || '.'
                                  || gcc.SEGMENT6
                                  || '.'
                                  || gcc.SEGMENT7
                                  || '.'
                                  || gcc.segment8
                                  || '.'
                                  || gcc.segment9
                             INTO l_prepay_accts
                             FROM financials_system_params_all fsp,
                                  gl_code_combinations_kfv gcc,
                                  fnd_lookup_values flv,
                                  hr_operating_units hru
                            WHERE     1 = 1
                                  AND fsp.org_id = l_org_id
                                  AND fsp.PREPAY_CODE_COMBINATION_ID =
                                         gcc.CODE_COMBINATION_ID
                                  AND hru.organization_id = fsp.org_id
                                  AND lookup_type =
                                         'OYO_EMP_AS_VENDOR_ACCOUNT_MAP'
                                  AND flv.attribute3 = hru.name
                                  AND hru.name = l_default_ou
                                  AND ROWNUM = 1;
                     --    AND flv.attribute1 = rec_emp.legal_entity;
                     END;

                     SELECT chart_of_accounts_id
                       INTO l_coa_id
                       FROM apps.gl_sets_of_books sob, hr_operating_units hou
                      WHERE     sob.set_of_books_id = hou.set_of_books_id
                            AND hou.organization_id = l_org_id;

                     l_vendor_site_rec.accts_pay_code_combination_id :=
                        apps.gl_code_combinations_pkg.get_ccid (l_coa_id,
                                                                SYSDATE,
                                                                l_liab_accts);

                     l_vendor_site_rec.prepay_code_combination_id :=
                        apps.gl_code_combinations_pkg.get_ccid (
                           l_coa_id,
                           SYSDATE,
                           l_prepay_accts);
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        l_err_msg :=
                              'Error in generating Liability and Prepay Account Code Combination, '
                           || SUBSTR (SQLERRM, 1, 500);

                        write_log (
                           'Error in getting accounts for the employee');
                        RAISE l_exception;
                  END;

                  l_vendor_site_rec.payment_priority := NULL;
                  l_vendor_site_rec.pay_date_basis_lookup_code := NULL;
                  l_vendor_site_rec.org_id := l_org_id;
                  l_vendor_site_rec.ext_payee_rec.default_pmt_method := 'EFT';
                  l_vendor_site_rec.email_address := rec_emp.email_address;

                  apps.ap_vendor_pub_pkg.create_vendor_site (
                     p_api_version        => l_api_version,
                     p_init_msg_list      => fnd_api.g_true,
                     p_commit             => fnd_api.g_false,
                     p_validation_level   => fnd_api.g_valid_level_full,
                     x_return_status      => l_return_status,
                     x_msg_count          => l_msg_count,
                     x_msg_data           => l_msg_data,
                     p_vendor_site_rec    => l_vendor_site_rec,
                     x_vendor_site_id     => l_vendor_site_id,
                     x_party_site_id      => l_party_site_id,
                     x_location_id        => l_location_id);

                  IF NVL (l_return_status, 'E') != 'S'
                  THEN
                     write_log (
                           'Error in Vendor Site API for Employee '
                        || rec_emp.employee_name
                        || ' Employee No:- '
                        || rec_emp.employee_number);

                     FOR i IN 1 .. l_msg_count
                     LOOP
                        l_api_message :=
                              i
                           || '. '
                           || fnd_msg_pub.get (p_encoded => fnd_api.g_false)
                           || CHR (10);
                        fnd_file.put (fnd_file.LOG, l_api_message);
                     END LOOP;

                     write_log ('***************************');
                     write_log ('Output information ....');
                     write_log ('x_return_status: ' || l_return_status);
                     write_log ('x_msg_count: ' || l_msg_count);
                     write_log ('x_msg_data: ' || l_msg_data);
                     write_log ('***************************');

                     UPDATE XXOYO_EMP_AS_VENDOR_TBL
                        SET process_flag = 'E',
                            ERROR_MSG =
                                  'Error while creating Vendor Site, '
                               || l_msg_data,
                            last_update_date = SYSDATE,
                            last_updated_by = l_user_id
                      WHERE     employee_id = rec_emp.employee_id
                            AND vendor_code = rec_emp.vendor_code
                            AND vendor_site_code = rec_emp.vendor_site_code
                            AND org_id = l_org_id
                            AND process_flag = 'N'
                            AND request_id = l_request_id;
                  ELSE
                     write_log (
                           '****** Supplier and Site '
                        || rec_emp.vendor_site_code
                        || ' created for Employee '
                        || rec_emp.employee_name
                        || ' Employee No:- '
                        || rec_emp.employee_number);

                     write_log (
                           'Creating Third Party registration...'
                        || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
                     --Invoke procedure to create Third Party registration
                     XXOYO_EMP_AS_VENDOR_CREATE_PKG.XXOYO_CREATE_PARTY_REG_PRC (
                        l_vendor_id,
                        l_vendor_site_id,
                        l_org_id,
                        rec_emp.pan,
                        l_ret_code,
                        l_ret_msg);

                     UPDATE XXOYO_EMP_AS_VENDOR_TBL
                        SET process_flag = DECODE (l_ret_code, 0, 'S', 'E'),
                            ERROR_MSG =
                               DECODE (l_ret_code, 0, NULL, l_ret_msg),
                            vendor_id = l_vendor_id,
                            party_id = l_party_id,
                            vendor_site_id = l_vendor_site_id,
                            last_update_date = SYSDATE,
                            last_updated_by = l_user_id
                      WHERE     employee_id = rec_emp.employee_id
                            AND vendor_code = rec_emp.vendor_code
                            AND vendor_site_code = rec_emp.vendor_site_code
                            AND org_id = l_org_id
                            AND process_flag = 'N'
                            AND request_id = l_request_id;

                     UPDATE ap_suppliers
                        SET vendor_name_alt = rec_emp.vendor_name_alt
                      WHERE     vendor_id = l_vendor_id
                            AND segment1 = rec_emp.vendor_code
                            AND vendor_name_alt IS NULL;


                     l_load_count := l_load_count + 1;
                  END IF;
               END IF;
            ELSE
               write_log (
                     'Creating Third Party registration...'
                  || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
               --Invoke procedure to create Third Party registration
               XXOYO_EMP_AS_VENDOR_CREATE_PKG.XXOYO_CREATE_PARTY_REG_PRC (
                  rec_emp.vendor_id,
                  rec_emp.vendor_site_id,
                  l_org_id,
                  rec_emp.pan,
                  l_ret_code,
                  l_ret_msg);

               UPDATE XXOYO_EMP_AS_VENDOR_TBL
                  SET process_flag = DECODE (l_ret_code, 0, 'S', 'E'),
                      ERROR_MSG = DECODE (l_ret_code, 0, NULL, l_ret_msg),
                      vendor_id = l_vendor_id,
                      party_id = l_party_id,
                      vendor_site_id = l_vendor_site_id,
                      last_update_date = SYSDATE,
                      last_updated_by = l_user_id
                WHERE     employee_id = rec_emp.employee_id
                      AND vendor_code = rec_emp.vendor_code
                      AND vendor_site_code = rec_emp.vendor_site_code
                      AND org_id = l_org_id
                      AND process_flag = 'N'
                      AND request_id = l_request_id;
            END IF;
         EXCEPTION
            WHEN l_exception
            THEN
               UPDATE XXOYO_EMP_AS_VENDOR_TBL
                  SET process_flag = 'E',
                      ERROR_MSG = l_err_msg,
                      last_update_date = SYSDATE,
                      last_updated_by = l_user_id
                WHERE     employee_id = rec_emp.employee_id
                      AND vendor_code = rec_emp.vendor_code
                      AND vendor_site_code = rec_emp.vendor_site_code
                      AND org_id = l_org_id
                      AND process_flag = 'N'
                      AND request_id = l_request_id;
            WHEN OTHERS
            THEN
               l_err_msg :=
                  'Unexpected error occurred, ' || SUBSTR (SQLERRM, 1, 2000);

               UPDATE XXOYO_EMP_AS_VENDOR_TBL
                  SET process_flag = 'E',
                      ERROR_MSG = l_err_msg,
                      last_update_date = SYSDATE,
                      last_updated_by = l_user_id
                WHERE     employee_id = rec_emp.employee_id
                      AND vendor_code = rec_emp.vendor_code
                      AND vendor_site_code = rec_emp.vendor_site_code
                      AND org_id = l_org_id
                      AND process_flag = 'N'
                      AND request_id = l_request_id;

               write_log (
                  'Unexpected error occurred, ' || SUBSTR (SQLERRM, 1, 2000));
               l_error_count := l_error_count + 1;
               ROLLBACK;
         END;

         IF l_count = 100
         THEN
            l_count := 0;
            COMMIT;
         END IF;
      END LOOP;

      UPDATE ap_product_setup
         SET supplier_numbering_method = 'AUTOMATIC',
             next_auto_supplier_num = l_next_supplier_number,
             supplier_num_type = 'NUMERIC';

      write_log ('');
      write_log (
            'Program completes to create suppliers from employee...'
         || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         p_out_ret_msg :=
               'Unexpected error occurred while creating Vendor of an Employee, '
            || SUBSTR (SQLERRM, 1, 500);
         p_out_ret_code := 2;
         FND_FILE.PUT_LINE (FND_FILE.LOG, p_out_ret_msg);
   END XXOYO_MAIN_PRC;

   PROCEDURE XXOYO_CREATE_PARTY_REG_PRC (
      p_in_vendor_id        IN     NUMBER,
      p_in_vendor_site_id   IN     NUMBER,
      p_in_org_id           IN     NUMBER,
      p_in_pan              IN     VARCHAR2,
      p_out_ret_code           OUT NUMBER,
      p_out_ret_msg            OUT VARCHAR2)
   IS
      l_tds_regime_id         NUMBER;
      l_null_site_exists      NUMBER;
      l_user_id               NUMBER := fnd_global.user_id;
      l_login_id              NUMBER := fnd_global.login_id;
      l_check_reg             NUMBER;
      l_old_pan               VARCHAR2 (240);
      l_old_pan_reg           NUMBER;
      l_regime_code           jai_regimes.regime_code%TYPE;
      l_reporting_code_id     JAI_REPORTING_CODES.REPORTING_CODE_ID%TYPE;
      l_reporting_code        JAI_REPORTING_CODES.REPORTING_CODE%TYPE;
      l_reporting_code_desc   JAI_REPORTING_CODES.REPORTING_CODE_DESCRIPTION%TYPE;
      l_reporting_type_id     JAI_REPORTING_TYPES.REPORTING_TYPE_ID%TYPE;
      l_reporting_type        JAI_REPORTING_TYPES.REPORTING_TYPE_NAME%TYPE;
      l_reporting_usage       JAI_REPORTING_TYPES.REPORTING_USAGE%TYPE;
   BEGIN
      p_out_ret_code := 0;
      p_out_ret_msg := 'Success';

      BEGIN
         SELECT regime_id, regime_code
           INTO l_tds_regime_id, l_regime_code
           FROM apps.jai_regimes
          WHERE     regime_name = 'TDS-India'
                AND regime_type = 'W'
                AND SYSDATE BETWEEN TRUNC (effective_from)
                                AND NVL (effective_to, SYSDATE);
      EXCEPTION
         WHEN OTHERS
         THEN
            l_tds_regime_id := NULL;
      END;

      BEGIN
         SELECT reporting_type_id, reporting_type_name, reporting_usage
           INTO l_reporting_type_id, l_reporting_type, l_reporting_usage
           FROM jai_reporting_types
          WHERE     UPPER (reporting_type_name) = 'VENDOR TYPE'
                AND NVL (EFFECTIVE_TO, SYSDATE) >= SYSDATE
                AND ENTITY_CODE = 'THIRD_PARTY';
      EXCEPTION
         WHEN OTHERS
         THEN
            p_out_ret_code := 1;
            p_out_ret_msg :=
                  'Error while fetching Reporting Types, '
               || SUBSTR (SQLERRM, 1, 200);
      END;

      BEGIN
         SELECT reporting_code_id, reporting_code, reporting_code_description
           INTO l_reporting_code_id, l_reporting_code, l_reporting_code_desc
           FROM JAI_REPORTING_CODES jrc, fnd_lookup_values flv
          WHERE     jrc.reporting_type_id = l_reporting_type_id
                AND UPPER (jrc.reporting_code) = UPPER (flv.meaning)
                AND flv.enabled_flag = 'Y'
                AND NVL (flv.end_date_active, SYSDATE) >= SYSDATE
                AND flv.lookup_type = 'OYO_IN_PAN_VENDOR_TYPE_MAPPING'
                AND flv.lookup_code = NVL (SUBSTR (p_in_pan, 4, 1), 'P')
                AND NVL (jrc.EFFECTIVE_TO, SYSDATE) >= SYSDATE;
      EXCEPTION
         WHEN OTHERS
         THEN
            BEGIN
               SELECT reporting_code_id,
                      reporting_code,
                      reporting_code_description
                 INTO l_reporting_code_id,
                      l_reporting_code,
                      l_reporting_code_desc
                 FROM JAI_REPORTING_CODES jrc, fnd_lookup_values flv
                WHERE     jrc.reporting_type_id = l_reporting_type_id
                      AND UPPER (jrc.reporting_code) = UPPER (flv.meaning)
                      AND flv.enabled_flag = 'Y'
                      AND NVL (flv.end_date_active, SYSDATE) >= SYSDATE
                      AND flv.lookup_type = 'OYO_IN_PAN_VENDOR_TYPE_MAPPING'
                      AND flv.lookup_code = 'P'
                      AND NVL (jrc.EFFECTIVE_TO, SYSDATE) >= SYSDATE;
            EXCEPTION
               WHEN OTHERS
               THEN
                  p_out_ret_code := 1;
                  p_out_ret_msg :=
                        'Error while fetching Reporting Code, '
                     || SUBSTR (SQLERRM, 1, 200);
            END;
      END;

      IF l_tds_regime_id IS NULL
      THEN
         p_out_ret_code := 1;
         p_out_ret_msg := 'TDS Regime is not defined';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO l_null_site_exists
              FROM apps.jai_party_regs jpr
             WHERE     jpr.party_id = p_in_vendor_id
                   AND jpr.party_Site_id IS NULL
                   AND jpr.SUPPLIER_FLAG = 'Y';
         EXCEPTION
            WHEN OTHERS
            THEN
               l_null_site_exists := 0;
         END;

         IF l_null_site_exists = 0
         THEN
            INSERT INTO jai_party_regs (party_reg_id,
                                        party_type_code,
                                        supplier_flag,
                                        customer_flag,
                                        site_flag,
                                        party_id,
                                        party_site_id,
                                        item_category_list,
                                        org_id,
                                        creation_date,
                                        created_by,
                                        last_update_date,
                                        last_update_login,
                                        last_updated_by,
                                        record_type_code)
                 VALUES (ja.jai_party_regs_s.NEXTVAL,
                         'THIRD_PARTY',
                         'Y',                                 -- supplier_flag
                         'N',                                 -- customer_flag
                         'N',                                     -- site_flag
                         p_in_vendor_id,
                         NULL,                                -- party_site_id
                         NULL,
                         NULL, -- p_in_org_id,                                -- org_id
                         SYSDATE,
                         l_user_id,
                         SYSDATE,
                         l_login_id,
                         l_user_id,
                         'DEFINED');

            INSERT INTO jai_party_reg_lines (party_reg_id,
                                             party_reg_line_id,
                                             line_context,
                                             regime_id,
                                             registration_type_code,
                                             registration_number,
                                             default_section_code,
                                             effective_from,
                                             creation_date,
                                             created_by,
                                             last_update_date,
                                             last_update_login,
                                             last_updated_by,
                                             record_type_code)
                 VALUES (ja.jai_party_regs_s.CURRVAL,
                         apps.jai_party_reg_lines_s.NEXTVAL,
                         'REGISTRATIONS',
                         l_tds_regime_id,
                         'PAN',
                         NVL (p_in_pan, 'PANNOTAVBL'),
                         NULL,
                         TO_DATE ('01-JUL-2017', 'DD-MON-YYYY'),
                         SYSDATE,
                         l_user_id,
                         SYSDATE,
                         l_login_id,
                         l_user_id,
                         'DEFINED');

            IF     l_reporting_type_id IS NOT NULL
               AND l_reporting_code_id IS NOT NULL
            THEN
               INSERT
                 INTO jai_reporting_associations (REPORTING_ASSOCIATION_ID,
                                                  REPORTING_TYPE_ID,
                                                  REPORTING_CODE_ID,
                                                  REPORTING_TYPE_NAME,
                                                  REPORTING_USAGE,
                                                  REPORTING_CODE_DESCRIPTION,
                                                  REPORTING_CODE,
                                                  ENTITY_CODE,
                                                  ENTITY_ID,
                                                  ENTITY_SOURCE_TABLE,
                                                  REGIME_ID,
                                                  REGIME_CODE,
                                                  EFFECTIVE_FROM,
                                                  EFFECTIVE_TO,
                                                  CREATION_DATE,
                                                  CREATED_BY,
                                                  LAST_UPDATE_DATE,
                                                  LAST_UPDATE_LOGIN,
                                                  LAST_UPDATED_BY,
                                                  RECORD_TYPE_CODE,
                                                  STL_HDR_ID)
               VALUES (jai_reporting_associations_s.NEXTVAL,
                       l_reporting_type_id,
                       l_reporting_code_id,
                       l_reporting_type,
                       NULL,                              --l_reporting_usage,
                       l_reporting_code_desc,
                       l_reporting_code,
                       'THIRD_PARTY',
                       ja.jai_party_regs_s.CURRVAL,
                       'JAI_PARTY_REGS',
                       l_tds_regime_id,
                       NULL,                                  --l_regime_code,
                       SYSDATE,
                       NULL,
                       SYSDATE,
                       l_user_id,
                       SYSDATE,
                       l_login_id,
                       l_user_id,
                       'DEFINED',
                       NULL);
            END IF;
         END IF;

         BEGIN
            SELECT COUNT (1)
              INTO l_check_reg
              FROM apps.jai_party_regs jpr
             WHERE     jpr.party_id = p_in_vendor_id
                   AND jpr.party_Site_id = p_in_vendor_site_id
                   AND jpr.org_id = p_in_org_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_check_reg := 0;
         END;

         IF l_check_reg = 0
         THEN
            INSERT INTO jai_party_regs (party_reg_id,
                                        party_type_code,
                                        supplier_flag,
                                        customer_flag,
                                        site_flag,
                                        party_id,
                                        party_site_id,
                                        item_category_list,
                                        org_id,
                                        creation_date,
                                        created_by,
                                        last_update_date,
                                        last_update_login,
                                        last_updated_by,
                                        record_type_code)
                 VALUES (ja.jai_party_regs_s.NEXTVAL,
                         'THIRD_PARTY_SITE',
                         'Y',                                 -- supplier_flag
                         'N',                                 -- customer_flag
                         'Y',                                     -- site_flag
                         p_in_vendor_id,                           -- party_id
                         p_in_vendor_site_id,                 -- party_site_id
                         NULL,                           -- item_category_list
                         p_in_org_id,                                -- org_id
                         SYSDATE,
                         l_user_id,
                         SYSDATE,
                         l_login_id,
                         l_user_id,
                         'DEFINED');

            INSERT INTO jai_party_reg_lines (party_reg_id,
                                             party_reg_line_id,
                                             line_context,
                                             regime_id,
                                             registration_type_code,
                                             registration_number,
                                             default_section_code,
                                             effective_from,
                                             creation_date,
                                             created_by,
                                             last_update_date,
                                             last_update_login,
                                             last_updated_by,
                                             record_type_code)
                 VALUES (ja.jai_party_regs_s.CURRVAL,
                         apps.jai_party_reg_lines_s.NEXTVAL,
                         'REGISTRATIONS',
                         l_tds_regime_id,
                         'PAN',
                         NVL (p_in_pan, 'PANNOTAVBL'),
                         NULL,
                         TO_DATE ('01-JUL-2017', 'DD-MON-YYYY'),
                         SYSDATE,
                         l_user_id,
                         SYSDATE,
                         l_login_id,
                         l_user_id,
                         'DEFINED');

            IF     l_reporting_type_id IS NOT NULL
               AND l_reporting_code_id IS NOT NULL
            THEN
               INSERT
                 INTO jai_reporting_associations (REPORTING_ASSOCIATION_ID,
                                                  REPORTING_TYPE_ID,
                                                  REPORTING_CODE_ID,
                                                  REPORTING_TYPE_NAME,
                                                  REPORTING_USAGE,
                                                  REPORTING_CODE_DESCRIPTION,
                                                  REPORTING_CODE,
                                                  ENTITY_CODE,
                                                  ENTITY_ID,
                                                  ENTITY_SOURCE_TABLE,
                                                  REGIME_ID,
                                                  REGIME_CODE,
                                                  EFFECTIVE_FROM,
                                                  EFFECTIVE_TO,
                                                  CREATION_DATE,
                                                  CREATED_BY,
                                                  LAST_UPDATE_DATE,
                                                  LAST_UPDATE_LOGIN,
                                                  LAST_UPDATED_BY,
                                                  RECORD_TYPE_CODE,
                                                  STL_HDR_ID)
               VALUES (jai_reporting_associations_s.NEXTVAL,
                       l_reporting_type_id,
                       l_reporting_code_id,
                       l_reporting_type,
                       NULL,                              --l_reporting_usage,
                       l_reporting_code_desc,
                       l_reporting_code,
                       'THIRD_PARTY',
                       ja.jai_party_regs_s.CURRVAL,
                       'JAI_PARTY_REGS',
                       l_tds_regime_id,
                       NULL,                                  --l_regime_code,
                       SYSDATE,
                       NULL,
                       SYSDATE,
                       l_user_id,
                       SYSDATE,
                       l_login_id,
                       l_user_id,
                       'DEFINED',
                       NULL);
            END IF;
         ELSE
            BEGIN
               SELECT jprl.REGISTRATION_NUMBER, jpr.party_reg_id
                 INTO l_old_pan, l_old_pan_reg
                 FROM jai_party_regs_v jpr, jai_party_reg_lines_v jprl
                WHERE     jprl.regime_code = 'TDS-India'
                      AND jprl.registration_number IS NOT NULL
                      AND jprl.registration_type_code LIKE 'PAN%'
                      AND jpr.party_reg_id = jprl.party_reg_id
                      AND jprl.effective_to IS NULL
                      AND jpr.party_id = p_in_vendor_id
                      AND jpr.party_site_id = p_in_vendor_site_id
                      AND org_id = p_in_org_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_old_pan := NULL;
                  l_old_pan_reg := NULL;
            END;

            IF NVL (p_in_pan, 'PANNOTAVBL') <> l_old_pan
            THEN
               UPDATE jai_party_reg_lines
                  SET effective_to = TRUNC (SYSDATE) - 1,
                      last_update_date = SYSDATE
                WHERE     registration_type_code = 'PAN'
                      AND party_reg_id = l_old_pan_reg
                      AND UPPER (TRIM (registration_number)) =
                             UPPER (TRIM (l_old_pan))
                      AND effective_to IS NULL;

               INSERT INTO jai_party_reg_lines (party_reg_id,
                                                party_reg_line_id,
                                                line_context,
                                                regime_id,
                                                registration_type_code,
                                                registration_number,
                                                default_section_code,
                                                effective_from,
                                                creation_date,
                                                created_by,
                                                last_update_date,
                                                last_update_login,
                                                last_updated_by,
                                                record_type_code)
                    VALUES (l_old_pan_reg,
                            apps.jai_party_reg_lines_s.NEXTVAL,
                            'REGISTRATIONS',
                            l_tds_regime_id,
                            'PAN',
                            NVL (p_in_pan, 'PANNOTAVBL'),
                            NULL,
                            TO_DATE (SYSDATE, 'DD-MON-RRRR'),
                            SYSDATE,
                            l_user_id,
                            SYSDATE,
                            l_login_id,
                            l_user_id,
                            'DEFINED');
            END IF;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         p_out_ret_code := 1;
         p_out_ret_msg :=
               'Error while creating Third party registration, '
            || SUBSTR (SQLERRM, 1, 500);
   END XXOYO_CREATE_PARTY_REG_PRC;
END XXOYO_EMP_AS_VENDOR_CREATE_PKG;