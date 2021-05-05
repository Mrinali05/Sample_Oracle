  PROCEDURE XXOYO_EMP_CREATE_PRC (p_out_ret_msg       OUT VARCHAR2,
                                   p_out_ret_code      OUT NUMBER,
                                   p_in_flow_id     IN     VARCHAR2)
   IS
      CURSOR get_emp_cur
      IS
         SELECT *
           FROM XXOYO_EMP_INTERFACE_TBL xeit
          WHERE     xeit.ics_flow_id = p_in_flow_id
                AND xeit.process_flag = 'V'
                AND xeit.worker_type = 'Employee'
                AND emp_status LIKE 'Active%'
                AND NOT EXISTS
                       (SELECT 1
                          FROM per_all_people_f papf
                         WHERE papf.employee_number = xeit.employee_number);

      l_user_id                     NUMBER := fnd_global.user_id;
      l_business_group_id           per_business_groups.name%TYPE;
      l_person_type_id              per_person_types.person_type_id%TYPE;
      l_err_msg                     VARCHAR2 (4000);
      l_gender                      VARCHAR2 (10);
      l_residential_status          VARCHAR2 (20);
      l_title                       VARCHAR2 (20);
      l_mail_type                   VARCHAR2 (30);

      x_person_id                   per_all_people_f.person_id%TYPE;
      x_assignment_id               NUMBER;
      x_per_object_version_number   NUMBER;
      x_asg_object_version_number   NUMBER;
      x_per_comment_id              NUMBER;
      x_assignment_sequence         NUMBER;
      x_per_effective_start_date    DATE;
      x_per_effective_end_date      DATE;
      x_name_combination_warning    BOOLEAN;
      x_assign_payroll_warning      BOOLEAN;
      x_orig_hire_warning           BOOLEAN;
      x_full_name                   apps.per_all_people_f.full_name%TYPE;
      x_assignment_number           apps.per_all_assignments_f.assignment_number%TYPE;
      l_rec_count                   NUMBER;
      l_cwk_exists                  NUMBER;
      l_emp_num                     VARCHAR2 (30);
   BEGIN
      p_out_ret_msg := 'Success';
      p_out_ret_code := 0;

      BEGIN
         SELECT business_group_id
           INTO l_business_group_id
           FROM per_business_groups
          WHERE                          --UPPER(name) = 'OYO BUSINESS GROUP';
                name = 'OYO Business Group';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_business_group_id := NULL;
      END;

      BEGIN
         SELECT person_type_id
           INTO l_person_type_id
           FROM per_person_types
          WHERE     business_group_id = l_business_group_id
                AND user_person_type = 'Employee';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_person_type_id := NULL;
      END;

      BEGIN
         SELECT lookup_code
           INTO l_residential_status
           FROM hr_lookups
          WHERE     lookup_type = 'IN_RESIDENTIAL_STATUS'
                AND meaning = 'Resident and ordinarily resident in India';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_residential_status := 'RO';
      END;

      fnd_file.put_line (
         fnd_file.LOG,
            'Employee creation process starts...'
         || TO_CHAR (SYSDATE, 'DD-MON-YYYY hh24:mi:ss'));

      BEGIN
         UPDATE XXOYO_EMP_INTERFACE_TBL xeit
            SET (person_id, assignment_id) =
                   (SELECT papf.person_id, paaf.assignment_id
                      FROM per_all_people_f papf, per_all_assignments_f paaf
                     WHERE     papf.person_id = paaf.person_id
                           AND papf.employee_number = xeit.employee_number
                           AND TRUNC (SYSDATE) BETWEEN TRUNC (
                                                          paaf.effective_start_date)
                                                   AND TRUNC (
                                                          paaf.effective_end_date)
                           AND ROWNUM = 1),
                LAST_UPDATE_DATE = SYSDATE,
                LAST_UPDATED_BY = l_user_id
          WHERE     xeit.ics_flow_id = p_in_flow_id
                AND xeit.process_flag = 'V'
                AND xeit.worker_type = 'Employee'
                --AND xeit.emp_status = 'Active - Payroll Eligible'
                AND EXISTS
                       (SELECT 1
                          FROM per_all_people_f papf
                         WHERE papf.employee_number = xeit.employee_number);
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;

      l_rec_count := 0;

      FOR i IN get_emp_cur
      LOOP
         l_rec_count := l_rec_count + 1;
         l_gender := NULL;
         l_title := NULL;
         l_cwk_exists := 0;
         l_emp_num := NULL;

         BEGIN
            SELECT COUNT (1)
              INTO l_cwk_exists
              FROM per_all_people_f
             WHERE npw_number = i.employee_number;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_cwk_exists := 0;
         END;

         IF l_cwk_exists = 0
         THEN
            l_emp_num := i.employee_number;
         ELSE
            l_emp_num := i.employee_number || '-1';
         END IF;

         BEGIN
            SELECT lookup_code
              INTO l_gender
              FROM hr_lookups
             WHERE     lookup_type LIKE 'SEX'
                   AND UPPER (meaning) = UPPER (i.gender);
         EXCEPTION
            WHEN OTHERS
            THEN
               l_gender := NULL;
         END;

         BEGIN
            SELECT lookup_code
              INTO l_title
              FROM hr_lookups
             WHERE     lookup_type LIKE 'TITLE'
                   AND UPPER (lookup_code) = UPPER (i.title);
         EXCEPTION
            WHEN OTHERS
            THEN
               l_title := NULL;
         END;

         BEGIN
            SELECT lookup_code
              INTO l_mail_type
              FROM hr_lookups
             WHERE     lookup_type = 'HOME_OFFICE'
                   AND meaning =
                          DECODE (i.mail_type,
                                  'Work Email', 'Office',
                                  'Office');
         EXCEPTION
            WHEN OTHERS
            THEN
               l_mail_type := 'O';
         END;

         BEGIN
            hr_employee_api.create_employee (
               p_first_name                     => i.first_name,
               p_middle_names                   => i.middle_name,
               p_last_name                      => i.last_name,
               p_employee_number                => l_emp_num,
               p_date_of_birth                  => TO_DATE (i.dob, 'DD-MON-RR'),
               p_title                          => l_title,
               p_email_address                  => i.email_address,
               p_expense_check_send_to_addres   => l_mail_type,
               p_validate                       => NULL,
               p_hire_date                      => TO_DATE (i.start_date,
                                                            'DD-MON-RR'),
               p_business_group_id              => l_business_group_id,
               p_person_type_id                 => l_person_type_id,
               p_sex                            => l_gender,
         p_ATTRIBUTE1           => i.attribute1,  --Priyanka
               p_work_telephone                 => NULL,
               p_date_employee_data_verified    => NULL,
               p_person_id                      => x_person_id,
               p_assignment_id                  => x_assignment_id,
               p_per_information4               => i.pan_number,
               p_per_information7               => l_residential_status,
               p_per_object_version_number      => x_per_object_version_number,
               p_asg_object_version_number      => x_asg_object_version_number,
               p_per_effective_start_date       => x_per_effective_start_date,
               p_per_effective_end_date         => x_per_effective_end_date,
               p_full_name                      => x_full_name,
               p_per_comment_id                 => x_per_comment_id,
               p_assignment_sequence            => x_assignment_sequence,
               p_assignment_number              => x_assignment_number,
               p_name_combination_warning       => x_name_combination_warning,
               p_assign_payroll_warning         => x_assign_payroll_warning,
               p_orig_hire_warning              => x_orig_hire_warning);

            IF x_person_id IS NOT NULL
            THEN
               UPDATE XXOYO_EMP_INTERFACE_TBL
                  SET person_id = x_person_id,
                      assignment_id = x_assignment_id,
                      LAST_UPDATE_DATE = SYSDATE,
                      LAST_UPDATED_BY = l_user_id
                WHERE     ics_flow_id = p_in_flow_id
                      AND process_flag = 'V'
                      AND worker_type = 'Employee'
                      AND emp_status LIKE 'Active%'
                      AND employee_number = l_emp_num;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_err_msg :=
                     'Error occurred while creating employee, '
                  || SUBSTR (SQLERRM, 1, 500);

               UPDATE XXOYO_EMP_INTERFACE_TBL
                  SET process_flag = 'E',
                      ERROR_MSG = l_err_msg,
                      LAST_UPDATE_DATE = SYSDATE,
                      LAST_UPDATED_BY = l_user_id
                WHERE     ics_flow_id = p_in_flow_id
                      AND process_flag = 'V'
                      AND worker_type = 'Employee'
                      AND emp_status LIKE 'Active%'
                      AND employee_number = l_emp_num;
         END;

         IF l_rec_count = 500
         THEN
            COMMIT;
            l_rec_count := 0;
         END IF;
      END LOOP;

      fnd_file.put_line (
         fnd_file.LOG,
            'Employee creation process completes...'
         || TO_CHAR (SYSDATE, 'DD-MON-YYYY hh24:mi:ss'));
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         p_out_ret_code := 2;
         p_out_ret_msg :=
               'Unexpected error occurred while creating employee record, '
            || SUBSTR (SQLERRM, 1, 500);

         UPDATE XXOYO_EMP_INTERFACE_TBL
            SET PROCESS_FLAG = 'E',
                ERROR_MSG = p_out_ret_msg,
                LAST_UPDATE_DATE = SYSDATE,
                LAST_UPDATED_BY = l_user_id
          WHERE     ics_flow_id = p_in_flow_id
                AND process_flag = 'V'
                AND worker_type = 'Employee'
                AND emp_status LIKE 'Active%';
   END;


















   //



   //







   