#ifndef __EMPLOYEES_H__
#define __EMPLOYEES_H__

int
addEmployee(int               employeeNumber,
            const char *      birthDate,  // "yyyy-mm-dd"
	    const char *      firstName,
	    const char *      lastName,
            const char *      gender,     // "yyyy-mm-dd"
            const char *      hireDate,
	    const char *      department,
	    int               salary,
            MySqlConnection * conn);

#endif // __EMPLOYEES_H__
