#include <iostream>
#include <sstream>

#include "connection.h"
#include "employees_db.h"

using namespace std;

int
addEmployee(int               employeeNumber,
            const char *      birthDate,  // yyyy-mm-dd
	    const char *      firstName,
	    const char *      lastName,
            const char *      gender,     // M/F
            const char *      hireDate,   // yyyy-mm-dd
	    const char *      department, // dept_no
	    int               salary,
            MySqlConnection * conn)
{
    stringstream errorMessage;
    int rc;

    // confirm that no current employee has this employee number
    conn->execute("get_employee_by_emp_no", 
                  "emp_no", employeeNumber);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsReturned(0)) return 1;

    // make sure the hire date is valid and is in the recent past
    conn->execute("days_from_now",
                  "date_string", hireDate);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsReturned(1)) return 1;
    const Value & daysValue = (*conn->getResults())["rows"][0]["days"];
    if (daysValue.IsNull())
    {
        errorMessage << "Hire date " << hireDate << " is not valid";
        conn->reportError(errorMessage);
        return 1;
    }
    else if (daysValue.GetInt() > 10 || daysValue.GetInt() < -60)
    {
        errorMessage << "Hire date " << hireDate << " is not recent";
        conn->reportError(errorMessage);
        return 1;
    }
    
    // validate the department name
    conn->execute("get_dept_by_dept_no",
                  "dept_no", department);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsReturned(1)) return 1;

    // sanity-check the salary
    conn->execute("salary_range_for_dept",
                  "dept_no", department);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsReturned(1)) return 1;
    const Value & range = (*conn->getResults())["rows"][0];
    int minSalary = range["min salary"].GetInt();
    int maxSalary = range["max salary"].GetInt();
    if (salary < (minSalary - .1*minSalary) || salary > (maxSalary + .1*maxSalary))
    {
        errorMessage << "salary " << salary << " out of range for department " << department
                     << " (" << minSalary << " - " << maxSalary << ")";
        return conn->reportError(errorMessage);
    }
 
    // start a transaction, (client_xt framework rolls back automatically on error)
    rc = conn->startTransaction("Add employee");
    if (rc != 0) return rc;

    // add the employee to the employee table
    conn->execute("add_employee_to_employee_table",
                  "emp_no", employeeNumber,
                  "birth_date", birthDate,
                  "first_name", firstName,
                  "last_name", lastName,
                  "gender", gender,
                  "hire_date", hireDate);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsAffected(1)) return 1;

    // assign the employee to the department
    conn->execute("assign_employee_to_department",
                  "emp_no", employeeNumber,
                  "dept_no", department,
                  "from_date", hireDate,
                  "to_date", "9999-01-01"); 
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsAffected(1)) return 1;

    // set the employee's salary
    conn->execute("set_employee_salary",
                  "emp_no", employeeNumber,
                  "salary", salary,
                  "from_date", hireDate,
                  "to_date", "9999-01-01");
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsAffected(1)) return 1;

    // confirm that information on the employee is complete
    conn->execute("get_current_employee_info_by_emp_no",
                  "emp_no", employeeNumber);
    rc = conn->getReturnCode();
    if (rc != 0) return rc;
    if (!conn->assertRowsReturned(1)) return 1;

    // commit the transaction and return
    errorMessage << "just testing";
    rc = conn->rollbackTransaction(errorMessage);
    if (rc != 0) return rc;
    return 0;
}

