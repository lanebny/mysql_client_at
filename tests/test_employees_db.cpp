#include <gtest/gtest.h>

#include <boost/regex.hpp>

#include <mysqld_error.h>

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/error/error.h>
#include <rapidjson/error/en.h>

#include "mysql_client_at/include/gtest.h"
#include "mysql_client_at/examples/employees_db.h"

namespace 
{

class EmployeesDbTest : public ::testing::Test, public MySqlGtest
{
public:
    static void SetUpTestCase()
    {
        setUpMySqlTestCase("EmployeesDbTest");
    }
    static void TearDownTestCase()
    {
        tearDownMySqlTestCase();
    }
    virtual void SetUp()
    {
        setUpMySqlTest();
    }
    virtual void TearDown()
    {
        tearDownMySqlTest();
    }
};

TEST_F(EmployeesDbTest, AddEmployee) 
{
    int rc;
    const char * errorMessage;
    cmatch regexMatch;

    if (debug_) conn_->addObserver("debug", DEBUG_OBS);
    
    rapidjson::Document auditParams;
    auditParams.SetObject();
    auditParams.AddMember("database", "employees", auditParams.GetAllocator());      
    auditParams.AddMember("table_name", "audit_test", auditParams.GetAllocator());   
    auditParams.AddMember("sql", "audit_employees.json", auditParams.GetAllocator());
    conn_->addObserver("audit", AUDIT_OBS, &auditParams);
    
    const rapidjson::Value & newEmployee = testInputDoc_["new_employee"];
    const rapidjson::Value & existingEmployee = testInputDoc_["existing_employee"];

    // Try to create an existing employee. Confirm that the attempt fails
    // and the error message looks like "get_employee_by_empno ... returned 1 row"
    rc = addEmployee(existingEmployee["emp_no"].GetInt(),
		     existingEmployee["birth_date"].GetString(),
		     existingEmployee["first_name"].GetString(),
		     existingEmployee["last_name"].GetString(),
		     existingEmployee["gender"].GetString(),
		     existingEmployee["hire_date"].GetString(),
		     existingEmployee["dept_no"].GetString(),
		     existingEmployee["salary"].GetInt(),
                     conn_.get());
    ASSERT_EQ(rc, 1) << "Expected failure (rc 1) when trying to create existing user " << existingEmployee["emp_no"].GetInt()
                     << ". Got rc " << rc;
    errorMessage = conn_->getErrorMessage();
    const regex existsRegex("get_employee_by_emp_no.+?returned 1 row");
    ASSERT_TRUE(regex_search(errorMessage, regexMatch, existsRegex))
                << "Expected failure because emp_no exists, received " << errorMessage;

    // try to assign the new employee to a department before creating him -- should fail because of FK violation
    conn_->execute("assign_employee_to_department",
                   "emp_no", newEmployee["emp_no"].GetInt(),
                   "dept_no", newEmployee["dept_no"].GetString(),
                   "from_date", "2012-12-01",
                   "to_date", "9999-01-01");
    rc = conn_->getReturnCode();
    errorMessage = conn_->getErrorMessage();
    ASSERT_EQ(rc, ER_NO_REFERENCED_ROW_2) << "Expected ER_NO_REFERENCED_ROW_2 (" << ER_NO_REFERENCED_ROW_2
                                          << ") when assigning non-existent user to dept. Got " << rc
                                          << ": " << conn_->getErrorMessage();
    const regex fkRegex("foreign key constraint fails");
    ASSERT_TRUE(regex_search(errorMessage, regexMatch, fkRegex)) << "Expected foreign-key error, received " << errorMessage;

    // Try to add an employee with an invalid hire date. Confirm that the attempt fails
    // with a "not recent" message
    rc = addEmployee(newEmployee["emp_no"].GetInt(),
		     newEmployee["birth_date"].GetString(),
		     newEmployee["first_name"].GetString(),
		     newEmployee["last_name"].GetString(),
		     newEmployee["gender"].GetString(),
		     "2010-10-02",  // reject because too far in past
		     newEmployee["dept_no"].GetString(),
		     newEmployee["salary"].GetInt(),
                     conn_.get());
    ASSERT_EQ(rc, 1) << "Expected failure (rc 1) when trying to add a user with an invalid hire date 2010-10-02"
                     << ". Got rc " << rc;
    errorMessage = conn_->getErrorMessage();
    const regex notRecentRegex("not recent");
    ASSERT_TRUE(regex_search(errorMessage, regexMatch, notRecentRegex))
                << "Expected \'...not recent\', received " << errorMessage;

    // Try to add an employee with an invalid department. Confirm that the attempt fails
    // with a "not recent" message
    rc = addEmployee(newEmployee["emp_no"].GetInt(),
		     newEmployee["birth_date"].GetString(),
		     newEmployee["first_name"].GetString(),
		     newEmployee["last_name"].GetString(),
		     newEmployee["gender"].GetString(),
		     newEmployee["hire_date"].GetString(),
		     "xxxx",  // bad department
		     newEmployee["salary"].GetInt(),
                     conn_.get());
    ASSERT_EQ(rc, 1) << "Expected failure (rc 1) when trying to add a user with an invalid hire date 2010-10-02"
                     << ". Got rc " << rc;
    errorMessage = conn_->getErrorMessage();
    const regex badDeptRegex("get_dept_by_dept_no.+?returned 0 rows");
    ASSERT_TRUE(regex_search(errorMessage, regexMatch, badDeptRegex))
                << "Expected \'get_dept_by_dept_no...returned 0 rows\', received " << errorMessage;

    // Add the employee. This should succeed
    rc = addEmployee(newEmployee["emp_no"].GetInt(),
		     newEmployee["birth_date"].GetString(),
		     newEmployee["first_name"].GetString(),
		     newEmployee["last_name"].GetString(),
		     newEmployee["gender"].GetString(),
		     newEmployee["hire_date"].GetString(),
		     newEmployee["dept_no"].GetString(),
		     newEmployee["salary"].GetInt(),
                     conn_.get());
    ASSERT_EQ(rc, 0) << "addEmployee failed (" << rc << "): " << conn_->getErrorMessage();
}

}  // namespace

int main(int argc, char **argv) 
{
    ::testing::InitGoogleTest(&argc, argv);
    MySqlGtest::analyzeProgramOptions<EmployeesDbTest>(argc, argv);
    return RUN_ALL_TESTS();
}
