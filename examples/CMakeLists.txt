set(EMPLOYEES_EXAMPLE ${SOURCES} "employees_db.cpp")
include_directories("../include" "../rapidjson/include")
add_library(employees_db ${EMPLOYEES_EXAMPLE})
target_link_libraries(employees_db mysql_client_at)
