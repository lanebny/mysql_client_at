find_package( Boost REQUIRED COMPONENTS log thread regex program_options filesystem )
add_definitions(-DBOOST_LOG_DYN_LINK)
include_directories("../include" "/usr/include/mysql" "../rapidjson/include/" ${Boost_INCLUDE_DIRS})
file(GLOB SOURCES "*.cpp" "*.h")
find_library(MYSQL_CLIENT
	NAMES mysqlclient.dll
	HINTS "${CMAKE_PREFIX_PATH}/usr/lib/mysql")
add_library(mysql_client_at STATIC ${SOURCES})
target_link_libraries(mysql_client_at ${MYSQL_CLIENT} ${Boost_LIBRARIES})
				
