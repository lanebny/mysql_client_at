#ifndef __mysql_gtest_h__
#define __mysql_gtest_h__

#include <iostream>

#include <boost/program_options.hpp>

#include <rapidjson/document.h>

#include "mysql_client_at/include/connection.h"

namespace po = boost::program_options;

using std::cerr;

//                              M Y S Q L  G T E S T

// Derive 
class MySqlGtest
{
public:
    template<typename T> static void   analyzeProgramOptions(int argc, char ** argv);
    static void                        setUpMySqlTestCase(const char * testCaseName);
    static void                        tearDownMySqlTestCase();
    virtual void                       setUpMySqlTest();
    virtual void                       tearDownMySqlTest();

public:
    static string                      testType_;
    static string                      testInputPath_;
    static rapidjson::Document         testInputDoc_;
    static string                      logPath_;
    static string                      currentProgram_;
    static bool                        debug_;
    static unique_ptr<MySqlConnection> conn_;
}; 

string                      MySqlGtest::testType_;
string                      MySqlGtest::testInputPath_;
rapidjson::Document         MySqlGtest::testInputDoc_;
string                      MySqlGtest::logPath_;
string                      MySqlGtest::currentProgram_;
bool                        MySqlGtest::debug_ = false;
unique_ptr<MySqlConnection> MySqlGtest::conn_;

template<typename T>
void
MySqlGtest::analyzeProgramOptions(int argc, char ** argv)
{
    po::options_description desc("Test options");
    desc.add_options()
        ("test_type", po::value<string>()->default_value("integration"), "either \'integration\' (connects to database) or \'unit\'")
        ("test_input", po::value<string>(), "JSON file containing test inputs")
        ("log_file", po::value<string>(), "Path of log file")
        ("debug", "Generate trace")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm); 

    if (vm.count("test_type")) 
    {
        const string & testType = vm["test_type"].as<string>();
        if (testType != "integration" && testType != "unit")
        {
            cerr << "Invalid test_type \'" << testType << "\'" << endl;
            exit(1);
        }
        T::testType_ = testType;
    } 

    if (vm.count("test_input")) 
    {
        const string & testInput = vm["test_input"].as<string>();
        T::testInputPath_ = testInput;
    }

    if (vm.count("log_file"))
    {
        const string & logPath = vm["log_file"].as<string>();
        T::logPath_ = logPath;      
    }

    if (vm.count("debug"))
    {
        T::debug_ = true;      
    }
}

// Load the test-input JSON doc, and set up a connection using
// information from doc. This connection will be used for the
// duration for the test case, and will be closed at test-case
// tear-down.
//
// Add an observer to the connection depending on the test type
// (i.e. the 'test_type' command argument). If it's integration,
// add a capture observer, which will serialize the results of
// every statement execution to a json doc. If it's unit, add a
// playback observer, which will use the captured json docs to
// provide statement results without connecting to MySQL.
void
MySqlGtest::setUpMySqlTestCase(const char * testCaseName)
{
    stringstream errorMessage;

    FILE* fp = fopen(testInputPath_.c_str(), "r"); 
    if (fp)
    {
        char readBuffer[65536];
        rapidjson::FileReadStream filestream(fp, readBuffer, sizeof(readBuffer));
        ParseResult ok = testInputDoc_.ParseStream(filestream);
        fclose(fp);
        if (!ok)
        {
            cout << "Error parsing test-input file " << testInputPath_ << ": " 
                 << GetParseError_En(ok.Code()) 
                 << " (" << ok.Offset() << ")" << endl;
            return;
        }
    }
    else
    {
        errorMessage << "Error opening test-input file " << testInputPath_;
        perror(errorMessage.str().c_str());
        return;
    }

    if (!testInputDoc_.HasMember("connection"))
    {
       cout << "No connection information found in " << testInputPath_ << endl;
       return;
    }
    const rapidjson::Value & connectionInfo = testInputDoc_["connection"];
    if (!connectionInfo.HasMember("database"))
    {
        cout << "No database specified in " << testInputPath_ << endl;
        return;
    }  
    const char * databaseName = connectionInfo["database"].GetString();
    
    if (!connectionInfo.HasMember("sql_dict"))
    {
        cout << "No SQL dictionary specified in " << testInputPath_ << endl;
        return;
    }  
    const char * sqlDict = connectionInfo["sql_dict"].GetString();
    
    if (!connectionInfo.HasMember("user"))
    {
        cout << "No user specified in " << testInputPath_ << endl;
        return;
    }  
    const char * user = connectionInfo["user"].GetString();

    char * password = NULL;
    if (connectionInfo.HasMember("password") && !connectionInfo["password"].IsNull())
      password = const_cast<char *>(connectionInfo["password"].GetString());

    char * host = NULL;
    if (connectionInfo.HasMember("host"))
      host = const_cast<char *>(connectionInfo["host"].GetString());
    
    int port = 3306;
    if (connectionInfo.HasMember("port"))
      port = connectionInfo["port"].GetInt();
     
    conn_ = MySqlConnection::createConnection(testCaseName,
					      databaseName,
 				              sqlDict,
					      user,
                                              password,
					      host,
					      port);
    if (!logPath_.empty())
        conn_->setFileLog(logPath_.c_str());

    if (!testType_.empty())
    {
        if (testType_ == "integration")
        {
            conn_->addObserver(testCaseName, CAPTURE_OBS); 
        }
        else if (testType_ == "unit")
        {
            conn_->addObserver(testCaseName, REPLAY_OBS); 
        }
    }

    conn_->startProgram(testCaseName);
}

void
MySqlGtest::tearDownMySqlTestCase()
{
    if (conn_) conn_->close();
}

void
MySqlGtest::setUpMySqlTest()
{
    if (!testInputPath_.empty())
        ASSERT_TRUE(testInputDoc_.IsObject() && !testInputDoc_.ObjectEmpty()) << "No test-input file supplied";

    if (conn_)
    {
        const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        currentProgram_ = test_info->name();
        conn_->startProgram(currentProgram_.c_str());
    }
}

void
MySqlGtest::tearDownMySqlTest()
{
    if (conn_)
    {
        if (!currentProgram_.empty())
        {
            conn_->endProgram(currentProgram_.c_str());
            currentProgram_.clear();
        }
    }
}

#endif //__mysql_gtest_h__
