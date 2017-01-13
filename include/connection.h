#ifndef __connection_h__
#define __connection_h__

#include <rapidjson/document.h>

#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/container/vector.hpp>
#include <boost/container/deque.hpp>
#include <boost/unordered_map.hpp>
#include <boost/move/unique_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>

using std::string;
using std::ostream;
using std::stringstream;
using std::cout;
using std::endl;

using namespace rapidjson;
using namespace boost;
using namespace boost::movelib;

namespace logging = boost::log;
namespace logsource = boost::log::sources;
namespace loglevel = boost::log::trivial;
namespace sinks = boost::log::sinks;

typedef boost::log::sources::severity_logger_mt<loglevel::severity_level> SeverityLogger;
typedef sinks::synchronous_sink< sinks::text_ostream_backend > text_sink;

#define CONN_LOG(conn, level) \
   BOOST_LOG_SEV(conn->logger(), loglevel::level) << "(" << conn->getConnectionName() << ") " 

#define EX_LOG(conn, execution, level) \
   BOOST_LOG_SEV(conn->logger(), loglevel::level) \
   << "(" << conn->getConnectionName() << ":" << execution->getHandle() << ") " 

class MySqlConnectionImpl;
class MySqlExecution;
class MySqlObserver;
class ExecutionThread;


//                                   T Y P E D E F S  /  E N U M S

enum AuditEventType
{
    AUDIT_EXECUTE = 1,
    AUDIT_COMMIT,
    AUDIT_ROLLBACK
};

enum ObserverType
{
    AUDIT_OBS = 1,
    PERFORMANCE_OBS,
    DEBUG_OBS,
    CAPTURE_OBS,
    REPLAY_OBS
};


//                                   M Y S Q L  C O N N E C T I O N

class MySqlConnection
{
    friend class MySqlConnectionImpl;
    friend class MySqlExecution;
    friend class MySqlObserver;
    friend class ExecutionThread;

public:
    typedef int ExecutionHandle;
    typedef boost::container::vector<shared_ptr<MySqlExecution> > ExecutionList;
    typedef boost::container::vector<unique_ptr<MySqlObserver> >  ObserverList;

    // In async mode, these codes identify requests queued for the execution thread  
    enum RequestType
    {
        NO_REQUEST,
        EXECUTION_REQUEST,
        START_TRANSACTION_REQUEST,
        COMMIT_TRANSACTION_REQUEST,
        ROLLBACK_TRANSACTION_REQUEST,
        START_PROGRAM_REQUEST,
        END_PROGRAM_REQUEST,
        KILL_THREAD_REQUEST
    };

    // First static to be initialized
    struct ConnInitializer
    {
        ConnInitializer();
    };

public:
    static unique_ptr<MySqlConnection> createConnection(const char *  name,
                                                        const char *  databaseName,
						        const char *  statementPath,
                                                        const char *  user,
		                                        const char *  password,
                                                        const char *  host,
                                                        int           port,
                                                        const char *  socket   = NULL,
                                                        unsigned long flags    = 0,
                                                        bool          async    = false);
  
private:
    MySqlConnection(const char *  name,
                    const char *  databaseName, 
                    const char *  statementPath,
                    const char *  user,
		    const char *  password,
                    const char *  host,
                    int           port,
                    const char *  socket,
                    unsigned long flags,
                    bool          async);

// no copying allowed
private:
    MySqlConnection(const MySqlConnection & otherconn);        
    MySqlConnection & operator=(MySqlConnection & otherconn);

public:
    ~MySqlConnection();

public:
    const Document &  getStatements();

    void              startExecutionThread(); // if connection is async
    void              flushExecutionThread(RequestType requestType, int iparam = 0, const char * strparam = NULL);
    bool              isAsync() const {  return async_; }

    ExecutionHandle   execute(const char * statementName, ...);  // parameter name/value pairs
    ExecutionHandle   executeJson(const char * statementName, const Document * paramSettings, ...);
    ExecutionHandle   doExecute(MySqlExecution * execution);
    MySqlExecution *  getCompletedExecution(ExecutionHandle xh = 0);
    MySqlExecution *  findExecution(ExecutionHandle xh = 0);
    int               getReturnCode(ExecutionHandle xh = 0);
    const Document *  getResults(ExecutionHandle xh = 0);
    int               getRowCount(ExecutionHandle xh = 0);
    int               getRowsAffected(ExecutionHandle xh = 0);
    bool              assertRowsAffected(int expectedRowsAffected, ExecutionHandle xh = 0);
    bool              assertRowsReturned(int expectedRowsReturned, ExecutionHandle xh = 0);

    void              setTransactions(bool isTransactions) { isTransactions_ = isTransactions; }
    bool              isTransactions() const {  return isTransactions_; }
    int               startTransaction(const char * transactionName);
    int               commitTransaction();
    int               rollbackTransaction(const stringstream & reason);
    const string &    getCurrentTransaction() const { return transactionName_; }
    
    void              startProgram(const char * programName);
    void              endProgram(const char * programName);
    string            getCurrentProgram() const;

    void              addObserver(const char * observerName, ObserverType type, const rapidjson::Document * params=NULL);
    void              removeObserver(const char * observerName);
    bool              isReplay() const;

    int               reportError(const stringstream & errorMessage, int errorNo=1, ExecutionHandle xh=0);
    int               reportError(const string & errorMessage, int errorNo=1, ExecutionHandle xh=0);
    int               getErrorNo() const  { return errorNo_; }
    const char *      getErrorMessage() const  { return errorMessage_.c_str(); }
    MySqlExecution *  getErrorExecution() const;

    int               open();
    bool              isOpen() const;
    void              close();

    const char *      getConnectionName() {  return name_.c_str(); }
    const char *      getUser() const;
    const char *      getPassword() const;
    const char *      getHost() const;
    int               getPort() const;
    const char *      getSocket() const;

    static SeverityLogger &          logger();
    static loglevel::severity_level  getConsoleLoglevel() {  return consoleLoglevel_; }
    static void                      setConsoleLoglevel(loglevel::severity_level loglevel);
    static void                      setFileLog(const char * logPath);
    static loglevel::severity_level  getFileLoglevel() {  return fileLoglevel_; }
    static void                      setFileLoglevel(loglevel::severity_level loglevel);
      
private:
    string                           name_;
    string                           databaseName_;
    unique_ptr<MySqlConnectionImpl>  impl_;
    ExecutionList                    executions_;
    ObserverList                     observers_;
    unique_ptr<ExecutionThread>      executionThread_;
    std::vector<string>              currentProgram_;
    string                           transactionName_;
    bool                             isTransactions_;
    bool                             async_;
    int                              errorNo_;
    string                           errorMessage_;
    ExecutionHandle                  errorExecutionHandle_;

    static ConnInitializer           connIntializer_;
    static SeverityLogger            logger_;
    static shared_ptr<text_sink>     consoleSink_;
    static loglevel::severity_level  consoleLoglevel_;
    static const char *              fileLogPath_;
    static shared_ptr<text_sink>     fileSink_;
    static loglevel::severity_level  fileLoglevel_;

}; // MySqlConnection


//                                   E X E C U T I O N  T H R E A D

class ExecutionThread
{
public:
    typedef MySqlConnection::RequestType RequestType;
    typedef int RequestSequence;

    struct Request
    {
        Request(RequestType type, int iparam, const char * strparam);
        Request();
        ~Request();

        RequestType            type_;
        RequestSequence        sequence_;
        int                    iparam_;
        string                 strparam_;
        int                    rc_;
        int                    errorNo_;
        string                 errorMessage_; 

        static RequestSequence nextRequestSequence_;           
    };

    typedef boost::container::deque<Request>               RequestQueue;
    typedef boost::unordered_map<RequestSequence, Request> RequestMap;

public:
    ExecutionThread(MySqlConnection * conn);
    ~ExecutionThread();

public:
    int                       start();
    void                      run();
    void                      kill();
    RequestSequence           putRequest(RequestType type, int iparam=0, const char * strparam=NULL);
    Request                   waitForRequest(RequestSequence seq);
    bool                      isCompleted(RequestSequence seq) { return lastCompletedRequest_ >= seq; }

private:
    Request                   getRequest();  

private:
    MySqlConnection *         conn_;
    boost::thread             thread_;
    RequestQueue              requestQueue_;
    boost::mutex              requestMutex_;
    boost::condition_variable requestCv_;
    boost::mutex              completionMutex_;
    boost::condition_variable completionCv_;
    RequestMap                completedRequests_;
    RequestSequence           lastCompletedRequest_;
    bool                      running_;
    
};  // ExecutionThread

ostream & operator<<(ostream & o, const ExecutionThread::Request & request);

#endif // __connection_h__
