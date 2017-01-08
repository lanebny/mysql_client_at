#include <cstdarg>
#include <cassert>
#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/move/make_unique.hpp>
#include <boost/bind/bind.hpp>
#include <boost/core/null_deleter.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/support/date_time.hpp>

#include "connection.h"
#include "connection_impl.h"
#include "execution.h"

namespace logexpr = boost::log::expressions;


//                              M Y S Q L  C O N N E C T I O N


unique_ptr<MySqlConnection>
MySqlConnection::createConnection(const char *  name,
                                  const char *  databaseName, 
                                  const char *  statementPath,
                                  const char *  host,
                                  const char *  user,
                                  const char *  password,
                                  int           port,
                                  const char *  socket,
                                  unsigned long flags,
                                  bool          async)
{
    MySqlConnection * rawConnection = new MySqlConnection(name,
                                                          databaseName, 
                                                          statementPath,
                                                          host,
                                                          user,
                                                          password,
                                                          port,
                                                          socket,
                                                          flags,
                                                          async);
    unique_ptr<MySqlConnection> conn(rawConnection);
    if (async) conn->startExecutionThread();
    return boost::move(conn);
}

MySqlConnection::MySqlConnection(const char *  name,
                                 const char *  databaseName, 
                                 const char *  statementPath, 
                                 const char *  host,
                                 const char *  user,
                                 const char *  password,
                                 int           port,
                                 const char *  socket,
                                 unsigned long flags,
                                 bool          async)
:  name_(name),
   databaseName_(databaseName),
   isTransactions_(true),
   async_(async),
   errorNo_(0),
   errorExecutionHandle_(0)
{
    impl_ = make_unique<MySqlConnectionImpl>(this,
                                             databaseName, 
                                             statementPath,
                                             host,
                                             user,
                                             password,
                                             port,
                                             socket,
                                             flags);
}

MySqlConnection::~MySqlConnection()
{
    close();
}

const Document & 
MySqlConnection::getStatements()
{
    return impl_->getStatements();
}

void
MySqlConnection::startExecutionThread()
{
    executionThread_ = make_unique<ExecutionThread>(this);
    executionThread_->start();
}

void              
MySqlConnection::flushExecutionThread(RequestType requestType, int iparam, const char * strparam)
{
    ExecutionThread::RequestSequence seq = executionThread_->putRequest(requestType, iparam, strparam);
    executionThread_->waitForRequest(seq);
}

void
MySqlConnection::addObserver(const char * observerName, ObserverType type, const rapidjson::Document * params)
{
    unique_ptr<MySqlObserver> observer = MySqlObserver::createObserver(observerName, type, params, this);
    observers_.push_back(boost::move(observer)); 
}

bool
MySqlConnection::isReplay() const
{
    for (ObserverList::const_iterator itrobs = observers_.begin();
         itrobs != observers_.end();
         ++itrobs)
    {
        if ((*itrobs)->getObserverType() == REPLAY_OBS) return true;
    }
    return false;
}

void
MySqlConnection::startProgram(const char * programName) 
{
    if (async_) flushExecutionThread(START_PROGRAM_REQUEST, 0, programName);
    currentProgram_ = programName;
    for (ObserverList::iterator itrobs = observers_.begin();
         itrobs != observers_.end();
         ++itrobs)
    {
        (*itrobs)->startProgram(programName);
    }
}

void
MySqlConnection::endProgram(const char * programName) 
{
    if (async_) flushExecutionThread(END_PROGRAM_REQUEST, 0, programName);
    for (ObserverList::iterator itrobs = observers_.begin();
         itrobs != observers_.end();
         ++itrobs)
    {
        (*itrobs)->endProgram(programName);
    }
    currentProgram_.clear();
}

MySqlConnection::ExecutionHandle
MySqlConnection::execute(const char * statementName, ...)
{
    va_list args;
    va_start(args, statementName);

    MySqlExecution * execution = new MySqlExecution(statementName, args, this, impl_.get());
    return doExecute(execution);
}

MySqlConnection::ExecutionHandle
MySqlConnection::executeJson(const char * statementName, const Document * paramSettings ...)
{
    va_list args;
    va_start(args, paramSettings);

    MySqlExecution * execution = new MySqlExecution(statementName, args, this, impl_.get());
    execution->setParameterValues(paramSettings);
    return doExecute(execution);
}

// Execute a statement. This is a two-step process:
//
// 1) Prepare the execution up to the point where we need to talk to the server:
//    that is, verify that the statement dictionary  contains a document with 
//    the right name, compose the statement text, and populate a bindings doc 
//    from the parameter specifications and the caller's arguments
// 2) If the connection is asynchronous, queue a request to the execution thread
//    and return, otherwise go ahead and execute it
MySqlConnection::ExecutionHandle
MySqlConnection::doExecute(MySqlExecution * execution)
{
    stringstream errorMessage;

    executions_.push_back(shared_ptr<MySqlExecution>(execution));
    int rc = execution->prepareToExecute(); 
    if (rc != 0) return execution->getHandle();

    // If the connection is asynchronous, queue the prepared statement to the execution thread and return
    if (async_)
    {
        ExecutionThread::RequestSequence seq = executionThread_->putRequest(EXECUTION_REQUEST, execution->getHandle());
        execution->setRequestSequence(seq);
    }
    // ... else the connection is synchronous: send to MySql and wait for results
    else
        execution->execute();

    return execution->getHandle();
}
    
// Using the execution handle, look up the execution. If the connection is async
// wait until it is complete (i.e. wait until the execution thread's completed 
// request counter exceeds the execution's request id).
MySqlExecution *  
MySqlConnection::getCompletedExecution(ExecutionHandle xh)
{
    MySqlExecution * execution = findExecution(xh);
    assert(execution != NULL);
    if (async_ && !executionThread_->isCompleted(execution->getRequestSequence()))
       executionThread_->waitForRequest(execution->getRequestSequence());
    return execution;
}

MySqlExecution *
MySqlConnection::findExecution(ExecutionHandle xh)
{
    if (executions_.empty()) return NULL;
    if (xh == 0) return executions_.back().get();
    for (ExecutionList::iterator itr = executions_.begin();
         itr != executions_.end();
         ++itr)
    {
        if ((*itr)->getHandle() == xh) return (*itr).get();
    }
    return NULL;
}

int
MySqlConnection::getReturnCode(ExecutionHandle xh) 
{
    MySqlExecution * execution = getCompletedExecution(xh);
    EX_LOG(this, execution, trace) << "rc " << execution->getReturnCode();
    return execution->getReturnCode();
}

const Document *
MySqlConnection::getResults(ExecutionHandle xh) 
{
    MySqlExecution * execution = getCompletedExecution(xh);
    if (execution != NULL)
        return &execution->getResults();
    else
        return NULL;
}

int      
MySqlConnection::getRowCount(ExecutionHandle xh) 
{
    MySqlExecution * execution = getCompletedExecution(xh);
    if (execution != NULL)
        return execution->getRowCount();
    else
        return 0;
}
int      
MySqlConnection::getRowsAffected(ExecutionHandle xh) 
{
    MySqlExecution * execution = getCompletedExecution(xh);
    if (execution != NULL)
        return execution->getRowsAffected();
    else
        return 0;
}

// Fail if the SELECT returned an unexpected number of rows. Before inserting
// a row you can SELECT the key and assert rows-returned = 0. Similarly before
// an update, you can SELECT the key and assert rows-returned = 1.
bool
MySqlConnection::assertRowsReturned(int expectedRowsReturned, ExecutionHandle xh)
{
    stringstream errorMessage;
    MySqlExecution * execution = getCompletedExecution(xh);
    int rowsReturned = execution->getRowCount();
    if (rowsReturned == expectedRowsReturned) return true;
    errorMessage << *execution << " returned " << rowsReturned << (rowsReturned == 1 ? " row. " : " rows. ")
                 << expectedRowsReturned << " expected";
    reportError(errorMessage);
    return false;
}

// Fail if an INSERT, UPDATE or DELETE did not make the expected change. Usually
// called to assert rows-affected = 1.
bool
MySqlConnection::assertRowsAffected(int expectedRowsAffected, ExecutionHandle xh)
{
    stringstream errorMessage;

    MySqlExecution * execution = getCompletedExecution(xh);
    int rowsAffected = execution->getRowsAffected();
    if (rowsAffected == expectedRowsAffected) return true;
    errorMessage << *execution << " affected " << rowsAffected << (rowsAffected == 1 ? " row. " : " rows. ")
                 << expectedRowsAffected << " expected";
    reportError(errorMessage);
    return false;
}

int 
MySqlConnection::startTransaction(const char * transactionName)
{
    if (!isTransactions()) return 0;
    if (async_) flushExecutionThread(START_TRANSACTION_REQUEST, 0, transactionName);

    CONN_LOG(this, info) << "Starting transaction " << transactionName;

    stringstream errorMessage;

    if (!impl_->isAutoCommit())
    {
        errorMessage << "Attempt to start transaction " << transactionName
                     << " while " << transactionName_ << " in progress";
        return reportError(errorMessage);         
    }
    int rc = impl_->setAutoCommit(false);
    if (rc == 0) 
    {
        transactionName_ = transactionName;
    }
    return rc;
}

int 
MySqlConnection::commitTransaction()
{
    if (!isTransactions()) return 0;
    if (async_) flushExecutionThread(COMMIT_TRANSACTION_REQUEST);

    stringstream errorMessage;

    if (impl_->isAutoCommit())
    {
        errorMessage << "Commit called with no transaction in progress";
        return reportError(errorMessage);         
    }
    int rc = impl_->commit();
    if (rc == 0) 
    {
        CONN_LOG(this, info) << "Committed transaction " << transactionName_;
        transactionName_.clear();
    }
    return rc;
}

int
MySqlConnection::rollbackTransaction(const stringstream & reason)
{
    if (!isTransactions()) return 0;
    if (impl_ == NULL || impl_->isAutoCommit()) return 0;
    if (async_) flushExecutionThread(ROLLBACK_TRANSACTION_REQUEST);

    stringstream errorMessage;
    int rc = impl_->rollback();
    if (rc == 0) 
    {
        CONN_LOG(this, info) << "Rolled back transaction " << transactionName_ 
                             << ": " << reason.str();
        transactionName_.clear();
    }
    return rc;
}

int
MySqlConnection::open()
{
    return impl_->open();
}

void
MySqlConnection::close()
{
    if (executionThread_) 
        executionThread_->kill();
    impl_->close();
}

bool
MySqlConnection::isOpen() const
{
    return impl_->isOpen_;
}

const char *      
MySqlConnection::getUser() const
{
    return impl_->user_;
}

const char *      
MySqlConnection::getPassword() const
{
    return impl_->password_;
}

const char *      
MySqlConnection::getHost() const
{
    return impl_->host_;
}

int      
MySqlConnection::getPort() const
{
    return impl_->port_;
}

const char *      
MySqlConnection::getSocket() const
{
    return impl_->socket_;
}

int
MySqlConnection::reportError(const stringstream & errorMessage, int errorNo, ExecutionHandle xh)
{
    return reportError(errorMessage.str(), errorNo, xh);
}

int
MySqlConnection::reportError(const string & errorMessage, int errorNo, ExecutionHandle xh)
{
    errorNo_ = errorNo;
    errorMessage_ = errorMessage;
    errorExecutionHandle_ = xh;
    CONN_LOG(this, error) <<  errorMessage_;
    stringstream rollbackReason;
    rollbackReason << "execution failed";
    rollbackTransaction(rollbackReason);
    return errorNo;
}

// Set up a logger and add a default sink that writes error messages 
// to the console and filters out the rest. Attaching a debug observer 
// will remove the filtering, so that all log entries go to the console
SeverityLogger &
MySqlConnection::logger()
{
    static bool initialized = false;
    if (!initialized)
    {
        initialized = true;
        logging::add_common_attributes();
        boost::shared_ptr<std::ostream> stream(&std::clog, boost::null_deleter());
        consoleSink_->locked_backend()->add_stream(stream);
        consoleSink_->set_filter
        (
           loglevel::severity >= getConsoleLoglevel()
        );
        logging::core::get()->add_sink(consoleSink_);
    }
    return logger_;
}

// Modify the severity threshold for displaying messages on the console.
// Called by the debug observer to remove severity filtering (set threshold 
// to trace).
void                     
MySqlConnection::setConsoleLoglevel(loglevel::severity_level loglevel)
{
    if (loglevel == consoleLoglevel_) return;
    consoleLoglevel_ = loglevel;
    consoleSink_->reset_filter();
    consoleSink_->set_filter
    (
       loglevel::severity >= getConsoleLoglevel()
    );
}

// Set the file sink to write to the caller's file. Set filter threshold to info.
// Format with timestamp and level.
void
MySqlConnection::setFileLog(const char * logPath)
{
    fileLogPath_ = logPath;
    fileSink_->locked_backend()->add_stream(boost::make_shared<std::ofstream>(fileLogPath_));
    fileSink_->set_filter
    (
       loglevel::severity >= getFileLoglevel()
    );
    fileSink_->set_formatter
    (
        logexpr::format("%1% %2%: %3%")
            % logexpr::format_date_time<boost::posix_time::ptime>("TimeStamp", "%H:%M:%S.%f")
            % loglevel::severity
            % logexpr::smessage
    );
    logging::core::get()->add_sink(fileSink_);
}

// Modify the severity threshold for writing messages to the log file.
// Called by the debug observer to remove severity filtering (set threshold 
// to trace).
void                     
MySqlConnection::setFileLoglevel(loglevel::severity_level loglevel)
{
    if (loglevel == fileLoglevel_) return;
    fileLoglevel_ = loglevel;
    fileSink_->reset_filter();
    fileSink_->set_filter
    (
       loglevel::severity >= getFileLoglevel()
    );
}


//                              E X E C U T I O N  T H R E A D

int ExecutionThread::Request::nextRequestSequence_ = 1;

ExecutionThread::ExecutionThread(MySqlConnection * conn)
:  conn_(conn),
   lastCompletedRequest_(0),
   running_(false)
{
}

ExecutionThread::~ExecutionThread()
{
    kill();
}

void
ExecutionThread::kill()
{
    if (running_)
    {
        putRequest(MySqlConnection::KILL_THREAD_REQUEST);
        thread_.join();
    }
}

int
ExecutionThread::start()
{
    thread_ = boost::thread(boost::bind(ExecutionThread::run, this));
}

void
ExecutionThread::run()
{
    CONN_LOG(conn_, info) << "Execution thread running";
    conn_->impl_->startMySqlThread();
    running_ = true;
    while (running_)
    {
        Request request = getRequest(); // blocks if queue is empty
        CONN_LOG(conn_, info) << "Received request " << request;
        switch (request.type_)
        {
            // Retrieve the prapared statement and send it to MySql
            case MySqlConnection::EXECUTION_REQUEST:
            {
                MySqlExecution * execution = conn_->findExecution(request.iparam_);
                assert(execution != NULL);
                execution->execute();
                EX_LOG(conn_, execution, info) << "Request " << request.sequence_ << ": async execution complete ";
                request.rc_ = execution->rc_;
                request.errorNo_ = execution->errorNo_;
                request.errorMessage_ = execution->errorMessage_;
                break;
            }

            // Transactions and programs are handled synchronously.
            // These requests are used by the thread flusher to make sure
            // all prior executions are complete.
            case MySqlConnection::START_TRANSACTION_REQUEST:
            case MySqlConnection::COMMIT_TRANSACTION_REQUEST:
            case MySqlConnection::ROLLBACK_TRANSACTION_REQUEST:
            case MySqlConnection::START_PROGRAM_REQUEST:
            case MySqlConnection::END_PROGRAM_REQUEST:
                break;

            case MySqlConnection::KILL_THREAD_REQUEST:
                running_ = false;
                break;
        }

	// update last-completed sequence and notify any threads awaiting completion of a request
        {
            boost::lock_guard<mutex> lock(completionMutex_);
            lastCompletedRequest_ = request.sequence_;
            completedRequests_[request.sequence_] = boost::move(request);
        }
        completionCv_.notify_all();
    }  
    conn_->impl_->endMySqlThread();
    CONN_LOG(conn_, info) << "Execution thread terminated" << endl;  
}

// The main thread queues a request and notifies the request condition-variable,
// which will wake up the execution thread if it's waiting. Returns the sequence
// number assigned to the request
ExecutionThread::RequestSequence
ExecutionThread::putRequest(RequestType type, int iparam, const char * strparam)
{
    RequestSequence seq;
    {
        boost::lock_guard<mutex> lock(requestMutex_);
        requestQueue_.push_front(Request(type, iparam, strparam));
        seq = requestQueue_.front().sequence_;
    }
    requestCv_.notify_one();
    return seq;
}

// The execution thread waits until there is a request in the queue.
ExecutionThread::Request
ExecutionThread::getRequest()
{
    boost::unique_lock<boost::mutex> lock(requestMutex_);
    while (requestQueue_.empty())
        requestCv_.wait(lock);
    Request request = boost::move(requestQueue_.back());
    requestQueue_.pop_back();
    return boost::move(request);
}

// The main thread waits until the execution thread processes 
// a specific request. The caller passes in the sequence number
// assigned to the request, and blocks as long as 'lastCompletedRequest_'
// is less than request. It waits on the condition variable that
// is notified each time a request is processed.
ExecutionThread::Request
ExecutionThread::waitForRequest(RequestSequence seq)
{
    if (seq > lastCompletedRequest_)
    {
        boost::unique_lock<boost::mutex> lock(completionMutex_);
        while (seq > lastCompletedRequest_)
            completionCv_.wait(lock);
    }
    RequestMap::iterator itr = completedRequests_.find(seq);
    assert(itr != completedRequests_.end());
    return (*itr).second;
}



ExecutionThread::Request::Request(RequestType type, int iparam=0, const char * strparam=NULL)
:  type_(type),
   sequence_(nextRequestSequence_++),
   iparam_(iparam),
   rc_(0),
   errorNo_(0)
{
    if (strparam != NULL) strparam_ = string(strparam);
}

ExecutionThread::Request::Request()
:  type_(MySqlConnection::NO_REQUEST),
   sequence_(0),
   iparam_(0),
   rc_(0),
   errorNo_(0)
{
}

ExecutionThread::Request::~Request()
{
}

ostream & operator<<(ostream & o, const ExecutionThread::Request & request)
{
    switch (request.type_)
    {
        case MySqlConnection::EXECUTION_REQUEST:
            return o << request.sequence_ << " EXECUTION_REQUEST " << ": execution " << request.iparam_;
        case MySqlConnection::START_TRANSACTION_REQUEST:
            return o << request.sequence_ << " START_TRANSACTION_REQUEST "  << ": " << request.strparam_;
        case MySqlConnection::COMMIT_TRANSACTION_REQUEST:
            return o << request.sequence_ << " COMMIT_TRANSACTION_REQUEST " << ": " << request.strparam_;
        case MySqlConnection::ROLLBACK_TRANSACTION_REQUEST:
            return o << request.sequence_ << " ROLLBACK_TRANSACTION_REQUEST " << ": " << request.strparam_;
        case MySqlConnection::START_PROGRAM_REQUEST:
            return o << request.sequence_ << " START_PROGRAM_REQUEST " << ": " << request.strparam_;
        case MySqlConnection::END_PROGRAM_REQUEST:
            return o << request.sequence_ << " END_PROGRAM_REQUEST " << ": " << request.strparam_;
        case MySqlConnection::KILL_THREAD_REQUEST:
            return o << request.sequence_ << " KILL_THREAD_REQUEST ";

    }
}

// Performs setup before any other statics are initialized. 
// Sets up locale so that file log sinks can be cleaned up properly.
// http://www.boost.org/doc/libs/1_56_0/libs/log/doc/html/log/rationale/why_crash_on_term.html
MySqlConnection::ConnInitializer::ConnInitializer()
{
    boost::filesystem::path::imbue(std::locale("C"));
}

SeverityLogger MySqlConnection::logger_;
shared_ptr<text_sink> MySqlConnection::consoleSink_ = boost::make_shared<text_sink>();
loglevel::severity_level MySqlConnection::consoleLoglevel_ = loglevel::warning;
const char * MySqlConnection::fileLogPath_ = NULL;
shared_ptr<text_sink> MySqlConnection::fileSink_ = boost::make_shared<text_sink>();
loglevel::severity_level MySqlConnection::fileLoglevel_ = loglevel::info;
