#ifndef __observer_h__
#define __observer_h__

#include <boost/move/unique_ptr.hpp>

#include "connection.h"
#include "execution.h"

using namespace boost;
using namespace boost::movelib;

class MySqlConnection;


//                                   M Y S Q L  O B S E R V E R

class MySqlObserver
{
public:
    typedef MySqlExecution::ExecutionState ExecutionState;
 
public:
    static unique_ptr<MySqlObserver> createObserver(const char *                name,
						    ObserverType                observerType,
						    const rapidjson::Document * params,
						    MySqlConnection *           conn);
    
public:
    MySqlObserver(const char * name, const rapidjson::Document * params, MySqlConnection * conn);
    virtual ~MySqlObserver();

public:
    virtual void            startProgram(const char * programName);
    virtual ExecutionState  onEvent(MySqlExecution * execution, ExecutionState newState) = 0;
    virtual void            onEvent(AuditEventType event, const char * comment = NULL, MySqlExecution * execution = NULL) {};
    virtual void            endProgram(const char * programName);
    virtual ObserverType    getObserverType() const = 0;

protected:
    void                    getWorkingDirectory(const rapidjson::Document * params);
    string                  getProgramPath();
    
protected:
    const char *            name_;
    MySqlConnection *       conn_;
    string                  currentProgram_;
    string                  workingDirectory_;
    
};

//                                     A U D I T  O B S E R V E R

class AuditObserver : public MySqlObserver
{
public:
    AuditObserver(const char *                name,
                  const rapidjson::Document * params,
                  MySqlConnection *           conn); 
    virtual ~AuditObserver();

public:
    virtual void                 startProgram(const char * programName);
    virtual ExecutionState       onEvent(MySqlExecution * execution, ExecutionState newState);
    virtual void                 onEvent(AuditEventType event, const char * comment = NULL, MySqlExecution * execution = NULL);
    virtual void                 endProgram(const char * programName);
    virtual ObserverType         getObserverType() const  {  return AUDIT_OBS; }

private:
    bool                         prepareToAudit();
    void                         insertRecord(const char *                event,
					      const rapidjson::Document * executionDom = NULL,
					      const char *                comment = NULL);

private:
    string                       auditDatabaseName_;
    string                       auditTableName_;
    string                       auditSqlPath_;
    unique_ptr<MySqlConnection>  auditConn_;  // connection for reading and writing audit records
    string                       insertStatement_;
    bool                         isAuditing_;
};



//                                   C A P T U R E  O B S E R V E R

class CaptureObserver : public MySqlObserver
{
public:
    CaptureObserver(const char *                name,
                    const rapidjson::Document * params,
                    MySqlConnection *           conn); 
    virtual ~CaptureObserver();

public:
    virtual void              startProgram(const char * programName);
    virtual ExecutionState    onEvent(MySqlExecution * execution, ExecutionState newState);
    virtual void              endProgram(const char * programName);
    virtual ObserverType      getObserverType() const  {  return CAPTURE_OBS; }

private:
    rapidjson::Document       capturedExecutions_;
};


//                                   R E P L A Y  O B S E R V E R

class ReplayObserver : public MySqlObserver
{
public:
    ReplayObserver(const char *                name,
                   const rapidjson::Document * params,
                   MySqlConnection *           conn); 
    virtual ~ReplayObserver();

public:
    virtual void              startProgram(const char * programName);
    virtual ExecutionState    onEvent(MySqlExecution * execution, ExecutionState newState);
    virtual void              endProgram(const char * programName);
    virtual ObserverType      getObserverType() const {  return REPLAY_OBS; }

private:
    rapidjson::Document       replayExecutions_;
    int                       executionNumber_;
};


//                                    D E B U G  O B S E R V E R

class DebugObserver : public MySqlObserver
{
public:
    DebugObserver(const char *                name,
                  const rapidjson::Document * params,
                  MySqlConnection *           conn); 
    virtual ~DebugObserver();

public:
    virtual void              startProgram(const char * programName);
    virtual ExecutionState    onEvent(MySqlExecution * execution, ExecutionState newState);
    virtual void              endProgram(const char * programName);
    virtual ObserverType      getObserverType() const {  return DEBUG_OBS; }

private:
    loglevel::severity_level  priorConsoleLoglevel_;    
    loglevel::severity_level  priorFileLoglevel_;    
};

#endif // __observer_h__
