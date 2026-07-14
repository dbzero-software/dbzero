// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/exception/AbstractException.hpp>

#include <iostream>
#include <typeinfo>
#include <mutex>
#include <cstddef>
#include <csignal>
#include <cstring>
#ifdef  __linux__
#include <execinfo.h>
#include <cxxabi.h>
#endif
/**
 * add flag
 * -D OVERRIDE_SIGNAL_HANDLERS
 * into cmake / make to enable signal override
 */
#ifndef  OVERRIDE_SIGNAL_HANDLERS

	#define DONT_YOU_EVEN_THINK_ABOUT_THIS 0
	#define OVERRIDE_SIGNAL_HANDLERS  DONT_YOU_EVEN_THINK_ABOUT_THIS

#endif

#ifndef  OVERRIDE_EXCEPTIONS_HANDLERS

	#define OVERRIDE_EXCEPTIONS_HANDLERS  0

#endif

// Cross-platform unused variable attribute
#ifdef __GNUC__
    #define UNUSED_VAR [[gnu::unused]]
#elif defined(_MSC_VER)
    #define UNUSED_VAR __pragma(warning(suppress: 4101))
#else
    #define UNUSED_VAR
#endif

//#define _GNU_SOURCE 1
//#include <dlfcn.h>

using namespace db0;
using namespace std;

TraceInfo::Mangled::Mangled(const char* str){
    parse(str, module, function, offset);
}

void TraceInfo::Mangled::parse(const char* str, std::string &module, std::string &function, std::string &offset){
    //./module(function+0x15c) [0x8048a6d]
    const char *p;

    module.clear();
    function.clear();
    offset.clear();

    module.reserve(128);
    for(p = str; *p && *p!='('; ++p){
        module += *p;
    }

    if(*p!='('){
        return ;
    }

    function.reserve(256);

    for(++p; *p && *p!='+' && *p!=')'; ++p){
        function += *p;
    }

    if(*p!='+'){
        return ;
    }

    offset.reserve(64);
    for(++p; *p && *p!=')'; ++p){
        offset += *p;
    }
}

TraceInfo::Demangled::Demangled(){
    comment.reserve(128);
}

void TraceInfo::Demangled::init(const char *str){
    //preallocated string
    static thread_local std::string helpString(4096, '\0');

    Mangled::parse(str, module, helpString, offset);
    demangle_success = demangle(helpString, function, comment);
}

TraceInfo::Demangled::Demangled(const Mangled& mi)
    : module(mi.module)
    , offset(mi.offset)
{
    demangle_success = demangle(mi.function, function, comment);
}

bool TraceInfo::Demangled::demangle(const std::string& name, string &result, std::string &comment){
    //data holder destroys memory it holds eventually...
    struct DataHolder{
        std::size_t length;
        char *buffer;

        DataHolder()
            : length(4096)
            , buffer(reinterpret_cast<char*>(std::malloc(4096)))
        {}
        ~DataHolder(){
            std::free(buffer);
        }
    };
    static thread_local DataHolder holder;

    int status[1];
    #ifdef  __linux__
        char *res = abi::__cxa_demangle(name.c_str(), holder.buffer, &holder.length, status);
    #else
        char *res = new char[name.length() + 1];
        strcpy(res, name.c_str());
    #endif
    //__cxa_demangle may realloc buffer...
    if(res)
        holder.buffer = res;

    if(!res || !*res){
        //sorry, no abi info for you - some error occured
        //at least store mangled name
        result = name;
        //clamp end status, according to documentation
        //*status should be in set {-3, -2, -1, 0}, but better be sure of it...
        switch(*status){
        case -3:
            comment = "Name was not correctly demangled, reason : one of the arguments is invalid";
            break ;
        case -2:
            comment = "Name was not correctly demangled, reason : mangled_name is not a valid name under the C++ ABI mangling rules";
            break;
        case -1:
            comment = "Name was not correctly demangled, reason : a memory allocation failiure occurred";
            break;
        default:
            comment = "Name was not correctly demangled, reason is unknown";
            break;
        }
        return false;
    }

    //buffer will contain result anyway...
    result = holder.buffer;
    comment = "Name was correctly demangled";
    return true;
}
/*
std::vector<TraceInfo::Demangled> TraceInfo::getDemangledDesc(const std::vector<Mangled> &mangledInfo) const {
    std::vector<Demangled> res;

    for(auto &mi : mangledInfo){
        res.push_back(Demangled(mi));
    }

    return res;
}
*/
void TraceInfo::generateInfo(){
    #ifdef  __linux__
        callsCnt = backtrace(callsPtr.data(), callsDepth);
    #else
        callsCnt = 0;
    #endif
}

void* const * TraceInfo::getCallsPtrs() const {
    return callsPtr.data();
}

unsigned int TraceInfo::getCallsCnt() const {
    return callsCnt;
}

std::vector<TraceInfo::Mangled> TraceInfo::getMangledDesc(unsigned int pruneTop) const {
    std::vector<Mangled> res;
    #ifdef  __linux__
        char** mangledSymbols = backtrace_symbols(callsPtr.data(), callsCnt);

        for(unsigned int i(pruneTop); i<callsCnt; ++i){
            res.push_back(Mangled(mangledSymbols[i]));
        }

        std::free(mangledSymbols);
    #endif
    return res;
}

std::vector<TraceInfo::Demangled> TraceInfo::getDemangledDesc(unsigned int pruneTop) const {
    std::vector<Demangled> dem;

    insertInfo(dem, pruneTop, true);

    return dem;
}

std::string TraceInfo::getPrintableInfo(unsigned int pruneTop, bool omitLastNotDemangled) const {
    std::ostringstream oss;
    auto v = getDemangledDesc(pruneTop);
    printInfo(oss, v, v.size(), omitLastNotDemangled);
    return oss.str();
}

unsigned int TraceInfo::insertInfo(std::vector<Demangled> &where, unsigned int pruneTop, bool shrinkWhere) const{

    if(pruneTop >= callsCnt){
        if(shrinkWhere)
            where.resize(0);
        return 0;
    }

    //hudge pain of execution time...
    //3-4 ms per call...
    //thats eternity
    #ifdef  __linux__
        char** mangledSymbols = backtrace_symbols(callsPtr.data(), callsCnt);

        if(shrinkWhere || (callsCnt-pruneTop)>where.size())
            where.resize(callsCnt - pruneTop);

        for(unsigned int i(pruneTop), j(0); i<callsCnt; ++i, ++j){
            where[j].init(mangledSymbols[i]);
        }

        std::free(mangledSymbols);
    #endif
    return callsCnt-pruneTop;
}

void TraceInfo::printInfo(std::ostream& os, const std::vector<Demangled>& demangledInfo, unsigned int validEntries, bool omitLastNotDemangled){

    os << "\n\n_________STACK TRACE:__________\n\n";

    unsigned int top = validEntries;

    if(omitLastNotDemangled){
        int insiderTop = validEntries;
        for(--insiderTop; insiderTop>=0; --insiderTop){
            if(demangledInfo[insiderTop].demangle_success){
                break;
            }
        }

        top = (unsigned int)(insiderTop + 1);
    }

    for(unsigned int i(0); i<demangledInfo.size() && i<top; ++i){
        auto &di = demangledInfo[i];

        if(di.demangle_success){
            os << "    " << di.module << "  " << di.function << "\n";
        } else {
            os << "    " << di.module << "(" << di.function << "+" << di.offset << ")" << "\t" << di.comment << "\n";
        }
    }

    if(top<validEntries){
        auto &di = demangledInfo[top];

        os << "Names below were not correctly demangled, last such name was: \n"
           << "    " << di.module << "(" << di.function << "+" << di.offset << ")\n";
    }

    os << "\n\n";
}



template<typename T>
std::string toString(const T &val) {
	stringstream ss;
	ss << val;
	return ss.str();
}

thread_local TraceInfo                         AbstractException::trace_info;
thread_local std::vector<TraceInfo::Demangled> AbstractException::demangled_buffer(TraceInfo::callsDepth);
thread_local unsigned int                      AbstractException::demangled_buffer_valid_entries;


bool AbstractException::is_signal_handler_initialized = false;
std::terminate_handler AbstractException::stored_terminate_handler
    = &db0::AbstractException::nullHandler;
db0::AbstractException::SignalHandler db0::AbstractException::stored_sigsegv_handler
    = &db0::AbstractException::nullHandlerInt;
db0::AbstractException::SignalHandler db0::AbstractException::stored_sigabrt_handler
    = &db0::AbstractException::nullHandlerInt;
#if OVERRIDE_SIGNAL_HANDLERS
    static struct SignalHandlerInitializer
    {
        SignalHandlerInitializer()
        {
            AbstractException::setupSignalHandler();
        }
    } s_signal_handler_initializer;
#endif

//#ifndef NDEBUG && ( ! OVERRIDE_SIGNAL_HANDLERS

#if  OVERRIDE_EXCEPTIONS_HANDLERS
	bool                                           AbstractException::generateStackTrace = true;
	bool                                           AbstractException::reportStackTrace = true;
	bool                                           AbstractException::reportWhereCatched = false;
	bool                                           AbstractException::reportFullCatchedStackTrace = false;  //even under normal debug circumstances this rather will NOT be needed
#else
	bool                                           AbstractException::generateStackTrace = false;
	bool                                           AbstractException::reportStackTrace = false;
	bool                                           AbstractException::reportWhereCatched = false;
	bool                                           AbstractException::reportFullCatchedStackTrace = false;
#endif

const TraceInfo &AbstractException::lastTraceInfo(){
    return trace_info;
}

void AbstractException::terminateHandler(){
    static std::mutex                           m;
    //terminate may be called in multithread environment!
    UNUSED_VAR std::lock_guard<std::mutex> guard(m);
    TraceInfo                                   ti;

    ti.generateInfo();

    std::cerr << "\n\ndb0::AbstractException::terminateHandler() called from context:" << ti.getPrintableInfo(0);

    std::exception_ptr eptr(current_exception());

    if(!eptr){
        //the standard way
        std::cerr << "Terminate called __NOT__ due to exception.\n";
        stored_terminate_handler();
    }

    std::cerr << "Terminate called due to exception : \n";

    try{
        std::rethrow_exception(eptr);
    } catch (std::exception& e){
        std::cerr << e.what()<<"\n";
    } catch (...) {
        std::cerr << "unknown other exception\n";
    }
    stored_terminate_handler();
}

void AbstractException::sigSegvHandler(int signal){
    static std::mutex                           m;
    //terminate may be called in multithread environment!
    UNUSED_VAR std::lock_guard<std::mutex> guard(m);
    TraceInfo                                   ti;

    ti.generateInfo();
    std::cerr << "\n\ndb0::AbstractException::sigSegvHandler(int) called from context:\n" << ti.getPrintableInfo(0);
    if (!stored_sigsegv_handler) {
        std::signal(SIGSEGV, SIG_DFL);
        std::raise(SIGSEGV);
    }
    else {
        stored_sigsegv_handler(signal);
    }
}

void AbstractException::sigAbortHandler(int signal)
{
    std::cerr << "\n\ndb0::AbstractException::sigAbortHandler(int) called from context:\n";
    if (!stored_sigabrt_handler) {
        std::signal(SIGABRT, SIG_DFL);
        std::raise(SIGABRT);
    }
    else {
        stored_sigabrt_handler(signal);
    }
}

void AbstractException::setupSignalHandler()
{
    if (is_signal_handler_initialized) {
        return;
    }
    stored_terminate_handler = std::set_terminate(AbstractException::terminateHandler);
    stored_sigsegv_handler = std::signal(SIGSEGV, db0::AbstractException::sigSegvHandler);
    stored_sigabrt_handler = std::signal(SIGABRT, db0::AbstractException::sigAbortHandler);
    is_signal_handler_initialized = true;
}

AbstractException::AbstractException(int err_id)
	: err_id(err_id)
{
    if(generateStackTrace)
        trace_info.generateInfo();
}

AbstractException::~AbstractException() throw()
{
    if(!generateStackTrace || !reportWhereCatched){
        return ;
    }

    trace_info.generateInfo();
#ifdef NDEBUG
    demangled_buffer_valid_entries = trace_info.insertInfo(demangled_buffer, 0, false);
#else
    demangled_buffer_valid_entries = trace_info.insertInfo(demangled_buffer, 3, false);
#endif
    std::cerr<<"\nReporting possible exception catch at:";
    if(reportFullCatchedStackTrace){
        trace_info.printInfo(std::cerr, demangled_buffer, demangled_buffer_valid_entries);
    } else {
        trace_info.printInfo(std::cerr, demangled_buffer, 6);
    }
}

const std::string& AbstractException::getDesc() const {
	return desc;
}

int AbstractException::getNumber() const {
	return this->err_id;
}

std::string AbstractException::getFullDesc () const {
	if (moreDesc=="") {
		return desc;
	}
    std::stringstream _str;
    _str << desc << "," << moreDesc;
    return _str.str();
}

void AbstractException::setDesc(const std::string& s) {
	desc = s;
}

void AbstractException::setDesc(int val) {
	desc = toString<int>(val);
}

void AbstractException::setDesc(char val) {
	char tmp[2];
	tmp[0] = val;
	tmp[1] = 0;
	desc = string(tmp);
}

const std::string& AbstractException::getMoreDesc() const {
	return moreDesc;
}

void AbstractException::setMoreDesc(const std::string& s) {
	moreDesc = s;
}

void AbstractException::setMoreDesc(int val) {
	moreDesc = toString<int>(val);
}

void AbstractException::addMoreDesc(const std::string& s) {
	moreDesc += s;
}

void AbstractException::setMoreDesc(char val) {
	char tmp[2];
	tmp[0] = val;
	tmp[1] = 0;
	moreDesc = string(tmp);
}

std::string AbstractException::getLogMsg() const {
	stringstream ss;
	ss << "Exception " << getName()
		<< " thrown in function " << function
		<< " at " <<  file << ", line " << line << ": "
		<< desc;
    if(!moreDesc.empty()) {
        ss << ": " << moreDesc;
    }

    if(reportStackTrace && generateStackTrace){
        demangled_buffer_valid_entries = trace_info.insertInfo(demangled_buffer, 0, false);
        trace_info.printInfo(ss, demangled_buffer, demangled_buffer_valid_entries);
    }

    return ss.str();
}

// TODO: implement method body ;)
std::string AbstractException::getXmlMsg() const {
	return string("");
}

const std::string& AbstractException::getFile() const {
	return file;
}

void AbstractException::setFile(const std::string& s) {
	file = s;
}

int AbstractException::getLine() const {
	return line;
}

void AbstractException::setLine(int s) {
	line = s;
}

const std::string& AbstractException::getFunction() const {
	return function;
}

void AbstractException::setFunction(const std::string& s) {
	function = s;
}

const char* AbstractException::what() const throw() {
    formattedMsg = getLogMsg();
	return formattedMsg.c_str();
}

string AbstractException::getName() const {
	return typeid(*this).name();
}

std::ostream &db0::showStackTrace(std::ostream &os, unsigned int pruneTop, bool omitLastNotDemangled)
{
    TraceInfo ti;
    ti.generateInfo();
    os << ti.getPrintableInfo(pruneTop, omitLastNotDemangled);
    return os;
}
