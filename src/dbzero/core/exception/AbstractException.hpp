// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <exception>
#include <string>
#include <vector>
#include <array>
#include <sstream>
#include <iostream>

namespace db0

{

    //#define OVERRIDE_SIGNAL_HANDLERS 1
    #define __FUNC__ __FUNCTION__

    #define PREPARE_EXCEPTION(exception, msg) { exception.setDesc(msg); exception.setFile(__FILE__); exception.setFunction(__FUNC__); exception.setLine(__LINE__); }
    #define THROW(exception, msg) { exception tmp_ex; PREPARE_EXCEPTION(tmp_ex, msg); throw tmp_ex; }
    #define THROW_ID(exception, id, msg) { exception tmp_ex(id); PREPARE_EXCEPTION(tmp_ex, msg); throw tmp_ex; }
    #define THROW_MORE(exception, msg, more_desc) { exception tmp_ex; PREPARE_EXCEPTION(tmp_ex, msg); tmp_ex.setMoreDesc(more_desc); throw tmp_ex; }
    #define RETHROW(exception, msg) { exception ___Tmpee; ___Tmpee.setDesc(msg); throw ___Tmpee; }

    #define THROWF(...) THROWF_MANY_ARGUMENTS(__VA_ARGS__)
    
    #define THROWF_MANY_ARGUMENTS(type, ...) \
        ::db0::ThrowIt<type>(__VA_ARGS__).fillBasicData(__FILE__, __FUNC__, __LINE__)

    //because of details and semantics of THROWF
    //this is valid ending of indirections
    //exception at the end will __not__ be throwed
    //because ~::db0::ThrowIt() will throw proper one
    //this should shut up dummy compiler complain about not exiting scope with proper return
    //note: you can throw here anything you want, even 1 but like Jarek said, it is blasphemy
    #define THROWF_END "\0"; throw std::exception()

    /*
    *THIS class will throw exception when automatic variable go out of scope
    * note: you can pass responsibility to another variable and arbitraly extend lifetime
    * of object owning right to throw at going out of scope
    * this is very non standard behaviour (moving to other place and then throw)
    * but it can be usefull and extends exception semantics in way of what finally operation would do
    * note: in C++ this should almost always be not needed as RAII is preffered paradigm
    **/

    template<typename T> class ThrowIt
    {
    public:
        T                  to_throw;
        std::ostringstream oss_desc,
                        oss_more_desc;
        bool               disarm_me = false,
                        desc_mode = true;
    public:
        template<typename... Args>
        ThrowIt(Args&& ...args)
            : to_throw(std::forward<Args>(args)...)
        {}
        ThrowIt(ThrowIt& rhs) = delete;
        ThrowIt& operator = (ThrowIt& rhs) = delete;

        ThrowIt(ThrowIt&& rhs)
        {
            *this = std::move(rhs);
        }
        ThrowIt& operator = (ThrowIt&& rhs)
        {
            to_throw = std::move(rhs.to_throw);
            oss_desc = std::move(rhs.oss_desc);
            oss_more_desc = std::move(rhs.oss_more_desc);
            desc_mode = rhs.desc_mode;
            rhs.disarm_me = true;
            return *this;
        }
        ~ThrowIt() noexcept(false) {
            std::exception_ptr eptr = std::current_exception();
            if(eptr && std::uncaught_exceptions()) {
                std::cerr << "\n!!!!!!\n!!!!!!\n"
                        << "About to throw, when there is other uncaught exception:\n";
                try {
                    std::rethrow_exception(eptr);
                } catch(std::exception &e) {
                    std::cerr << e.what() << std::endl;
                } catch(...) {
                    std::cerr << "Unknown other exception!\n";
                }
                std::cerr << "\n!!!!!!\n!!!!!!\n";
            }
            if(disarm_me)
                return ;
            oss_desc<<std::endl;
            oss_more_desc<<std::endl;
            to_throw.setDesc(oss_desc.str());
            to_throw.setMoreDesc(oss_more_desc.str());
            throw to_throw;
        }
        ThrowIt& fillBasicData(const char* file, const char* fun, int line){
            to_throw.setFile(file);
            to_throw.setFunction(fun);
            to_throw.setLine(line);
            return *this;
        }

        template<typename TP>
        ThrowIt& operator << (TP&& to_print) {
            try {
                (desc_mode ? oss_desc : oss_more_desc) << std::forward<TP>(to_print);
            } catch(...) {
                disarm_me = true;
                throw;
            }
            return *this;
        }

        typedef ThrowIt& (*ManipFun)(ThrowIt& ti);

        ThrowIt& operator << (ManipFun&& f){
            return f(*this);
        }

        template<typename T2>
        friend ThrowIt<T2>& descMode(ThrowIt<T2>& ti);
        template<typename T2>
        friend ThrowIt<T2>& moreDescMode(ThrowIt<T2>& ti);
    };

    template<typename T>
    ThrowIt<T>& descMode(ThrowIt<T>& ti) {
        ti.desc_mode = true;
        return ti;
    }

    template<typename T>
    ThrowIt<T>& moreDescMode(ThrowIt<T>& ti) {
        ti.desc_mode = false;
        return ti;
    }

    class TraceInfo {
    public:
        static constexpr std::size_t  callsDepth = 4096;
    private:
        std::array<void*, callsDepth> callsPtr;
        unsigned int                  callsCnt = 0;

    public:
        class Mangled{
        public:
            Mangled() = default;
            Mangled(const char* str);
            std::string module, function, offset;

            static void parse(const char* str, std::string &module, std::string &function, std::string &offset);
        };

        struct Demangled{
        public:
            Demangled();
            Demangled(const Mangled& mi);

            void init(const char* str);

            std::string module, function, offset, comment;
            bool demangle_success = false;

            //uses static thread_local variables to obtain result
            static bool demangle(const std::string& name, std::string &result, std::string &comment);
        };

    protected:

    public:
        void generateInfo();

        /*weird return type...*/
        void* const * getCallsPtrs() const ;

        unsigned int getCallsCnt() const ;

        std::vector<Mangled> getMangledDesc(unsigned int pruneTop=0) const ;

        std::vector<Demangled> getDemangledDesc(unsigned int pruneTop=0) const ;

        std::string getPrintableInfo(unsigned int pruneTop=0, bool omitLastNotDemangled=true) const ;

        //will insert info, but leave previously allocated rows intact if !shrinkWhere
        //you have been warned!
        unsigned int insertInfo(std::vector<Demangled>& where, unsigned int pruneTop=0, bool shrinkWhere=true) const ;

        static void printInfo(std::ostream& os, const std::vector<Demangled>& dem, unsigned int validEntries, bool omitLastNotDemangled=true);
    };

    class AbstractException : public std::exception {
    public:
        typedef void (*SignalHandler)(int);

        static void nullHandler(){};
        static void nullHandlerInt(int){};
    private:
        static thread_local TraceInfo                         trace_info;
        static thread_local std::vector<TraceInfo::Demangled> demangled_buffer;
        static thread_local unsigned int                      demangled_buffer_valid_entries;
        static std::terminate_handler                         stored_terminate_handler;
        static SignalHandler                                  stored_sigsegv_handler;
        static SignalHandler                                  stored_sigabrt_handler;
        static bool                                           is_signal_handler_initialized;

    public:
        static constexpr int exception_id = 0;

        static bool generateStackTrace;
        static bool reportStackTrace;
        static bool reportWhereCatched;
        static bool reportFullCatchedStackTrace;

        static const TraceInfo &lastTraceInfo();
        //this handler by the standard is __not__ required
        //to be either noexcept or [[noreturn]]
        static void terminateHandler();
        static void sigSegvHandler(int signal);
        static void sigAbortHandler(int signal);
        // should be called per target
        static void setupSignalHandler();

        AbstractException(int err_id);
        virtual ~AbstractException() throw();
        
        int getNumber() const;
        std::string getName() const;
        
        const std::string& getDesc() const;
        std::string getFullDesc () const;
        
        void setDesc(const std::string& s);
        void setDesc(char c);
        void setDesc(int val);
        
        const std::string& getMoreDesc() const;
        void setMoreDesc(const std::string& s);
        void setMoreDesc(char c);
        void setMoreDesc(int val);
        void addMoreDesc(const std::string& s);
        
        std::string getLogMsg() const;
        std::string getXmlMsg() const;
        
        const std::string& getFile() const;
        void setFile(const std::string& s);
        
        int getLine() const;
        void setLine(int s);
        
        const std::string& getFunction() const;
        void setFunction(const std::string& s);
        
        virtual const char* what() const throw() override;
        
    private:
        int err_id;
        std::string desc;
        std::string moreDesc;
        std::string file;
        int line;
        std::string function;
        mutable std::string formattedMsg;	
    };
    
    std::ostream &showStackTrace(std::ostream &os, unsigned int pruneTop = 0, bool omitLastNotDemangled = true);
    
}