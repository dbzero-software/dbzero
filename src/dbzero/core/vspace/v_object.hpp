#pragma once

#include <dbzero/core/vspace/v_ptr.hpp>
#include <dbzero/core/metaprog/int_seq.hpp>
#include <dbzero/core/metaprog/last_type_is.hpp>

namespace db0
    
{

    struct tag_verified {};

    /**
     * Base class for vspace-mapped objects
     * @tparam T container object type
     */
    template <typename T, std::uint32_t SLOT_NUM, unsigned char REALM_ID>
    class v_object
    {
    public:
        using c_type = T;
        using ptr_t = v_ptr<c_type, SLOT_NUM, REALM_ID>;        

        v_object() = default;

        v_object(const ptr_t &ptr)
            : v_this(ptr)
        {
        }

        v_object(mptr ptr, FlagSet<AccessOptions> access_mode = {})
            : v_this(ptr, access_mode)
        {
        }
        
        // Construct a verified instance - i.e. backed by a valid db0 address with a known size
        v_object(db0::tag_verified, mptr ptr, std::size_t size_of = 0, FlagSet<AccessOptions> access_mode = {})
            : v_this(ptr, access_mode)
        {
            v_this.safeConstRef(size_of);
        }
        
        v_object(const v_object &other)
            : v_this(other.v_this)
        {
        }
        
        static constexpr unsigned char getRealmID() {
            return REALM_ID;
        }

    private:
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        v_object(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
            : v_this(ptr_t::makeNew(
                memspace,
                c_type::measure(std::get<I>(std::forward<Tuple>(t))...),
                std::get<N>(std::forward<Tuple>(t)) )
            )
        {
            c_type::__new(reinterpret_cast<std::byte*>(&v_this.modify()), std::get<I>(std::forward<Tuple>(t))...);
        }
        
        /// Pre-locked constructor
        struct tag_prelocked {};
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        v_object(Memspace &memspace, tag_prelocked, Tuple&& t, int_seq<std::size_t, I...>)
            : v_this(ptr_t::makeNew(memspace, std::move(std::get<N>(std::forward<Tuple>(t)))))
        {
            // placement new syntax
            c_type::__new(reinterpret_cast<std::byte*>(&v_this.modify()), std::get<I>(std::forward<Tuple>(t))...);
        }
        
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        void init(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
        {
            v_this = ptr_t::makeNew(
                memspace, 
                c_type::measure(std::get<I>(std::forward<Tuple>(t))...),
                std::get<N>(std::forward<Tuple>(t)) 
            );
            c_type::__new(reinterpret_cast<std::byte*>(&v_this.modify()), std::get<I>(std::forward<Tuple>(t))...);
        }

        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>        
        std::uint16_t initUnique(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
        {
            std::uint16_t instance_id;
            v_this = ptr_t::makeNewUnique(memspace, instance_id, c_type::measure(std::get<I>(std::forward<Tuple>(t))...), 
                std::get<N>(std::forward<Tuple>(t)) );
            c_type::__new(reinterpret_cast<std::byte*>(&v_this.modify()), std::forward<Args>(args)...);
            return instance_id;
        }

    public:
        /**
         * Allocating constructor with flags
         */
        template<typename... Args, last_type_is_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>())
        {
        }

        /**
         * Pre-locked allocating constructor
         * this constructor allows to pass the address and the mapped range for the instance being created
         */
        template<typename... Args, last_type_is_t<MappedAddress, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, tag_prelocked(), std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>())
        {
        }
        
        // Standard allocating constructor
        template<typename... Args, last_type_is_not_t<FlagSet<AccessOptions>, Args...>* = nullptr, last_type_is_not_t<MappedAddress, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {})
        {
        }
        
        // Create a new dbzero instance in the given memory space
        template<typename... Args, last_type_is_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        void init(Memspace &memspace, Args&&... args) {
            init(memspace, std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>())
        }

        template<typename... Args, last_type_is_not_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        void init(Memspace &memspace, Args&&... args)
            : v_object(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {})
        {
            init(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {});
        }

        // Create new instance assigned unique address
        // @return instance id
        template <typename... Args>
        std::uint16_t initUnique(Memspace &memspace, Args&&... args)
        {
            std::uint16_t instance_id;
            v_this = ptr_t::makeNewUnique(memspace, instance_id, c_type::measure(std::forward<Args>(args)...));
            c_type::__new(reinterpret_cast<std::byte*>(&v_this.modify()), std::forward<Args>(args)...);
            return instance_id;
        }

        // Construct from v-pointer
        v_object(ptr_t &&ptr)
            : v_this(std::move(ptr))
        {
        }
        
        v_object(v_object &&other)
            : v_this(std::move(other.v_this))
        {            
        }
        
        /**
         * static V-Space allocator
         */
        template<typename... Args>
        static std::uint64_t makeNew(Memspace &memspace, Args&&... args)
        {
            v_object new_object(memspace, std::forward<Args>(args)...);
            return new_object.getAddress();
        }
        
        void operator=(const v_object &other) {
            v_this = other.v_this;
        }        

        void operator=(v_object &&other)
        {
            v_this = std::move(other.v_this);
            other.v_this = {};
        }        

        /**
         * Readonly data access operator
         */
        inline const c_type *operator->() const {
            return v_this.get();
        }

        inline const c_type *getData() const {
            return v_this.get();
        }

        /**
         * Reference data container for read
         */
        inline const c_type &const_ref() const {
            return *(v_this.get());
        }

        const c_type& safeRef() const {
            return v_this.safeRef();
        }

        const c_type& safeRef(std::uint32_t access_mode) const {
            return v_this.safeRef(access_mode);
        }

        /**
         * Reference data container for update
         */
        inline c_type &modify() {
            return v_this.modify();
        }
        
        // Mark specific range as modified
        // NOTE: even if the range is not updated it will be forced-diff
        void modify(std::size_t offset, std::size_t size) {
            v_this.modify(offset, size);
        }
        
        inline Address getAddress() const {
            return v_this.getAddress();
        }

        inline const ptr_t &get_v_ptr() const {
            return this->v_this;
        }

        inline ptr_t &get_v_ptr() {
            return this->v_this;
        }
        
        void destroy() const
        {
            if (v_this) {
                v_this.destroy();
                v_this = {};
            }
        }
        
        inline Memspace &getMemspace() const {
            return v_this.getMemspace();
        }
        
        inline bool isNull() const {
            return v_this.isNull();
        }

        /**
         * instance compare
         */
        bool operator==(const v_object &other) const {
            return (v_this==other.v_this);
        }

        explicit operator bool() const {
            return !v_this.isNull();
        }

        bool operator!() const {
            return v_this.isNull();
        }
                
        mptr myPtr(Address address, FlagSet<AccessOptions> access_mode = {}) const {
            return v_this.getMemspace().myPtr(address, access_mode);
        }

        /**
         * Get use count of the underlying lock
        */
        unsigned int use_count() const {
            return v_this.use_count();
        }
        
        void detach() const {
            v_this.detach();            
        }
        
        void commit() const
        {
            // FIXME: optimization
            // potentially we could call v_this.commit() here BUT
            // if there exist 2 instances of v_object and one of them gets modified
            // then the "read-only" instance will not see the updates

            v_this.detach();            
        }
        
        // Calculate the number of DPs spanned by this object
        // NOTE: even small objects may span more than 1 DP if are positioned on a boundary
        // however allocators typically will avoid such situations
        unsigned int span() const
        {
            auto first_dp = v_this.getMemspace().getPageNum(v_this.getAddress());
            auto last_dp = v_this.getMemspace().getPageNum(v_this.getAddress() + v_this->sizeOf());
            return last_dp - first_dp + 1;
        }

        // Check if the underlying resource is available as mutable
        // i.e. was already access for read/write
        bool isModified() const {
            return v_this.isModified();
        }
        
    protected:
        // container reference
        mutable ptr_t v_this;
    };

    // Utility function to safely mutate a v_object's fixed-size member
    template <typename T, typename MemberT>
    MemberT &modifyMember(T &obj, const MemberT &member) 
    {
        assert((std::byte*)&member >= (std::byte*)obj.getData());
        assert((std::byte*)&member + sizeof(MemberT) <= (std::byte*)obj.getData() + obj->sizeOf());
        auto offset = (std::byte*)&member - (std::byte*)obj.getData();
        return *(MemberT*)((std::byte*)(&obj.modify()) + offset);
    }

}