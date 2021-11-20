#pragma once 

#include <storage/buffer_pool.h>
#include <storage/page.h>
#include <cassert>
#include <optional>
#include <iostream>
#include <string>
#include <list>
#include <cstdint>
#include <limits>
#include <chrono>

template <typename T>
class pinned_node_ptr {
public:

    pinned_node_ptr( buffer_pool &pool, T *obj_ptr, page *page_ptr ) :
        pool_( pool ), obj_ptr_( obj_ptr ), page_ptr_( page_ptr ) {
            if( page_ptr != nullptr ) {
                pool_.pin_page( page_ptr_ );
            } }

    pinned_node_ptr( const pinned_node_ptr &other ) :
        pool_( other.pool_ ), obj_ptr_( other.obj_ptr_ ),
        page_ptr_( other.page_ptr_ ){
            if( page_ptr_ != nullptr ) {
                pool_.pin_page( page_ptr_ );
            }
        
    }

    ~pinned_node_ptr() {
        if( page_ptr_ != nullptr ) {
            pool_.unpin_page( page_ptr_ );
        }
    }

    pinned_node_ptr &operator=( pinned_node_ptr other ) {
        // Unpin old
        if( page_ptr_ != nullptr ) {
            pool_.unpin_page( page_ptr_ );
        }
        // Set new
        obj_ptr_ = other.obj_ptr_;
        page_ptr_ = other.page_ptr_;

        // Pin new
        if( page_ptr_ != nullptr ) {
            pool_.pin_page( page_ptr_ );
        }
        return *this;
    }

    bool operator==( std::nullptr_t ptr ) const {
        return obj_ptr_ == ptr; 
    }

    bool operator!=( std::nullptr_t ptr ) const {
        return !(obj_ptr_ == ptr); 
    }

    bool operator==( const pinned_node_ptr &other ) const {
        return obj_ptr_ == other.obj_ptr_;
    }

    bool operator!=( const pinned_node_ptr &other ) const {
        return !(obj_ptr_ == other.obj_ptr_);
    }

    constexpr T& operator*() const {
        return *obj_ptr_;
    }

    constexpr T *operator->() const {
        return obj_ptr_;
    }

    buffer_pool &pool_;
    T *obj_ptr_;
    page *page_ptr_;

};

// A wrapper around an int
// The sole purpose of this struct is avoid footguns where you
// accidentally specify a type as the size during allocations and cause
// amazing memory problems
struct NodeHandleType {
    explicit NodeHandleType( uint16_t type ) :
       type_( type ) { } 

    uint16_t type_;
};

class tree_node_handle {
public:
    struct page_location {
        page_location( unsigned page_id, unsigned offset ) :
            page_id_( page_id ),
            offset_( offset ) {}

        // Total size = 64 bytes. I could make this smaller, but
        // I think x86 wants to access things in units of 64 bytes
        // anyways
        uint32_t page_id_;
        uint16_t offset_; // Only need 12 bits to index 4k pages
        static_assert( PAGE_SIZE <= std::numeric_limits<uint16_t>::max() );

        bool operator==( const page_location &other ) const {
            return page_id_ == other.page_id_ and offset_ ==
                other.offset_;
        }
    };

    tree_node_handle( uint32_t page_id, uint16_t offset, NodeHandleType
            type ) :
        page_location_( std::in_place, page_id, offset ),
        type_( type.type_ ) {
    }

    tree_node_handle() :
        page_location_( std::nullopt ),
        type_( 0 ) {}

    tree_node_handle( std::nullptr_t ) :
        page_location_( std::nullopt ),
        type_( 0 ) {}

    operator bool() const {
        return page_location_.has_value();
    }

    bool operator==( const tree_node_handle &other ) const {
        return page_location_ == other.page_location_;
    }

    bool operator!=( const tree_node_handle &other ) const {
        return !(*this == other);
    }

    bool operator==( const std::nullptr_t ) const {
        return not page_location_.has_value();
    }

    tree_node_handle &operator=( const tree_node_handle &other ) =
        default;

    tree_node_handle &operator=( const std::nullptr_t ) { 
        page_location_.reset();
        return *this;
    }

    inline uint32_t get_page_id() {
        return page_location_.value().page_id_;
    }

    inline uint16_t get_offset() {
        return page_location_.value().offset_;
    }

    inline uint16_t get_type() {
        return type_;
    }

    inline void set_type( NodeHandleType type ) {
        type_ = type.type_;
    }

    friend std::ostream& operator<<(std::ostream &os, const
            tree_node_handle &handle ) {
        if( handle.page_location_.has_value() ) {
            os << "{ PageID: " << handle.page_location_.value().page_id_
                << ", Offset: " << handle.page_location_.value().offset_ << "}";
        } else {
            os << "{ nullptr }";
        }
        return os;
    }

private:
    std::optional<page_location> page_location_;
    // Special bits to indicate what type of node is on the other
    // end of this handle.
    uint16_t type_;

};

template <typename T, typename U>
pinned_node_ptr<U> reinterpret_handle_ptr( const pinned_node_ptr<T> &ptr )
{
    return pinned_node_ptr( ptr.pool_, (U *) ptr.obj_ptr_, ptr.page_ptr_ );
}

static_assert( std::is_trivially_copyable<tree_node_handle>::value );

class tree_node_allocator {
public:
    tree_node_allocator( size_t memory_budget,
            std::string backing_file );

    inline void initialize() {
        buffer_pool_.initialize();
    }

    inline std::string get_backing_file_name() {
        return buffer_pool_.get_backing_file_name();
    }

    void dump_free_list() const {
        for (auto it : free_list_) {
            std::cerr << "(" << it.first << ", " << it.second << ")" << " -> ";
        }
        std::cerr << "NULL" << std::endl;
    }

    // Primarily used by unit tests
    size_t get_free_list_length() const {
        return free_list_.size();
    }

    void insert_to_free_list(std::pair<tree_node_handle,uint16_t> free_block) {
        // First, check if we can merge this block with an already freed one
        // If we can, modify an entry in the list
        // Otherwise iter will point to the node after which we must insert
        uint32_t initial_size = validate_free_list();
        if (!free_block.first) return;
        if (get_free_list_length() == 0) {
            free_list_.push_back(free_block);
            return;
        }
        auto iter = std::upper_bound(free_list_.begin(), free_list_.end(), free_block,
        [](std::pair<tree_node_handle,uint16_t> lhs, std::pair<tree_node_handle,uint16_t> rhs) -> bool {
            if (lhs.first.get_page_id() == rhs.first.get_page_id()) {
                uint16_t lhs_start = lhs.first.get_offset();
                uint16_t lhs_end = lhs_start + lhs.second;
                uint16_t rhs_start = rhs.first.get_offset();
                uint16_t rhs_end = rhs_start + rhs.second;
                bool match = (rhs_end == lhs_start || lhs_end == rhs_start);
                return match || lhs.first.get_offset() < rhs.first.get_offset();
            }
            return lhs.first.get_page_id() < rhs.first.get_page_id();
        });
        if (iter == free_list_.end()) {
            free_list_.push_back(free_block);
            return;
        }

        auto &free_location = *iter;
        assert(free_location.first);

        if (free_location.first.get_page_id() == free_block.first.get_page_id() && iter->first) {
            // On the same page!
            // Now check if the offsets are aligned such that
            // fre_block lies adjacent to free_location
            uint16_t free_block_start = free_block.first.get_offset();
            uint16_t free_block_end = free_block_start + free_block.second;
            uint16_t free_location_start = free_location.first.get_offset();
            uint16_t free_location_end = free_location_start + free_location.second;
            assert(free_block_end <= free_location_start || free_location_end <= free_block_start);

            if (free_block_end == free_location_start) {
                free_location.first = free_block.first;
                free_location.second += free_block.second;
                // Since we just modified this node by increasing it towards iter--, let's check if a merge is possible
                auto predecessor_iter = iter;
                predecessor_iter--;
                if ( iter != free_list_.begin() ) {
                    auto &predecessor = *predecessor_iter;
                    if (predecessor.first.get_page_id() != free_location.first.get_page_id()) return;
                    uint16_t predecessor_end = predecessor.first.get_offset() + predecessor.second;
                    if (predecessor_end != free_location.first) return;
                    predecessor.second += free_location.second;
                    free_list_.erase(iter);
                    return;
                }
                return;
            } else if (free_location_end == free_block_start) {
                free_location.second += free_block.second;

                auto descendant_iter = iter;
                descendant_iter++;
                if ( descendant_iter != free_list_.end() ) {
                    auto &descendant = *descendant_iter;
                    if (descendant.first.get_page_id() != free_location.first.get_page_id()) return;
                    uint16_t descendant_start = descendant.first.get_offset();
                    if (descendant_start != free_location.first.get_offset() + free_location.second) return;
                    free_location.second += descendant.second;
                    free_list_.erase(descendant_iter);
                    return;
                }
                return;
            }
        }

        // Since we reached this point, free_block will be a standalone entry in the free_list
        free_list_.insert(iter, free_block);
        uint32_t final_size = validate_free_list();
        assert(initial_size + free_block.second == final_size);
    }

    template <typename T>
    std::pair<pinned_node_ptr<T>, tree_node_handle>
    create_new_tree_node( NodeHandleType type_code = NodeHandleType(0) ) {
        return create_new_tree_node<T>( sizeof( T ), type_code );
    }

    void print_metrics() const {
        std::cout << "Alloc count: " << alloc_count << ", total alloc time: " << total_alloc_time << std::endl;
        std::cout << "\t\tAVG: " << (total_alloc_time / double(alloc_count)) << std::endl;

        std::cout << "Free count: " << free_count << ", total free time: " << total_free_time << std::endl;
        std::cout << "\t\tAVG: " << (total_free_time / double(free_count)) << std::endl;
    }

    uint16_t get_free_list_size() const {
        uint16_t total_size = 0;
        for (auto entry : free_list_) total_size += entry.second;
        return total_size;
    }

    uint32_t validate_free_list() const {
        uint32_t total_size = 0;
#ifndef NDEBUG
        if (get_free_list_length() == 0) return 0;

        auto it = free_list_.begin();
        auto lhs = *it;
        total_size += lhs.second;
        auto rhs = *(++it);
        bool fail = false;
        try
        {
            while (it != free_list_.end()) {
                total_size += it->second;
                if (!(lhs.first.get_offset() <= PAGE_DATA_SIZE)) { fail = true; break; }
                if (!(rhs.first.get_offset() <= PAGE_DATA_SIZE)) { fail = true; break; }
                assert(lhs.first.get_offset() <= PAGE_DATA_SIZE);
                assert(rhs.first.get_offset() <= PAGE_DATA_SIZE);
                uint16_t lhs_start = lhs.first.get_offset();
                uint16_t lhs_end = lhs_start + lhs.second;
                uint16_t rhs_start = rhs.first.get_offset();

                if (lhs.first.get_page_id() == rhs.first.get_page_id()) {

                    if (!(lhs_end < rhs_start)) { fail = true; break; }
                    assert(lhs_end < rhs_start);
                } else {

                    if (!(lhs.first.get_page_id() < rhs.first.get_page_id())) { fail = true; break; }
                    assert(lhs.first.get_page_id() < rhs.first.get_page_id());
                }

                lhs = *it;
                rhs = *(++it);
            }
            if (fail) throw std::exception();
        }
        catch (const std::exception &e)
        {
            dump_free_list();
            assert(false);
        }
#endif
        return total_size;
    }

    template <typename T>
    std::pair<pinned_node_ptr<T>, tree_node_handle>
    create_new_tree_node( uint16_t node_size, NodeHandleType type_code ) {
        assert( node_size <= PAGE_DATA_SIZE );
        alloc_count += 1;
        std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now();


        validate_free_list();
        for( auto iter = free_list_.begin(); iter != free_list_.end();
                iter++ ) {
            auto alloc_location = *iter;
            if( alloc_location.second < node_size ) {
                continue;
            }

            alloc_location.first.set_type( type_code );

            size_t remainder = alloc_location.second - node_size;
            free_list_.erase( iter );
            page *page_ptr = buffer_pool_.get_page(
                alloc_location.first.get_page_id() );
            T *obj_ptr = (T *) (page_ptr->data_ +
                    alloc_location.first.get_offset() );


            // sizeof inline unbounded polygon with
            // MAX_RECTANGLE_COUNT+1
            // Can't use that symbol here because it would be recursive
            // includes. So instead I static assert it in that file and
            // use the constant here.
            if( remainder > 273 ) {
                uint16_t new_offset = alloc_location.first.get_offset() +
                    node_size;
                tree_node_handle split_handle(
                        alloc_location.first.get_page_id(), new_offset,
                        type_code );
                insert_to_free_list( std::make_pair( split_handle,
                           remainder ) );
            }

            std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> delta = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin);
            total_alloc_time += delta.count();

            return std::make_pair( pinned_node_ptr( buffer_pool_,
                        obj_ptr, page_ptr ), alloc_location.first );


        }

        // Fall through, need new location
        page *page_ptr = get_page_to_alloc_on( node_size );
        if( page_ptr == nullptr ) {
            std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> delta = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin);
            total_alloc_time += delta.count();

            return std::make_pair( pinned_node_ptr( buffer_pool_,
                        static_cast<T *>( nullptr ), static_cast<page *>(
                            nullptr ) ), tree_node_handle() );
        }

        uint16_t offset_into_page = (PAGE_DATA_SIZE - space_left_in_cur_page_);
        T *obj_ptr = (T *) (page_ptr->data_ + offset_into_page);
        space_left_in_cur_page_ -= node_size;
        tree_node_handle meta_ptr( page_ptr->header_.page_id_,
                offset_into_page, type_code );
        

        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> delta = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin);
        total_alloc_time += delta.count();

        return std::make_pair( pinned_node_ptr( buffer_pool_, obj_ptr,
                    page_ptr ), std::move(meta_ptr) );
    }

    void free( tree_node_handle handle, uint16_t alloc_size ) {
        free_count += 1;
        std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now();

        validate_free_list();
#ifndef NDEBUG
        if( handle.get_type() == 1 ) {
            assert( alloc_size == 176 );
        } else if( handle.get_type() == 2 ) {
            assert( alloc_size == 1840 );
        }
#endif
        if (!handle) return;
        insert_to_free_list( std::make_pair( handle, alloc_size ) );
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> delta = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin);
        total_free_time += delta.count();

    }

    template <typename T>
    pinned_node_ptr<T> get_tree_node( tree_node_handle node_ptr ) {
#ifndef NDEBUG
        for( const auto &entry : free_list_ ) {
            if( entry.first == node_ptr ) {
                std::cout << "Using freed pointer: " << node_ptr <<
                    std::endl;
            }
            assert( entry.first != node_ptr );
        }
#endif
        validate_free_list();
        page *page_ptr = buffer_pool_.get_page( node_ptr.get_page_id() );
        assert( page_ptr != nullptr );
        T *obj_ptr = (T *) (page_ptr->data_ + node_ptr.get_offset() );
        return pinned_node_ptr( buffer_pool_, obj_ptr, page_ptr );
    }

    buffer_pool buffer_pool_;

protected:
    int alloc_count = 0;
    int free_count = 0;
    double total_alloc_time = 0;
    double total_free_time = 0;

    page *get_page_to_alloc_on( uint16_t object_size );

    uint16_t space_left_in_cur_page_;
    uint32_t cur_page_;
    std::vector<std::pair<tree_node_handle,uint16_t>> free_list_;
};
