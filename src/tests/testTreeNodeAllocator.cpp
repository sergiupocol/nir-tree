#include <catch2/catch.hpp>
#include <rstartree/rstartree.h>
#include <rstartree/node.h>
#include <storage/tree_node_allocator.h>
#include <storage/page.h>
#include <unistd.h>
#include <util/geometry.h>
#include <nirtreedisk/nirtreedisk.h>

TEST_CASE( "Tree Node Allocator: Single RStarTree Node" ) {

    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
        allocator.create_new_tree_node<rstartree::Node>();

    REQUIRE( alloc_data.first != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 0 );
    REQUIRE( alloc_data.second.get_offset() == 0 );
}

TEST_CASE( "Tree Node Allocator : Free Consecutive RStar TreeNodes" ) {

    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
        allocator.create_new_tree_node<rstartree::Node>(); 

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data_two =
        allocator.create_new_tree_node<rstartree::Node>();

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data_three =
        allocator.create_new_tree_node<rstartree::Node>();

    REQUIRE( alloc_data.first != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 0 );
    REQUIRE( alloc_data.second.get_offset() == 0 );

    REQUIRE( alloc_data_two.first != nullptr );
    REQUIRE( alloc_data_two.second.get_page_id() == 0 );
    REQUIRE( alloc_data_two.second.get_offset() == 48 );

    REQUIRE( alloc_data_three.first != nullptr );
    REQUIRE( alloc_data_three.second.get_page_id() == 0 );
    REQUIRE( alloc_data_three.second.get_offset() == 96 );

    allocator.free(alloc_data.second, sizeof(rstartree::Node));
    REQUIRE(allocator.get_free_list_length() == 1);

    allocator.free(alloc_data_two.second, sizeof(rstartree::Node));
    REQUIRE(allocator.get_free_list_length() == 1);

    allocator.free(alloc_data_three.second, sizeof(rstartree::Node));
    REQUIRE(allocator.get_free_list_length() == 1);

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data_four =
        allocator.create_new_tree_node<rstartree::Node>();

    // This should make use of free'd memory
    REQUIRE( alloc_data_four.first != nullptr );
    REQUIRE( alloc_data_four.second.get_page_id() == 0 );
    REQUIRE( alloc_data_four.second.get_offset() == 0 );

    // Memory lost because remainder is less than 272
    REQUIRE(allocator.get_free_list_length() == 0);
}

TEST_CASE( "Tree Node Allocator : Free Consecutive RStar TreeNodes with Large Remainder" ) {

    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    int num_nodes = ceil(272 / sizeof(rstartree::Node)) + 2;
    std::vector<std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle>> allocs;
    for (int i = 0; i < num_nodes; i++) {
        std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<rstartree::Node>();

        allocs.emplace_back(alloc_data);
    }

    for (int i = 0; i < num_nodes; i++) {
        auto &alloc_data = allocs.at(i);
        allocator.free(alloc_data.second, sizeof(rstartree::Node));
        REQUIRE(allocator.get_free_list_length() == 1);
    }

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
        allocator.create_new_tree_node<rstartree::Node>();

    // This should make use of freed memory
    REQUIRE( alloc_data.first != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 0 );
    REQUIRE( alloc_data.second.get_offset() == 0 );
    REQUIRE(allocator.get_free_list_length() == 1);
}

TEST_CASE( "Tree Node Allocator : Free Non-Consecutive RStar TreeNodes" ) {

    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    int num_nodes = 11;
    // 3 nodes + 1 partition alloc + 3 nodes + 1 partition alloc + 3 nodes
    std::vector<std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle>> allocs;
    for (int i = 0; i < num_nodes; i++) {
        std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<rstartree::Node>();

        allocs.emplace_back(alloc_data);
    }

    for (int i = 0; i < 3; i++) {
        auto &alloc_data = allocs.at(i);
        allocator.free(alloc_data.second, sizeof(rstartree::Node));
        REQUIRE(allocator.get_free_list_length() == 1);
    }

    for (int i = 4; i < 7; i++) {
        auto &alloc_data = allocs.at(i);
        allocator.free(alloc_data.second, sizeof(rstartree::Node));
        REQUIRE(allocator.get_free_list_length() == 2);
    }

    for (int i = 8; i < 11; i++) {
        auto &alloc_data = allocs.at(i);
        allocator.free(alloc_data.second, sizeof(rstartree::Node));
        REQUIRE(allocator.get_free_list_length() == 3);
    }

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
        allocator.create_new_tree_node<rstartree::Node>();

    // This should make use of freed memory
    REQUIRE( alloc_data.first != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 0 );
    REQUIRE( alloc_data.second.get_offset() == 0 );

    REQUIRE(allocator.get_free_list_length() == 2);
}

TEST_CASE( "Tree Node Allocator : Free Remainder of Page During Allocation" ) {

    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    int num_nodes = floor((PAGE_DATA_SIZE - 272) / sizeof(rstartree::Node));
    std::vector<std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle>> allocs;
    for (int i = 0; i < num_nodes; i++) {
        std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<rstartree::Node>();

        allocs.emplace_back(alloc_data);
    }

    REQUIRE(allocator.get_free_list_length() == 0);
    // Allocate something bigger than 272
    auto alloc_data_huge = allocator.create_new_tree_node<InlineUnboundedIsotheticPolygon>(
            PAGE_DATA_SIZE, NodeHandleType( 0 ) );
    // It should be on a new page
    REQUIRE(allocator.get_free_list_length() == 1);

    for (int i = 0; i < num_nodes; i++) {
        auto &alloc_data = allocs.at(i);
        allocator.free(alloc_data.second, sizeof(rstartree::Node));
        REQUIRE(allocator.get_free_list_length() == 2);
    }

    allocator.free(alloc_data_huge.second, PAGE_DATA_SIZE);
}

TEST_CASE( "Tree Node Allocator: Overflow one Page" ) {
    size_t node_size = sizeof( rstartree::Node );
    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    for( size_t i = 0; i < PAGE_DATA_SIZE / node_size; i++ ) {
        std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<rstartree::Node>();
        REQUIRE( alloc_data.first != nullptr );
        REQUIRE( alloc_data.second.get_page_id() == 0 );
        REQUIRE( alloc_data.second.get_offset() ==  i * node_size );
    }
    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<rstartree::Node>();
    REQUIRE( alloc_data.first != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 1 );
    REQUIRE( alloc_data.second.get_offset() == 0 );
}

TEST_CASE( "Tree Node Allocator: Convert TreeNodePtr to Raw Ptr" ) {
    tree_node_allocator allocator( 10 * PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    std::pair<pinned_node_ptr<rstartree::Node>, tree_node_handle> alloc_data =
        allocator.create_new_tree_node<rstartree::Node>();
    pinned_node_ptr<rstartree::Node> output_ptr = alloc_data.first;
    REQUIRE( output_ptr != nullptr );
    REQUIRE( alloc_data.second.get_page_id() == 0 );
    REQUIRE( alloc_data.second.get_offset() == 0 );
    pinned_node_ptr<rstartree::Node> converted_ptr =
        allocator.get_tree_node<rstartree::Node>(
            alloc_data.second );
    REQUIRE( output_ptr == converted_ptr );

}

TEST_CASE( "Tree Node Allocator: Can Handle Paged Out data" ) {

    // Create a single page allocator
    tree_node_allocator allocator( PAGE_SIZE, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    // Size_t's to make it easier to understand, but it doesn't matter
    size_t node_size = sizeof( size_t );
    std::vector<tree_node_handle> allocated_ptrs;
    for( size_t i = 0; i < PAGE_DATA_SIZE / node_size; i++ ) {
        std::pair<pinned_node_ptr<size_t>, tree_node_handle> alloc_data =
            allocator.create_new_tree_node<size_t>();
        pinned_node_ptr<size_t> sz_ptr = alloc_data.first;
        REQUIRE( sz_ptr != nullptr );
        REQUIRE( alloc_data.second.get_page_id() == 0 );
        REQUIRE( alloc_data.second.get_offset() == i *
                sizeof(size_t));
        *sz_ptr = i;
        allocated_ptrs.push_back( alloc_data.second );
    }

    // This will go on the next page, forcing a page out of the first
    allocator.create_new_tree_node<size_t>();

    // Walk over all the pointers in the first page, make sure the data
    // is preserved
    for( size_t i = 0; i < allocated_ptrs.size(); i++ ) {
        pinned_node_ptr<size_t> sz_ptr = allocator.get_tree_node<size_t>(
                allocated_ptrs[i] );
        REQUIRE( *sz_ptr == i );
    }
}

class allocator_tester : public tree_node_allocator {
public:

    allocator_tester( size_t memory_budget, std::string
            backing_file_name ) :
        tree_node_allocator( memory_budget, backing_file_name ) {}

    buffer_pool &get_buffer_pool() {
        return buffer_pool_;
    }

    size_t get_cur_page() {
        return cur_page_;
    }
    size_t get_space_left_in_cur_page() {
        return space_left_in_cur_page_;
    }
};

TEST_CASE( "Tree Node Allocator: Test pinned_node_ptr scope" ) {
    // 2 page allocator
    allocator_tester allocator( PAGE_SIZE *2, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    buffer_pool &bp = allocator.get_buffer_pool();
    tree_node_handle first_obj_handle;

    {
        std::pair<pinned_node_ptr<size_t>, tree_node_handle> alloc_data
            = allocator.create_new_tree_node<size_t>();
        first_obj_handle = alloc_data.second;

        
        // There's only one page, so we know the ptr is on it.
        page *p = bp.get_page( 0 );
        REQUIRE( p->header_.pin_count_ == 1 );

        std::pair<pinned_node_ptr<size_t>, tree_node_handle> alloc_data2
            = allocator.create_new_tree_node<size_t>();

        REQUIRE( p->header_.pin_count_ == 2 );
    }

    page *p = bp.get_page( 0 );
    REQUIRE( p->header_.pin_count_ == 0 );

    // Fill up one whole page
    for( size_t i = 0; i < ( PAGE_DATA_SIZE / sizeof( size_t ) ) - 2;
            i++ ) {
        std::pair<pinned_node_ptr<size_t>, tree_node_handle> alloc_data
            = allocator.create_new_tree_node<size_t>();
    }

    // Should be nothing pinned!
    page *p1 = bp.get_page( 0 );
    REQUIRE( p1->header_.pin_count_ == 0 );

    // NOthing will be allocated on this yet, but because it is in the
    // freelist we can get it (since we prealloc'd 2 pages)
    page *p2 = bp.get_page( 1 );

    // Get first object
    pinned_node_ptr<size_t> first_obj_ptr = allocator.get_tree_node<size_t>(
            first_obj_handle );
    REQUIRE( p1->header_.pin_count_ == 1 );

    {
        std::pair<pinned_node_ptr<size_t>, tree_node_handle> alloc_data
            = allocator.create_new_tree_node<size_t>();
        p2 = bp.get_page( 1 );
        REQUIRE( p2 != nullptr );

        // Both pinned
        REQUIRE( p1->header_.pin_count_ == 1 );
        REQUIRE( p2->header_.pin_count_ == 1 );

        // Overwrite ptr
        first_obj_ptr = alloc_data.first;

        // P1 unpinned
        REQUIRE( p1->header_.pin_count_ == 0 );
        REQUIRE( p2->header_.pin_count_ == 2 );
        
        // Second ref falls out of scope
    }

    REQUIRE( p1->header_.pin_count_ == 0 );
    REQUIRE( p2->header_.pin_count_ == 1 );
}

TEST_CASE( "Tree Node Allocator: Test freelist perfect allocs" ) {

    allocator_tester allocator( PAGE_SIZE*2, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    for( unsigned i = 0; i < PAGE_DATA_SIZE/sizeof(size_t)+1; i++ ) {
        auto alloc_data = allocator.create_new_tree_node<size_t>();
        REQUIRE( allocator.get_space_left_in_cur_page() == PAGE_DATA_SIZE -
                sizeof(size_t) );
        allocator.free( alloc_data.second, sizeof(size_t) ); // perfect fit, reuse.
    }
    REQUIRE( allocator.get_cur_page() == 0 );
}

TEST_CASE( "Tree Node Allocator: Test freelist split allocs" ) {
    using NodeType =
        nirtreedisk::BranchNode<3,7,nirtreedisk::LineMinimizeDownsplits>;
    allocator_tester allocator( PAGE_SIZE*2, "file_backing.db" );
    unlink( allocator.get_backing_file_name().c_str() );
    allocator.initialize();

    size_t poly_size = compute_sizeof_inline_unbounded_polygon(
        MAX_RECTANGLE_COUNT+1);
    for( unsigned i = 0; i < (PAGE_DATA_SIZE/sizeof(NodeType))-1; i++ ) {
        auto alloc_data = allocator.create_new_tree_node<NodeType>();
    }
    auto alloc_data = allocator.create_new_tree_node<NodeType>();
    REQUIRE( allocator.get_cur_page() == 0 );
    allocator.free( alloc_data.second, sizeof(NodeType) );
    unsigned remaining_slots = (PAGE_DATA_SIZE % sizeof(NodeType))/poly_size + 
        sizeof(NodeType)/poly_size;
    REQUIRE( remaining_slots == 7 );
    for( unsigned i = 0; i < remaining_slots; i++ ) {
        auto alloc_data2 =
            allocator.create_new_tree_node<InlineUnboundedIsotheticPolygon>(
                    compute_sizeof_inline_unbounded_polygon(
                        MAX_RECTANGLE_COUNT+1), NodeHandleType(0) );
        REQUIRE( allocator.get_cur_page() == 0 );
    }
    auto alloc_data2 =
            allocator.create_new_tree_node<InlineUnboundedIsotheticPolygon>(
                    compute_sizeof_inline_unbounded_polygon(
                        MAX_RECTANGLE_COUNT+1), NodeHandleType(0) );
    REQUIRE( allocator.get_cur_page() == 1 );

}
