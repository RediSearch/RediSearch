#include "test_util.h"
#include "info/vector_index_stats.h"

int test_memory_and_marked_deleted_setter_getter() {
    VectorIndexStats stats = VectorIndexStats_Init();
    VectorIndexStats_SetMemory(&stats, 1024);
    VectorIndexStats_SetMarkedDeleted(&stats, 10);

    ASSERT(VectorIndexStats_GetMemory(&stats) == 1024);
    ASSERT(VectorIndexStats_GetMarkedDeleted(&stats) == 10);
    return 0;
}

int test_memory_and_marked_deleted_getter_setter() {
    VectorIndexStats stats = VectorIndexStats_Init();

    VectorIndexStats_SetMemory(&stats, 2048);
    VectorIndexStats_SetMarkedDeleted(&stats, 20);

    ASSERT(VectorIndexStats_GetMemory(&stats) == 2048);
    ASSERT(VectorIndexStats_GetMarkedDeleted(&stats) == 20);
    return 0;
}

int test_memory_and_marked_deleted_getter() {
    VectorIndexStats stats = VectorIndexStats_Init();
    VectorIndexStats_SetMemory(&stats, 1024);
    VectorIndexStats_SetMarkedDeleted(&stats, 10);

    VectorIndexStats_Getter getter = VectorIndexStats_GetGetter("memory");
    ASSERT(getter == VectorIndexStats_GetMemory);
    ASSERT(getter(&stats) == 1024);

    getter = VectorIndexStats_GetGetter("marked_deleted");
    ASSERT(getter == VectorIndexStats_GetMarkedDeleted);
    ASSERT(getter(&stats) == 10);
    return 0;
}

int test_memory_and_marked_deleted_setter() {
    VectorIndexStats stats = VectorIndexStats_Init();

    VectorIndexStats_Setter setter = VectorIndexStats_GetSetter("memory");
    ASSERT(setter == VectorIndexStats_SetMemory);
    setter(&stats, 2048);

    setter = VectorIndexStats_GetSetter("marked_deleted");
    ASSERT(setter == VectorIndexStats_SetMarkedDeleted);
    setter(&stats, 20);

    ASSERT(VectorIndexStats_GetMemory(&stats) == 2048);
    ASSERT(VectorIndexStats_GetMarkedDeleted(&stats) == 20);

    return 0;
}

TEST_MAIN({
    TESTFUNC(test_memory_and_marked_deleted_setter_getter);
    TESTFUNC(test_memory_and_marked_deleted_getter_setter);
    TESTFUNC(test_memory_and_marked_deleted_getter);
    TESTFUNC(test_memory_and_marked_deleted_setter);
});
