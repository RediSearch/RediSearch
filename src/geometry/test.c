
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "rtree.h"
#include "wkt.h"

int main() {
    struct RTree *rt = RTree_New();
    assert(RTree_IsEmpty(rt));
    RTree_Free(rt);
    
    rt = Load_WKT_File("in.geometry");

    assert(!RTree_IsEmpty(rt));
    size_t presize = RTree_Size(rt);
    assert(presize == 10);

    printf("searching for polygons containing\n");
    struct Polygon *qpg = Polygon_NewByCoords(4, 1.25, 1.25, 1.5, 1.333, 1.333, 1.5, 1.25, 1.25);
    Polygon_Print(qpg);
    struct QueryIterator *iter = RTree_Query_Contains(rt, qpg);
    printf("num found results: %ld\n", QIter_Remaining(iter));
    for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
        RTDoc_Print(result);
    }
    puts("");
    QIter_Free(iter);
    Polygon_Free(qpg);

    printf("searching for polygons within\n");
    qpg = Polygon_NewByCoords(4, 7.0000004, 0., 7.0000004, 7.0000004, 0., 7.0000004, 7.0000004, 0.);
    Polygon_Print(qpg);
    iter = RTree_Query_Within(rt, qpg);
    printf("num found results: %ld\n", QIter_Remaining(iter));
    for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
        RTDoc_Print(result);
    }
    puts("");
    QIter_Free(iter);
    Polygon_Free(qpg);

    struct Polygon *pg = Polygon_NewByCoords(4, 0., 0., 2., 1., 2., 2., 0., 0.);
    struct RTDoc *r = RTDoc_New(pg);
    assert(RTree_Remove(rt, r));
    RTDoc_Free(r);
    Polygon_Free(pg);

    size_t postsize = RTree_Size(rt);
    assert(postsize == presize - 1);

    RTree_Clear(rt);
    assert(RTree_IsEmpty(rt));

    RTree_Free(rt);
    return 0;
}
