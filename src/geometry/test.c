
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "rtree.h"
#include "wkt.h"

int main() {
    struct RTree *rt = RTree_New();
    assert(RTree_IsEmpty(rt));
    
    for (int i = 0; i < 10; ++i) {
        char wkt[64] = "POLYGON((";
        char ppp[10] = {0};
        snprintf(ppp, 10, "%d", i);
        strcat(wkt, ppp);
        strcat(wkt, " ");
        snprintf(ppp, 10, "%d", i);
        strcat(wkt, ppp);
        strcat(wkt, ", ");
        snprintf(ppp, 10, "%d", i+2);
        strcat(wkt, ppp);
        strcat(wkt, " ");
        snprintf(ppp, 10, "%d", i+1);
        strcat(wkt, ppp);
        strcat(wkt, ", ");
        snprintf(ppp, 10, "%d", i+1);
        strcat(wkt, ppp);
        strcat(wkt, " ");
        snprintf(ppp, 10, "%d", i+2);
        strcat(wkt, ppp);
        strcat(wkt, ", ");
        snprintf(ppp, 10, "%d", i);
        strcat(wkt, ppp);
        strcat(wkt, " ");
        snprintf(ppp, 10, "%d", i);
        strcat(wkt, ppp);
        strcat(wkt, "))");

        struct Polygon *pg = From_WKT(wkt);
        struct RTDoc *r = RTDoc_New(pg);
        RTree_Insert(rt, r);
        RTDoc_Free(r);
        Polygon_Free(pg);
    }

    assert(!RTree_IsEmpty(rt));
    size_t presize = RTree_Size(rt);
    assert(presize == 10);

    struct Polygon *qpg = Polygon_NewByCoords(4, 1.001, 1.001, 1.665, 1.333, 1.333, 1.665, 1.001, 1.001);
    struct QueryIterator *iter = RTree_Query_Contains(rt, qpg);
    printf("num found results: %ld\n", QIter_Remaining(iter));
    for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
        RTDoc_Print(result);
    }
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
