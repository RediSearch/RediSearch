#include "varint.h"

int main(int argc, char **argv) {
    
    VarintVectorWriter *vw = NewVarintVectorWriter(8);
    VVW_Write(vw, 1);
    VVW_Write(vw, 2);
    VVW_Write(vw, 3);
    VVW_Write(vw, 100);
    printf("%d %d\n", vw->len, vw->cap);
    
    VVW_Free(vw);
    
    
}