/*

The MIT License (MIT)

Copyright (c) 2012-2014 Erik Soma

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

#pragma once

#ifndef IO_QUEUE_H
#define IO_QUEUE_H

// standard library
#include <assert.h>
#include <malloc.h>
#include <stdatomic.h>
#include <string.h>

#ifndef NDEBUG
#define IO_QUEUE_ASSERT(x) assert(x)
#else
#define IO_QUEUE_ASSERT(x) ((void)(x))
#endif

// all io-queue operations return an IoQueueResult
typedef int IoQueueResult;
#define IO_QUEUE_RESULT_TRUE (2)
#define IO_QUEUE_RESULT_SUCCESS (1)
#define IO_QUEUE_RESULT_FALSE (0)
#define IO_QUEUE_RESULT_OUT_OF_MEMORY (-1)

// internal structure used for implementing the queue
typedef struct IoQueueNode IoQueueNode;
typedef struct IoQueueNode
{
    atomic_uintptr_t next;
    // data for the node is stored "after" it
} IoQueueNode;

// queue object
typedef struct IoQueue
{
    atomic_uintptr_t head;
    atomic_uintptr_t tail;
    size_t item_size;
} IoQueue;

// initialize an io queue structure
// should be called before anything else
//
// item_size indicates the size (in bytes, sizeof()) of the items you are
// storing in the queue
IoQueueResult io_queue_init(IoQueue* io_queue, size_t item_size)
{
    IO_QUEUE_ASSERT(io_queue);
    atomic_init(&io_queue->head, (atomic_uintptr_t)NULL);
    atomic_init(&io_queue->tail, (atomic_uintptr_t)NULL);
    io_queue->item_size = item_size;
    return IO_QUEUE_RESULT_SUCCESS;
}

// checks if the queue has any data
// this is a consumer operation
//
// returns IO_QUEUE_RESULT_TRUE if there is a front and IO_QUEUE_RESULT_FALSE
// if there is not
IoQueueResult io_queue_has_front(IoQueue* io_queue)
{
    IO_QUEUE_ASSERT(io_queue);
    if (atomic_load(&io_queue->head) == 0)
    {
        return IO_QUEUE_RESULT_FALSE;
    }
    return IO_QUEUE_RESULT_TRUE;
}

// gets the value at the front of the queue
// this is a consumer operation
//
// the value stored will be copied to value pointed to by the data argument
IoQueueResult io_queue_front(IoQueue* io_queue, void* data)
{
    IO_QUEUE_ASSERT(io_queue);
    IO_QUEUE_ASSERT(data);
    IoQueueNode* head = (IoQueueNode*)atomic_load(&io_queue->head);
    IO_QUEUE_ASSERT(head);
    memcpy(data, (void*)(head + 1), io_queue->item_size);
    return IO_QUEUE_RESULT_SUCCESS;
}

// removes the item at the front of the queue
// this is a consumer operation
IoQueueResult io_queue_pop(IoQueue* io_queue)
{
    assert(io_queue);
    assert(io_queue_has_front(io_queue) == IO_QUEUE_RESULT_TRUE);
    // get the head
    IoQueueNode* popped = (IoQueueNode*)atomic_load(&io_queue->head);
    IoQueueNode* compare = popped;
    // set the tail and head to nothing if they are the same
    if (atomic_compare_exchange_strong(&io_queue->tail, &compare, 0))
    {
        compare = popped;
        // its possible for another thread to have pushed after we swap out the
        // tail, in this case the head will be different then what was popped,
        // so we just do a blind exchange, not caring about the result
        atomic_compare_exchange_strong(&io_queue->head, &compare, 0);
    }
    else
    // tail is different from head, set the head to the next value
    {
        IoQueueNode* new_head = 0;
        while(!new_head)
        {
            // its possible that the next node hasn't been assigned yet, so just
            // spin until the pushing thread stores the value
            new_head = (IoQueueNode*)atomic_load(&popped->next);
        }
        atomic_store(&io_queue->head, (atomic_uintptr_t)new_head);
    }
    // delete the popped node
    free(popped);
    return IO_QUEUE_RESULT_SUCCESS;
}

// adds an item to the back of the queue
// this is a producer operation
//
// the value stored will be copied from the value pointed to by the data
// argument
//
// may fail with IO_QUEUE_RESULT_OUT_OF_MEMORY if the heap is exhausted
IoQueueResult io_queue_push(IoQueue* io_queue, void* data)
{
    IO_QUEUE_ASSERT(io_queue);
    // create the new tail
    IoQueueNode* new_tail = malloc(
        sizeof(IoQueueNode) + io_queue->item_size
    );
    if (!new_tail){ return IO_QUEUE_RESULT_OUT_OF_MEMORY; }
    atomic_init(&new_tail->next, 0);
    memcpy(new_tail + 1, data, io_queue->item_size);
    // swap the new tail with the old
    IoQueueNode* old_tail = (IoQueueNode*)atomic_exchange(
        &io_queue->tail,
        (atomic_uintptr_t)new_tail
    );
    // link the old tail to the new
    if (old_tail)
    {
        atomic_store(&old_tail->next, (atomic_uintptr_t)new_tail);
    }
    else
    {
        atomic_store(&io_queue->head, (atomic_uintptr_t)new_tail);
    }
    return IO_QUEUE_RESULT_SUCCESS;
}

// clears the entire queue
// this is a consumer operation
//
// consider this to be similar to a destructor for the queue, although the queue
// will still be usueable after a clear, you should always clear it before
// deleting the IoQueue structure itself 
IoQueueResult io_queue_clear(IoQueue* io_queue)
{
    IO_QUEUE_ASSERT(io_queue);
    // pop everything
    while(io_queue_has_front(io_queue) == IO_QUEUE_RESULT_TRUE)
    {
        IoQueueResult result = io_queue_pop(io_queue);
        IO_QUEUE_ASSERT(result == IO_QUEUE_RESULT_SUCCESS);
    }
    return IO_QUEUE_RESULT_SUCCESS;
}

#endif