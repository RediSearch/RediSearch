/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

struct RawDataContainer {
    enum class Status { OK = 0, ID_ALREADY_EXIST, ID_NOT_EXIST, ERR };
    /**
     * This is an abstract interface, constructor/destructor should be implemented by the derived
     * classes
     */
    RawDataContainer() = default;
    virtual ~RawDataContainer() = default;

    /**
     * @return number of elements in the container
     */
    virtual size_t size() const = 0;
    /**
     * @param element element's raw data to be added into the container
     * @param id of the new element
     * @return status
     */
    virtual Status addElement(const void *element, size_t id) = 0;
    /**
     * @param id of the element to return
     * @return Immutable reference to the element's data, NULL if id doesn't exist
     */
    virtual const char *getElement(size_t id) const = 0;
    /**
     * @param id of the element to remove
     * @return status
     */
    virtual Status removeElement(size_t id) = 0;
    /**
     * @param id to change its asociated data
     * @param element the new raw data to associate with id
     * @return status
     */
    virtual Status updateElement(size_t id, const void *element) = 0;

    struct Iterator {
        /**
         * This is an abstract interface, constructor/destructor should be implemented by the
         * derived classes
         */
        Iterator() = default;
        virtual ~Iterator() = default;

        /**
         * The basic iterator operations API
         */
        virtual bool hasNext() const = 0;
        virtual const char *next() = 0;
        virtual void reset() = 0;
    };

    /**
     * Create a new iterator. Should be freed by the iterator's destroctor.
     */
    virtual std::unique_ptr<Iterator> getIterator() const = 0;

#ifdef BUILD_TESTS
    /**
     * Save the raw data of all elements in the container to the output stream.
     */
    virtual void saveVectorsData(std::ostream &output) const = 0;
#endif
};
