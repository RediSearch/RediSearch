/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

// Serializing and tests functions.
public:
HNSWIndex(std::ifstream &input, const HNSWParams *params,
          const AbstractIndexInitParams &abstractInitParams,
          const IndexComponents<DataType, DistType> &components,
          HNSWSerializer::EncodingVersion version);

// Validates the connections between vectors
HNSWIndexMetaData checkIntegrity() const;

// Index memory size might be changed during index saving.
virtual void saveIndexIMP(std::ofstream &output) override;

// used by index factory to load nodes connections
void restoreGraph(std::ifstream &input, HNSWSerializer::EncodingVersion version);

private:
HNSWSerializer::EncodingVersion getWriteVersion() const override;

void saveIndexFields(std::ofstream &output) const override;

void saveGraph(std::ofstream &output) const;

void saveLevel(std::ofstream &output, ElementLevelData &data) const;
void restoreLevel(std::ifstream &input, ElementLevelData &data,
                  HNSWSerializer::EncodingVersion version);
void computeIndegreeForAll();

// Functions for index loading.
void restoreIndexFields(std::ifstream &input);
void fieldsValidation() const;
