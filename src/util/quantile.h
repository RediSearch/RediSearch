#pragma once

#include <stdlib.h>
#include <stdio.h>

struct Sample {
    // Variables are named per the paper
    double v;  // Value represented
    float g;   // Number of ranks
    float d;   // Delta between ranks

    Sample *prev;
    Sample *next;
};

struct QuantStream {
    double *buffer;
    size_t bufferLength;
    size_t bufferCap;

    Sample *firstSample;
    Sample *lastSample;
    size_t n;              // Total number of values
    size_t samplesLength;  // Number of samples currently in list

    double *quantiles;
    size_t numQuantiles;
    Sample *pool;

private:
    Sample *NewSample();
    void InsertSampleAt(Sample *pos, Sample *sample);
    void AppendSample(Sample *sample);
    void RemoveSample(Sample *sample);
    double GetMaxVal(double r) const;

    void Flush();
    void Compress();

    void verifyCount() const;

public:
    QuantStream(const double *quantiles, size_t numQuantiles, size_t bufferLength);
    ~QuantStream();

    void Insert(double val);
    double Query(double val);
    void Dump(FILE *fp) const;
    size_t GetCount() const;
};
