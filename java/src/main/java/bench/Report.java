package bench;

import org.HdrHistogram.Histogram;

record Report(long fills, long volume, int resting, long checksum, Histogram hist) {}
