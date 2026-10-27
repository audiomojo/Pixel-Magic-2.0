import workerpool from 'workerpool';
import { computeImageFactsWire, endDecodeResources, pixelDigest } from './decode.js';

// Decode child entry (no logic). Registered as `computeImageFacts`; hashes come back as
// decimal strings because process workers talk JSON, which can't carry bigint.
// `pixelDigest` already returns plain numbers and a hex string.
workerpool.worker(
  { computeImageFacts: computeImageFactsWire, pixelDigest },
  { onTerminate: endDecodeResources },
);
