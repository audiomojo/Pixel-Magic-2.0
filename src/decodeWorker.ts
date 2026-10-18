import workerpool from 'workerpool';
import { computeImageFactsWire, endDecodeResources } from './decode.js';

// Decode child entry (no logic). Registered as `computeImageFacts`; hashes come back as
// decimal strings because process workers talk JSON, which can't carry bigint.
workerpool.worker(
  { computeImageFacts: computeImageFactsWire },
  { onTerminate: endDecodeResources },
);
