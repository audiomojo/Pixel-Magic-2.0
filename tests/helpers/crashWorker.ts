import workerpool from 'workerpool';
import { computeImageFactsWire } from '../../src/decode.js';

// Decode child that dies hard (SIGKILL, like a segfault) on ext `crash`, and decodes normally
// otherwise, so a test can show the pool replaces the dead child.
workerpool.worker({
  computeImageFacts: (abs: string, ext: string) => {
    if (ext === 'crash') process.kill(process.pid, 'SIGKILL');
    return computeImageFactsWire(abs, ext);
  },
});
