import workerpool from 'workerpool';

// Decode child that never answers: exercises the decode timeout.
workerpool.worker({ computeImageFacts: () => new Promise(() => {}) });
