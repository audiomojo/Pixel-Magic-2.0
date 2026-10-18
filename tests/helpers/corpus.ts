import { existsSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** The private, read-only sample corpus. Tests read it in place and never write to it. */
const CORPUS_DIR = fileURLToPath(new URL('../../test-automation/sample-images/', import.meta.url));

export const hasCorpus = existsSync(CORPUS_DIR);

/** Absolute path of a corpus file. */
export const C = (name: string): string => CORPUS_DIR + name;

/** Sorted corpus file names accepted by `filter`. */
export const corpusNames = (filter: (name: string) => boolean): string[] =>
  readdirSync(CORPUS_DIR).filter(filter).sort();
