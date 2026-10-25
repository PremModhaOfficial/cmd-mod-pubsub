import type {SpoolFs} from './types.ts';

export function readLog(fs: Pick<SpoolFs, 'readFile'>, file: string): string {
	try {
		return fs.readFile(file);
	} catch {
		return '';
	}
}
