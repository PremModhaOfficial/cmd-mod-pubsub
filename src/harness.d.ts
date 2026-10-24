// The host boundary. The mod is compiled against the real package, but the
// source tree only needs the shape the bundle imports.
declare module '@commandcode/harness' {
	export type ModApi = any;
}
