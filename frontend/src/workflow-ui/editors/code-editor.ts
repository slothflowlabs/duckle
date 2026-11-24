import Imported from 'react-simple-code-editor';

/**
 * The code editor the routine and document editors render.
 *
 * react-simple-code-editor is CommonJS: it sets `exports.default` beside an
 * `__esModule` flag. The production build's interop hands a default import the
 * module object rather than that `default`, so `<Editor>` rendered an object,
 * React threw error #130, and the whole app went blank the moment either
 * editor opened (#408). Take `default` when it is there, which leaves an
 * interop that already unwraps it unaffected.
 */
const CodeEditor = ((Imported as unknown as { default?: typeof Imported }).default ??
    Imported) as typeof Imported;

export default CodeEditor;
