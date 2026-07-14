// Conditional field visibility for node property forms (#166 follow-up).
//
// A field with `visibleWhen` is shown only while every condition matches the
// EFFECTIVE value of its controlling field: the stored prop when set, else
// that field's defaultValue - mirroring how PropertiesPanel feeds values into
// FieldRenderer, so a fresh node (no props yet) evaluates against the same
// defaults the user sees selected. The mechanism is generic; any manifest can
// adopt it by adding `visibleWhen` to a field.
import type { ComponentManifest, Field, FieldCondition } from './types';

/**
 * The value a condition should judge: the stored prop when present, else the
 * defaultValue of the manifest field with that key (scanning every section;
 * keys are unique per manifest in practice). A key with no matching field
 * falls back to the raw prop only - safe for props like `connectionRef` that
 * exist on some component variants and not others.
 */
export function effectiveValue(
    key: string,
    manifest: ComponentManifest,
    props: Record<string, unknown>,
): unknown {
    if (props[key] !== undefined) return props[key];
    for (const section of manifest.sections) {
        const field = section.fields.find(f => f.key === key);
        if (field) return field.defaultValue;
    }
    return undefined;
}

function matches(
    cond: FieldCondition,
    manifest: ComponentManifest,
    props: Record<string, unknown>,
): boolean {
    const value = effectiveValue(cond.key, manifest, props);
    if (cond.empty !== undefined) {
        const isEmpty = value === undefined || value === null || value === '';
        if (isEmpty !== cond.empty) return false;
    }
    if (cond.equals !== undefined) {
        // Selects store strings; String() also copes with bool/number defaults.
        const s = String(value);
        const wanted = Array.isArray(cond.equals) ? cond.equals : [cond.equals];
        if (!wanted.includes(s)) return false;
    }
    return true;
}

/** True when the field has no `visibleWhen`, or every condition matches. */
export function isFieldVisible(
    field: Field,
    manifest: ComponentManifest,
    props: Record<string, unknown>,
): boolean {
    if (!field.visibleWhen) return true;
    const conds = Array.isArray(field.visibleWhen) ? field.visibleWhen : [field.visibleWhen];
    return conds.every(c => matches(c, manifest, props));
}
