/**
 * Which property keys hold a credential, by the engine's own rule:
 * `is_secret_prop_key` in crates/duckdb-engine/src/util.rs, which decides what
 * is redacted from run logs and refused on deploy.
 *
 * The form masks by declared field; a page meant to be shared, like generated
 * documentation, has to go by the key as well, because an expression field can
 * carry a connection string and an old property may have no field at all.
 * scripts/check-secret-fields.ts fails if these lists drift from the Rust ones.
 */
export const SECRET_NEEDLES = [
    'password', 'passwd', 'passphrase', 'secret', 'token', 'apikey', 'api_key',
    'privatekey', 'private_key', 'accesskey', 'access_key', 'accountkey',
    'clientsecret', 'client_secret', 'connectionstring', 'connection_string',
    'sas', 'credential',
];

/** Whole keys the needles would swallow that name something public, a token URL say. */
export const NAMES_SOMETHING_PUBLIC = [
    'tokenurl',
    'saslmechanism',
    'saslusername',
    'maxtokens',
    'maxinputtokens',
    'maxoutputtokens',
    'inputusdpermilliontokens',
    'outputusdpermilliontokens',
];

export function isSecretPropKey(key: string): boolean {
    const k = key.toLowerCase();
    // `pat` is matched whole: as a substring it would swallow path and pattern.
    if (k === 'pat') return true;
    if (NAMES_SOMETHING_PUBLIC.includes(k)) return false;
    return SECRET_NEEDLES.some(n => k.includes(n));
}
