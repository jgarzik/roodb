//! Client authentication shared by all wire protocols.
//!
//! Accounts live in `system.users`. The stored credential is the
//! mysql_native_password hash, hex(SHA1(SHA1(password))), which can be
//! checked against either a native challenge response or a cleartext
//! password received over TLS. Each protocol supplies the check for the
//! credential form it receives; everything else is common.

use std::net::IpAddr;
use std::sync::Arc;

use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;
use tracing::warn;

use crate::catalog::system_tables::SYSTEM_USERS;
use crate::executor::Datum;
use crate::sql::privileges::HostPattern;
use crate::storage::StorageEngine;

/// Why authentication did not succeed.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    /// The client must not be let in. Deliberately carries no reason:
    /// unknown user, wrong password, locked account and expired password
    /// are indistinguishable to the client.
    Denied,
    /// The account store could not be read
    Internal(String),
}

/// An account row from `system.users`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    /// hex(SHA1(SHA1(password))), or empty for an empty password
    pub password_hash: String,
    /// Account is administratively locked
    pub account_locked: bool,
    /// Password has expired
    pub password_expired: bool,
}

/// Hash checked when the account does not exist, so that an unknown user
/// costs the same work as a wrong password.
const DUMMY_PASSWORD_HASH: &str = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

/// Authenticate `username` connecting from `client_ip`.
///
/// `verify` checks the client's credential against a stored password hash
/// (hex(SHA1(SHA1(password))), empty for an empty password).
pub async fn authenticate(
    storage: &Arc<dyn StorageEngine>,
    username: &str,
    client_ip: IpAddr,
    verify: impl Fn(&str) -> bool,
) -> Result<(), AuthError> {
    let record = lookup_user(storage, username, &client_ip.to_string())
        .await
        .map_err(AuthError::Internal)?;

    match check(record.as_ref(), verify) {
        Ok(()) => Ok(()),
        Err(reason) => {
            warn!(username, %client_ip, reason, "Authentication failed");
            Err(AuthError::Denied)
        }
    }
}

/// Decide whether the credential opens the account. The error is the
/// reason for the server log; it is never sent to the client.
///
/// The credential is always verified, even for a missing, locked or
/// expired account, so the outcome does not depend on how far the check
/// got.
pub fn check(
    record: Option<&UserRecord>,
    verify: impl Fn(&str) -> bool,
) -> Result<(), &'static str> {
    let stored_hash = record.map_or(DUMMY_PASSWORD_HASH, |r| r.password_hash.as_str());
    let credential_ok = verify(stored_hash);

    match record {
        None => Err("unknown user"),
        Some(_) if !credential_ok => Err("wrong password"),
        Some(r) if r.account_locked => Err("account is locked"),
        Some(r) if r.password_expired => Err("password has expired"),
        Some(_) => Ok(()),
    }
}

/// Hash a password for storage: hex(SHA1(SHA1(password))), the
/// mysql_native_password format. An empty password hashes to "".
pub fn compute_password_hash(password: &str) -> String {
    if password.is_empty() {
        return String::new();
    }
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(stage1);
    hex::encode(stage2)
}

/// Check a cleartext password against a stored password hash in constant
/// time.
pub fn verify_cleartext_password(password: &str, stored_hash_hex: &str) -> bool {
    let computed = compute_password_hash(password);
    bool::from(computed.as_bytes().ct_eq(stored_hash_hex.as_bytes()))
}

/// Find the `system.users` row for `username` whose host pattern matches
/// `client_host`.
pub async fn lookup_user(
    storage: &Arc<dyn StorageEngine>,
    username: &str,
    client_host: &str,
) -> Result<Option<UserRecord>, String> {
    let rows = crate::catalog::store::scan_rows(storage.as_ref(), SYSTEM_USERS)
        .await
        .map_err(|e| format!("Storage error: {}", e))?;

    // system.users schema: username, host, password_hash, auth_plugin,
    //   ssl_subject, ssl_issuer, account_locked, password_expired, created_at, updated_at
    for stored in rows {
        let row = stored.row;

        match row.get_opt(0) {
            Some(Datum::String(s)) if s == username => {}
            _ => continue,
        }

        let Some(Datum::String(row_host)) = row.get_opt(1) else {
            continue;
        };
        if !HostPattern::new(row_host).matches(client_host, None) {
            continue;
        }

        let password_hash = match row.get_opt(2) {
            Some(Datum::String(s)) => s.clone(),
            Some(Datum::Null) | None => String::new(),
            _ => continue,
        };
        let flag = |idx| matches!(row.get_opt(idx), Some(Datum::Bool(true)));

        return Ok(Some(UserRecord {
            password_hash,
            account_locked: flag(6),
            password_expired: flag(7),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(password: &str) -> UserRecord {
        UserRecord {
            password_hash: compute_password_hash(password),
            account_locked: false,
            password_expired: false,
        }
    }

    fn cleartext(password: &str) -> impl Fn(&str) -> bool + '_ {
        move |stored| verify_cleartext_password(password, stored)
    }

    #[test]
    fn password_hash_format() {
        assert_eq!(compute_password_hash(""), "");
        // SHA1(SHA1("password")), as produced by MySQL's PASSWORD()
        assert_eq!(
            compute_password_hash("password"),
            "2470c0c06dee42fd1618bb99005adca2ec9d1e19"
        );
    }

    #[test]
    fn right_password_accepted() {
        let r = record("secret123");
        assert_eq!(check(Some(&r), cleartext("secret123")), Ok(()));
    }

    #[test]
    fn wrong_password_rejected() {
        let r = record("secret123");
        assert_eq!(
            check(Some(&r), cleartext("secret124")),
            Err("wrong password")
        );
        assert_eq!(check(Some(&r), cleartext("")), Err("wrong password"));
    }

    #[test]
    fn empty_password_account() {
        let r = record("");
        assert_eq!(check(Some(&r), cleartext("")), Ok(()));
        assert!(check(Some(&r), cleartext("x")).is_err());
    }

    #[test]
    fn unknown_user_rejected_for_any_credential() {
        assert_eq!(check(None, cleartext("")), Err("unknown user"));
        assert_eq!(check(None, cleartext("secret123")), Err("unknown user"));
        // Even a verifier that accepts everything cannot open a missing account.
        assert_eq!(check(None, |_| true), Err("unknown user"));
    }

    #[test]
    fn unknown_user_still_runs_the_verifier() {
        let called = std::cell::Cell::new(false);
        let _ = check(None, |_| {
            called.set(true);
            false
        });
        assert!(called.get());
    }

    #[test]
    fn locked_and_expired_accounts_rejected_even_with_right_password() {
        let mut locked = record("pw");
        locked.account_locked = true;
        assert_eq!(
            check(Some(&locked), cleartext("pw")),
            Err("account is locked")
        );

        let mut expired = record("pw");
        expired.password_expired = true;
        assert_eq!(
            check(Some(&expired), cleartext("pw")),
            Err("password has expired")
        );
    }

    #[test]
    fn wrong_password_on_locked_account_reports_wrong_password() {
        let mut locked = record("pw");
        locked.account_locked = true;
        assert_eq!(
            check(Some(&locked), cleartext("nope")),
            Err("wrong password")
        );
    }
}
