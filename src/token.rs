//! Zello auth tokens, for consumer Zello (Zello Friends and Family).
//!
//! Zello wants a JSON Web Token on logon: the Issuer and an expiry, signed
//! (RS256) with the Private Key from developers.zello.com. The plugin makes a
//! fresh one for each logon, good for two minutes, so there's no 30-day
//! development token to renew. Zello Work doesn't use tokens.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde_json::json;

/// How long a token is good for, as in Zello's own examples.
const LIFETIME_S: u64 = 120;

/// Makes tokens. Holds the key, so it has no `Debug`: nothing prints it.
pub struct TokenMaker {
    issuer: String,
    key: RsaKeyPair,
    rng: SystemRandom,
}

impl TokenMaker {
    pub fn new(issuer: &str, private_key: &str) -> Result<TokenMaker, String> {
        let issuer = issuer.trim();
        if issuer.is_empty() {
            return Err("Add the Issuer from developers.zello.com (Keys).".into());
        }
        Ok(TokenMaker { issuer: issuer.to_string(), key: parse_key(private_key)?, rng: SystemRandom::new() })
    }

    /// A token for a logon at `now` (Unix seconds).
    pub fn token(&self, now: u64) -> Result<String, String> {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(json!({ "iss": self.issuer, "exp": now + LIFETIME_S }).to_string());
        let input = format!("{header}.{payload}");
        let mut sig = vec![0; self.key.public().modulus_len()];
        self.key.sign(&RSA_PKCS1_SHA256, &self.rng, input.as_bytes(), &mut sig).map_err(|_| "couldn't sign the Zello token".to_string())?;
        Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig)))
    }
}

/// The key as pasted: PEM ("BEGIN PRIVATE KEY" or "BEGIN RSA PRIVATE KEY"),
/// with or without its line breaks, or the bare base64.
fn parse_key(text: &str) -> Result<RsaKeyPair, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("Add the Private Key from developers.zello.com (Keys).".into());
    }
    if text.contains("ENCRYPTED") {
        return Err("The Private Key is encrypted. Paste the key exactly as Zello gives it.".into());
    }
    let pkcs1 = text.contains("BEGIN RSA PRIVATE KEY");
    // Drop the -----BEGIN …----- and -----END …----- markers, then all whitespace.
    let mut s = text.to_string();
    while let Some(a) = s.find("-----") {
        let Some(b) = s[a + 5..].find("-----") else { break };
        s.replace_range(a..a + 5 + b + 5, " ");
    }
    let body: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    let unreadable = "The Private Key isn't readable. Paste the whole key, including its BEGIN and END lines.";
    let der = STANDARD.decode(body).map_err(|_| unreadable.to_string())?;
    let key = if pkcs1 { RsaKeyPair::from_der(&der) } else { RsaKeyPair::from_pkcs8(&der).or_else(|_| RsaKeyPair::from_der(&der)) };
    key.map_err(|e| format!("The Private Key isn't an RSA key Zello tokens can be signed with ({e})."))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use ring::signature::{RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};
    use serde_json::Value;

    // A throwaway 2048-bit RSA key made for these tests only (it signs nothing
    // real), as DER in base64. The tests put the PEM markers around it.
    pub const TEST_PKCS8: &str = include_str!("../testdata/test-key-pkcs8.b64");
    const TEST_PKCS1: &str = include_str!("../testdata/test-key-pkcs1.b64");

    pub fn pem(label: &str, b64: &str) -> String {
        let body: String = b64.split_whitespace().collect();
        let lines: Vec<&str> = body.as_bytes().chunks(64).map(|c| std::str::from_utf8(c).unwrap()).collect();
        format!("-----BEGIN {label}-----\n{}\n-----END {label}-----\n", lines.join("\n"))
    }

    fn part(token: &str, i: usize) -> Vec<u8> {
        URL_SAFE_NO_PAD.decode(token.split('.').nth(i).unwrap()).unwrap()
    }

    #[test]
    fn makes_a_signed_token() {
        let t = TokenMaker::new("  my-issuer ", &pem("PRIVATE KEY", TEST_PKCS8)).unwrap();
        let token = t.token(1_000_000).unwrap();
        assert_eq!(token.split('.').count(), 3);
        let header: Value = serde_json::from_slice(&part(&token, 0)).unwrap();
        assert_eq!(header["alg"], "RS256");
        let payload: Value = serde_json::from_slice(&part(&token, 1)).unwrap();
        assert_eq!(payload["iss"], "my-issuer");
        assert_eq!(payload["exp"], 1_000_120);
        // The signature checks out with the key's public half.
        let signed = token.rsplit_once('.').unwrap().0;
        let public = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, t.key.public().as_ref());
        public.verify(signed.as_bytes(), &part(&token, 2)).expect("signature verifies");
    }

    #[test]
    fn takes_the_key_however_it_is_pasted() {
        assert!(TokenMaker::new("i", &pem("RSA PRIVATE KEY", TEST_PKCS1)).is_ok(), "PKCS#1");
        assert!(TokenMaker::new("i", &pem("PRIVATE KEY", TEST_PKCS8).replace('\n', " ")).is_ok(), "line breaks lost");
        assert!(TokenMaker::new("i", TEST_PKCS8).is_ok(), "bare base64");
    }

    #[test]
    fn says_what_is_wrong() {
        let err = |issuer: &str, key: &str| TokenMaker::new(issuer, key).err().unwrap();
        assert!(err("", &pem("PRIVATE KEY", TEST_PKCS8)).contains("Issuer"));
        assert!(err("i", "  ").contains("Add the Private Key"));
        assert!(err("i", "-----BEGIN PRIVATE KEY-----\nnot base64!\n-----END PRIVATE KEY-----").contains("isn't readable"));
        assert!(err("i", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----").contains("isn't an RSA key"));
        assert!(err("i", "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----").contains("encrypted"));
    }
}
