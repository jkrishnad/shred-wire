//! Which of a client's named filters a transaction matches.
//!
//! A filter matches when every part of it holds, and a part left empty always holds.
//! vote: true only vote txs, false none of them, unset both.
//! account_include: at least one of these accounts, account_exclude: none of them,
//! account_required: all of them.
//! Accounts are the tx's static keys. Accounts it loads through a lookup table are not
//! known from shreds alone, so a filter never matches on those.

use std::collections::HashSet;
use std::str::FromStr;

use shredwire_proto::TxFilter;
use solana_transaction::simple_vote_transaction_checker::is_simple_vote_transaction_impl;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction::{Address, VersionedMessage};

/// What filters look at, worked out once per tx when it is decoded.
#[derive(Debug, Default)]
pub struct TxKeys {
    pub accounts: Vec<Address>,
    pub vote: bool,
}

impl TxKeys {
    pub fn of(tx: &VersionedTransaction) -> Self {
        let accounts = tx.message.static_account_keys();
        let programs = tx
            .message
            .instructions()
            .iter()
            .filter_map(|ix| accounts.get(usize::from(ix.program_id_index)));
        // the same check Agave uses to mark a tx as a vote
        let legacy = matches!(tx.message, VersionedMessage::Legacy(_));
        let vote = is_simple_vote_transaction_impl(&tx.signatures, legacy, programs);
        Self {
            accounts: accounts.to_vec(),
            vote,
        }
    }
}

#[derive(Debug)]
pub struct Filter {
    vote: Option<bool>,
    include: HashSet<Address>,
    exclude: HashSet<Address>,
    required: Vec<Address>,
}

impl Filter {
    /// Reads a filter as the client sent it. Err names the account that is not a valid address.
    pub fn parse(filter: &TxFilter) -> Result<Self, String> {
        Ok(Self {
            vote: filter.vote,
            include: addresses(&filter.account_include)?,
            exclude: addresses(&filter.account_exclude)?,
            required: addresses(&filter.account_required)?.into_iter().collect(),
        })
    }

    pub fn matches(&self, tx: &TxKeys) -> bool {
        if self.vote.is_some_and(|vote| vote != tx.vote) {
            return false;
        }
        if !self.include.is_empty() && !tx.accounts.iter().any(|a| self.include.contains(a)) {
            return false;
        }
        if tx.accounts.iter().any(|a| self.exclude.contains(a)) {
            return false;
        }
        self.required.iter().all(|r| tx.accounts.contains(r))
    }
}

fn addresses(keys: &[String]) -> Result<HashSet<Address>, String> {
    keys.iter()
        .map(|key| Address::from_str(key).map_err(|_| format!("{key} is not a valid address")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(vote: Option<bool>, include: &[&str], exclude: &[&str], required: &[&str]) -> Filter {
        let list = |keys: &[&str]| keys.iter().map(|k| k.to_string()).collect();
        Filter::parse(&TxFilter {
            vote,
            account_include: list(include),
            account_exclude: list(exclude),
            account_required: list(required),
        })
        .unwrap()
    }

    #[test]
    fn test_matches() {
        // the vote program and the system program, real addresses
        let vote = "Vote111111111111111111111111111111111111111";
        let system = "11111111111111111111111111111111";
        let tx = |accounts: &[&str], is_vote| TxKeys {
            accounts: accounts
                .iter()
                .map(|a| Address::from_str(a).unwrap())
                .collect(),
            vote: is_vote,
        };
        let voting = tx(&[vote], true);
        let transfer = tx(&[system], false);

        let everything = filter(None, &[], &[], &[]);
        assert!(everything.matches(&voting) && everything.matches(&transfer));

        let no_votes = filter(Some(false), &[], &[], &[]);
        assert!(!no_votes.matches(&voting) && no_votes.matches(&transfer));

        let touches_system = filter(None, &[system], &[], &[]);
        assert!(!touches_system.matches(&voting) && touches_system.matches(&transfer));

        let not_system = filter(None, &[], &[system], &[]);
        assert!(not_system.matches(&voting) && !not_system.matches(&transfer));

        let both = filter(None, &[], &[], &[system, vote]);
        assert!(!both.matches(&transfer) && both.matches(&tx(&[system, vote], false)));

        assert!(
            Filter::parse(&TxFilter {
                account_include: vec!["not-a-key".into()],
                ..TxFilter::default()
            })
            .is_err()
        );
    }
}
