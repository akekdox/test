use std::path::Path;

use anyhow::Context;
use common::serialization::{BlockContents, BlockWithoutMetadata};
use solana_ledger::blockstore::Blockstore;
use solana_sdk::hash::Hash;
use solana_transaction_status::{EntrySummary, VersionedConfirmedBlockWithEntries};

pub trait BlockstoreApi: Send + Sync {
    fn ledger_path(&self) -> &Path;
    fn is_root(&self, slot: u64) -> bool;
    fn max_root(&self) -> u64;
    fn first_available_block(&self) -> anyhow::Result<u64>;
    fn get_block_for_slot(&self, slot: u64) -> anyhow::Result<BlockContents>;
}

impl BlockstoreApi for Blockstore {
    fn ledger_path(&self) -> &Path {
        self.ledger_path()
    }

    fn is_root(&self, slot: u64) -> bool {
        self.is_root(slot)
    }

    fn max_root(&self) -> u64 {
        self.max_root()
    }

    fn first_available_block(&self) -> anyhow::Result<u64> {
        self.get_first_available_block().map_err(anyhow::Error::new)
    }

    fn get_block_for_slot(&self, slot: u64) -> anyhow::Result<BlockContents> {
        match self.get_complete_block_with_entries(
            slot, /*require_previous_blockhash:*/ false, /*populate_entries:*/ true,
            /*allow_dead_slots:*/ true,
        ) {
            Ok(VersionedConfirmedBlockWithEntries { block, .. }) => {
                Ok(BlockContents::VersionedConfirmedBlock(block))
            }
            Err(e) => {
                tracing::warn!("Failed to get complete block for slot {}: {:#}", slot, e);
                // Transaction metadata could be missing, try to fetch just the
                // entries and leave the metadata fields empty
                let Some(meta) = self.meta(slot).context("Failed to get slot meta")? else {
                    return Err(anyhow::anyhow!("No slot meta"));
                };
                let entries = self
                    .get_slot_entries(slot, /*shred_start_index:*/ 0)
                    .context("Failed to get slot entries")?;
                let blockhash = entries
                    .last()
                    .filter(|_| meta.is_full())
                    .map(|entry| entry.hash)
                    .unwrap_or(Hash::default());
                let parent_slot = meta.parent_slot.unwrap_or(0);
                let mut entry_summaries = Vec::with_capacity(entries.len());
                let mut starting_transaction_index = 0;
                let transactions = entries
                    .into_iter()
                    .flat_map(|entry| {
                        entry_summaries.push(EntrySummary {
                            num_hashes: entry.num_hashes,
                            hash: entry.hash,
                            num_transactions: entry.transactions.len() as u64,
                            starting_transaction_index,
                        });
                        starting_transaction_index += entry.transactions.len();

                        entry.transactions
                    })
                    .collect();
                let block = BlockWithoutMetadata {
                    blockhash: blockhash.to_string(),
                    parent_slot,
                    transactions,
                };
                Ok(BlockContents::BlockWithoutMetadata(block))
            }
        }
    }
}
