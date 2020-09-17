// Copyright 2020 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

use super::{Trait, Module, Store};
use crate::configuration::{self, HostConfiguration};
use sp_std::prelude::*;
use sp_std::collections::{btree_map::BTreeMap, vec_deque::VecDeque};
use frame_support::{StorageMap, StorageValue, weights::Weight, traits::Get};
use primitives::v1::{Id as ParaId, UpwardMessage};

pub trait UmpSink {
	/// Process an incoming upward message and return the amount of weight it consumed.
	fn process_upward_message(origin: ParaId, msg: Vec<u8>) -> Weight;
}

impl UmpSink for () {
	fn process_upward_message(_: ParaId, _: Vec<u8>) -> Weight {
		0
	}
}

impl<T: Trait> Module<T> {
	pub(super) fn outgoing_para_cleanup_ump(outgoing_para: ParaId) {
		<Self as Store>::RelayDispatchQueueSize::remove(&outgoing_para);
		<Self as Store>::RelayDispatchQueues::remove(&outgoing_para);
		<Self as Store>::NeedsDispatch::mutate(|v| {
			if let Ok(i) = v.binary_search(&outgoing_para) {
				v.remove(i);
			}
		});
		<Self as Store>::NextDispatchRoundStartWith::mutate(|v| {
			*v = v.filter(|p| *p == outgoing_para)
		});
	}

	/// Check that all the upward messages sent by a candidate pass the acceptance criteria. Returns
	/// false, if any of the messages doesn't pass.
	pub(crate) fn check_upward_messages(
		config: &HostConfiguration<T::BlockNumber>,
		para: ParaId,
		upward_messages: &[UpwardMessage],
	) -> bool {
		if upward_messages.len() as u32 > config.max_upward_message_num_per_candidate {
			return false;
		}

		let (mut para_queue_count, mut para_queue_size) =
			<Self as Store>::RelayDispatchQueueSize::get(&para);

		for msg in upward_messages {
			para_queue_count += 1;
			para_queue_size += msg.len() as u32;
		}

		// make sure that the queue is not overfilled.
		// we do it here only once since returning false invalidates the whole relay-chain block.
		if para_queue_count > config.max_upward_queue_count
			|| para_queue_size > config.max_upward_queue_size
		{
			return false;
		}

		true
	}

	/// Enacts all the upward messages sent by a candidate.
	pub(crate) fn enact_upward_messages(
		para: ParaId,
		upward_messages: Vec<UpwardMessage>,
	) -> Weight {
		let mut weight = 0;

		if !upward_messages.is_empty() {
			let (extra_cnt, extra_size) = upward_messages
				.iter()
				.fold((0, 0), |(cnt, size), d| (cnt + 1, size + d.len() as u32));

			<Self as Store>::RelayDispatchQueues::mutate(&para, |v| {
				v.extend(upward_messages.into_iter())
			});

			<Self as Store>::RelayDispatchQueueSize::mutate(
				&para,
				|(ref mut cnt, ref mut size)| {
					*cnt += extra_cnt;
					*size += extra_size;
				},
			);

			<Self as Store>::NeedsDispatch::mutate(|v| {
				if let Err(i) = v.binary_search(&para) {
					v.insert(i, para);
				}
			});

			weight += T::DbWeight::get().reads_writes(3, 3);
		}

		weight
	}

	/// Devote some time into dispatching pending upward messages.
	pub(crate) fn process_pending_upward_messages() {
		let mut weight = 0;

		let mut queue_cache: BTreeMap<ParaId, VecDeque<UpwardMessage>> = BTreeMap::new();

		let mut needs_dispatch: Vec<ParaId> = <Self as Store>::NeedsDispatch::get();
		let start_with = <Self as Store>::NextDispatchRoundStartWith::get();

		let config = <configuration::Module<T>>::config();

		let mut idx = match start_with {
			Some(para) => match needs_dispatch.binary_search(&para) {
				Ok(found_idx) => found_idx,
				// well, that's weird, since the `NextDispatchRoundStartWith` is supposed to be reset.
				// let's select 0 as the starting index as a safe bet.
				Err(_supposed_idx) => 0,
			},
			None => 0,
		};

		loop {
			// find the next dispatchee
			let dispatchee = match needs_dispatch.get(idx) {
				Some(para) => {
					// update the index now. It may be used to set `NextDispatchRoundStartWith`.
					idx = (idx + 1) % needs_dispatch.len();
					*para
				}
				None => {
					// no pending upward queues need processing at the moment.
					break;
				}
			};

			if weight >= config.preferred_dispatchable_upward_messages_step_weight {
				// Then check whether we've reached or overshoot the
				// preferred weight for the dispatching stage.
				//
				// if so - bail.
				break;
			}

			// deuque the next message from the queue of the dispatchee
			let queue = queue_cache
				.entry(dispatchee)
				.or_insert_with(|| <Self as Store>::RelayDispatchQueues::get(&dispatchee));
			match queue.pop_front() {
				Some(upward_msg) => {
					weight += T::UmpSink::process_upward_message(dispatchee, upward_msg);
				}
				None => {}
			}

			if queue.is_empty() {
				// the queue is empty - this para doesn't need attention anymore.
				match needs_dispatch.binary_search(&dispatchee) {
					Ok(i) => {
						let _ = needs_dispatch.remove(i);
					}
					Err(_) => {
						// the invariant is we dispatch only queues that present in the
						// `needs_dispatch` in the first place.
						//
						// that should not be harmful though.
						debug_assert!(false);
					}
				}
			}
		}

		let next_one = needs_dispatch.get(idx).cloned();
		<Self as Store>::NextDispatchRoundStartWith::set(next_one);
		<Self as Store>::NeedsDispatch::put(needs_dispatch);
	}
}

#[cfg(test)]
pub(crate) mod mock_sink {
	use super::{UmpSink, UpwardMessage, ParaId};
	use std::cell::RefCell;
	use std::collections::vec_deque::VecDeque;
	use frame_support::weights::Weight;

	#[derive(Debug)]
	struct UmpExpectation {
		expected_origin: ParaId,
		expected_msg: UpwardMessage,
		mock_weight: Weight,
	}

	std::thread_local! {
		// `Some` here indicates that there is an active probe.
		static HOOK: RefCell<Option<VecDeque<UmpExpectation>>> = RefCell::new(None);
	}

	pub struct MockUmpSink;
	impl UmpSink for MockUmpSink {
		fn process_upward_message(actual_origin: ParaId, actual_msg: Vec<u8>) -> Weight {
			HOOK.with(|opt_hook| match &mut *opt_hook.borrow_mut() {
				Some(hook) => {
					let UmpExpectation {
						expected_origin,
						expected_msg,
						mock_weight,
					} = match hook.pop_front() {
						Some(expectation) => expectation,
						None => {
							panic!(
								"The probe is active but didn't expect the message {:?}.",
								actual_msg,
							);
						}
					};
					assert_eq!(expected_origin, actual_origin);
					assert_eq!(expected_msg, actual_msg);
					mock_weight
				}
				None => 0,
			})
		}
	}

	pub struct Probe {
		_private: (),
	}

	impl Probe {
		pub fn new() -> Self {
			HOOK.with(|opt_hook| {
				let prev = opt_hook.borrow_mut().replace(VecDeque::default());

				// that can trigger if there were two probes were created during one session which
				// is may be a bit strict, but may save time figuring out what's wrong.
				// if you land here and you do need the two probes in one session consider
				// dropping the the existing probe explicitly.
				assert!(prev.is_none());
			});
			Self { _private: () }
		}

		pub fn assert_msg(
			&mut self,
			expected_origin: ParaId,
			expected_msg: UpwardMessage,
			mock_weight: Weight,
		) {
			HOOK.with(|opt_hook| {
				opt_hook
					.borrow_mut()
					.as_mut()
					.unwrap()
					.push_back(UmpExpectation {
						expected_origin,
						expected_msg,
						mock_weight,
					})
			});
		}
	}

	impl Drop for Probe {
		fn drop(&mut self) {
			let _ = HOOK.try_with(|opt_hook| {
				let prev = opt_hook.borrow_mut().take().expect(
					"this probe was created and hasn't been yet destroyed;
					the probe cannot be replaced;
					there is only one probe at a time allowed;
					thus it cannot be `None`;
					qed",
				);
				if !prev.is_empty() {
					panic!(
						"the probe is dropped and not all expected messages arrived: {:?}",
						prev
					);
				}
			});
			// an `Err` here signals here that the thread local was already destroyed.
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use super::mock_sink::Probe;
	use crate::router::tests::default_genesis_config;
	use crate::mock::{Configuration, Router, new_test_ext};

	struct GenesisConfigBuilder {
		max_upward_message_num_per_candidate: u32,
		max_upward_queue_count: u32,
		max_upward_queue_size: u32,
		preferred_dispatchable_upward_messages_step_weight: Weight,
	}

	impl Default for GenesisConfigBuilder {
		fn default() -> Self {
			Self {
				max_upward_message_num_per_candidate: 2,
				max_upward_queue_count: 4,
				max_upward_queue_size: 64,
				preferred_dispatchable_upward_messages_step_weight: 1000,
			}
		}
	}

	impl GenesisConfigBuilder {
		fn build(self) -> crate::mock::GenesisConfig {
			let mut cfg = default_genesis_config();
			cfg.configuration
				.config
				.max_upward_message_num_per_candidate = self.max_upward_message_num_per_candidate;
			cfg.configuration.config.max_upward_queue_count = self.max_upward_queue_count;
			cfg.configuration.config.max_upward_queue_size = self.max_upward_queue_size;
			cfg.configuration
				.config
				.preferred_dispatchable_upward_messages_step_weight =
				self.preferred_dispatchable_upward_messages_step_weight;
			cfg
		}
	}

	fn queue_upward_msg(para: ParaId, msg: UpwardMessage) {
		let msgs = vec![msg];
		assert!(Router::check_upward_messages(
			&Configuration::config(),
			para,
			&msgs,
		));
		let _ = Router::enact_upward_messages(para, msgs);
	}

	#[test]
	fn dispatch_empty() {
		new_test_ext(default_genesis_config()).execute_with(|| {
			// make sure that the case with empty queues is handled properly
			Router::process_pending_upward_messages();
		});
	}

	#[test]
	fn dispatch_single_message() {
		let a = ParaId::from(228);
		let msg = vec![1, 2, 3];

		new_test_ext(
			GenesisConfigBuilder {
				..Default::default()
			}
			.build(),
		)
		.execute_with(|| {
			let mut probe = Probe::new();

			probe.assert_msg(a, msg.clone(), 0);
			queue_upward_msg(a, msg);

			Router::process_pending_upward_messages();
		});
	}
}
