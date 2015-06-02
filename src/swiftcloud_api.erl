%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(swiftcloud_api).
-include("antidote.hrl").

-export([get_clock/1, read_object/3, execute_transaction/2]).

%% Returns the K-durable clock as perceived by this DC
-spec get_clock(K::integer()) -> {ok, snapshot_time()} | {error, reason()}.
get_clock(K) -> swiftcloud_kdur:get_kdur_snapshot(K).

%% Reads the object with the specified key and dependencies
-spec read_object(Clock::snapshot_time(), Key::key(), Type::type()) -> {ok, {term(), snapshot_time()}} | {error, reason()}.
read_object(Clock, Key, Type) ->
  case antidote:clocksi_read(dict:from_list(Clock), Key, Type) of
    {ok,{_, [Val], ActualClock}} ->
      {ok, {Val, dict:to_list(ActualClock)}};
    {error, Reason} ->
      {error, Reason}
    end.

%% Executes the given transaction
-spec execute_transaction(OTID::otid(), {Clock::snapshot_time(), Operations::[any()]}) -> {ok, commit_time()} | {error, reason()}.
execute_transaction(OTID, Transaction) ->
  case swiftcloud_otid:was_otid_observed(OTID) of
    true ->
      DcId = dc_utilities:get_my_dc_id(),
      {ok, SnapshotTime} = clocksi_interactive_tx_coord_fsm:get_snapshot_time(),
      {ok, {DcId, vectorclock:get_clock_of_dc(DcId, SnapshotTime)}};
    false -> execute_after_otid_check(OTID, Transaction)
  end.

execute_after_otid_check(OTID, {Clock, Operations}) ->
  {ClientID, _} = OTID,
  Fun = fun({Key, Type, Op}) -> {update, Key, Type, {Op, ClientID}} end,
  case execute_tx_with_otid(dict:from_list(Clock), lists:map(Fun, Operations), OTID) of
    {ok, {_, _, CommitTime}} ->
      DcId = dc_utilities:get_my_dc_id(),
      {ok, ClockSiValue} = vectorclock:get_clock_of_dc(DcId, CommitTime),
      {ok, {DcId, ClockSiValue}};
    {error, Reason} ->
      %% we have a serious problem - the OTID was stored, but the transaction failed.
      {error, Reason}
  end.

execute_tx_with_otid(Clock, Operations, OTID) ->
  {ok, _} = clocksi_static_tx_coord_sup:start_fsm([self(), Clock, Operations, OTID]),
  receive
    EndOfTx ->
      EndOfTx
  end.