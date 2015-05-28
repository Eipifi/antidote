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

-export([
  get_clock/0,
  read_object/3,
  execute_transaction/2]).

%% Returns the K-durable clock as perceived by this DC
get_clock() ->
  %% TODO: implement actual k-durability
  case clocksi_interactive_tx_coord_fsm:get_snapshot_time() of
    {ok, VC} -> {ok, dict:to_list(VC)};
    {error, Reason} -> {error, Reason}
  end.

%% Reads the object with the specified key and dependencies
read_object(Clock, Key, Type) ->
  case antidote:clocksi_read(dict:from_list(Clock), Key, Type) of
    {ok,{_, [Val], ActualClock}} ->
      {ok, {Val, dict:to_list(ActualClock)}};
    {error, Reason} ->
      {error, Reason}
    end.

%% Executes the given transaction
execute_transaction(OTID, {Clock, Operations}) ->
  {ClientID, _} = OTID,
  Fun = fun(OP) -> format_operation(OP, ClientID) end,
  case antidote:clocksi_execute_tx(dict:from_list(Clock), lists:map(Fun, Operations)) of
    {ok, {_, _, CommitTime}} ->
      DcId = dc_utilities:get_my_dc_id(),
      {ok, ClockSiValue} = vectorclock:get_clock_of_dc(DcId, CommitTime),
      {ok, {DcId, ClockSiValue}};
    {error, Reason} -> {error, Reason}
  end.

format_operation({Key, Type, Method, Args}, Actor) ->
  OpParam = case Args of
    [] -> Method;
    _ -> list_to_tuple([ Method | Args])
  end,
  {update, Key, Type, {OpParam, Actor}}.