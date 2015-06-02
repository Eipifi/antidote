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

-module(swiftcloud_otid_fsm).
-behaviour(gen_fsm).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4, start_link/1, ready/3, start_link/0, was_otid_observed/1]).

start_link() -> start_link(dict:new()).

start_link(Otids) -> gen_fsm:start_link({local, ?MODULE}, ?MODULE, Otids, []).

init(Otids) -> {ok, ready, Otids}.

ready({check_otid, {ClientID, TxnID}}, _From, State) ->
  lager:info("Check_otid called"),
  CurrentValue = case dict:find(ClientID, State) of
    {ok, Value} -> Value;
    error -> 0
  end,
  case CurrentValue >= TxnID of
    true -> {reply, true, ready, State};
    false -> {reply, false, ready, dict:store(ClientID, TxnID, State)}
  end.

handle_info(_Info, _StateName, StateData) -> {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.

was_otid_observed(OTID) ->
  case OTID of
    none -> false;
    _ -> gen_fsm:sync_send_event(?MODULE, {check_otid, OTID})
  end.
