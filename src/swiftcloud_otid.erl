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
-module(swiftcloud_otid).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/0, was_otid_observed/1]).

start_link() -> start_link(dict:new()).

start_link(Otids) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Otids, []).

init(State)-> {ok, State}.

handle_info(_Info, State)-> {noreply, State}.

handle_call({check_otid, {ClientID, TxnID}}, _From, State) ->
  CurrentValue = case dict:find(ClientID, State) of
    {ok, Value} -> Value;
    error -> 0
  end,
  case CurrentValue >= TxnID of
    true -> {reply, true, State};
    false -> {reply, false, dict:store(ClientID, TxnID, State)}
  end.

handle_cast(_Request, State) -> {noreply, State}.

%% TODO: durable storage of observed otids
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> State.

was_otid_observed(OTID) ->
  case OTID of
    none -> false;
    _ -> gen_server:call(?MODULE, {check_otid, OTID})
  end.
