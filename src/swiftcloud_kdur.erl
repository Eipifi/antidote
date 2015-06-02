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

-module(swiftcloud_kdur).

-export([update_clock_map/0, get_kdur_snapshot/1]).

update_clock_map() ->
  DcId = dc_utilities:get_my_dc_id(),
  case clocksi_interactive_tx_coord_fsm:get_snapshot_time() of
    {ok, Snapshot} ->
      EntryUpdateOp = {update, {DcId, riak_dt_lwwreg}, {assign, Snapshot}},
      OpParam = {update, [EntryUpdateOp]},
      lager:info("Updating ~p", [OpParam]),
      antidote:append(swiftcloud_clock_map, riak_dt_map, {OpParam, clock_map_update_actor});
    {error, Reason} -> {error, Reason}
  end.

get_kdur_snapshot(K) ->
  case antidote:read(swiftcloud_clock_map, riak_dt_map) of
    {ok, ClockMap} -> {ok, compute_kdur_snapshot(K, ClockMap)};
    {error, Reason} -> {error, Reason}
  end.

clockmap_get_dcs(ClockMap) ->
  F = fun({{DcId, riak_dt_lwwreg}, _Snapshot}) -> DcId end,
  lists:map(F, ClockMap).

clockmap_get_snapshots(ClockMap) ->
  F = fun({{_DcId, riak_dt_lwwreg}, Snapshot}) -> Snapshot end,
  lists:map(F, ClockMap).

compute_kdur_snapshot(K, ClockMap) ->
  Dcs = clockmap_get_dcs(ClockMap),
  Snapshots = clockmap_get_snapshots(ClockMap),
  F = fun(DcId) ->
    Values = values_for_dc(DcId, Snapshots),
    {DcId, nth_or_last(K, Values)}
  end,
  lists:map(F, Dcs).

values_for_dc(DcId, Snapshots) ->
  F = fun(S) ->
    {ok, Val} = vectorclock:get_clock_of_dc(DcId, S),
    Val
  end,
  L = lists:map(F, Snapshots),
  lists:reverse(lists:sort(L)).

nth_or_last(N, List) ->
  case length(List) > N of
    true -> lists:nth(N, List);
    false -> lists:last(List)
  end.