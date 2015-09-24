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
-module(vectorclock).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
  is_greater_than/2,
  get_clock_of_dc/2,
  set_clock_of_dc/3,
  get_stable_snapshot/0,
  get_partition_snapshot/1,
  from_list/1,
  new/0,
  eq/2,
  lt/2,
  gt/2,
  le/2,
  ge/2,
  strict_ge/2,
  strict_le/2]).

-export_type([vectorclock/0]).


new() ->
    dict:new().

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
-spec get_stable_snapshot() -> {ok, snapshot_time()}.
get_stable_snapshot() ->
    case meta_data_sender:get_merged_data(stable) of
	undefined ->
	    %% The snapshot isn't realy yet, need to wait for startup
	    timer:sleep(10),
	    get_stable_snapshot();
	SS ->
	    {ok, SS}
    end.

-spec get_partition_snapshot(partition_id()) -> snapshot_time().
get_partition_snapshot(Partition) ->
    case meta_data_sender:get_meta_dict(stable,Partition) of
	undefined ->
	    %% The partition isnt ready yet, wait for startup
	    timer:sleep(10),
	    get_partition_snapshot(Partition);
	SS ->
	    SS
    end.

%% @doc Return true if Clock1 > Clock2
-spec is_greater_than(Clock1 :: vectorclock(), Clock2 :: vectorclock())
                     -> boolean().
is_greater_than(Clock1, Clock2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, Clock1) of
                           {ok, Time1} ->
                               case Time1 > Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error -> %%Localclock has not observered some dcid
                               false
                       end
               end,
               true, Clock2).

get_clock_of_dc(Dcid, VectorClock) ->
    case dict:find(Dcid, VectorClock) of
        {ok, Value} ->
            {ok, Value};
        error ->
            {ok, 0}
    end.

set_clock_of_dc(DcId, Time, VectorClock) ->
    dict:update(DcId,
                fun(_Value) ->
                        Time
                end,
                Time,
                VectorClock).

from_list(List) ->
    dict:from_list(List).

eq(V1, V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 =:= Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

le(V1, V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 =< Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               Result
                       end
               end,
               true, V2).

ge(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 >= Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

lt(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 < Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               Result
                       end
               end,
               true, V2).

gt(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 > Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

strict_ge(V1,V2) ->
    ge(V1,V2) and (not eq(V1,V2)).

strict_le(V1,V2) ->
    le(V1,V2) and (not eq(V1,V2)).

-ifdef(TEST).

vectorclock_test() ->
    V1 = vectorclock:from_list([{1,5},{2,4},{3,5},{4,6}]),
    V2 = vectorclock:from_list([{1,4}, {2,3}, {3,4},{4,5}]),
    V3 = vectorclock:from_list([{1,5}, {2,4}, {3,4},{4,5}]),
    V4 = vectorclock:from_list([{1,6},{2,3},{3,1},{4,7}]),
    V5 = vectorclock:from_list([{1,6},{2,7}]),
    ?assertEqual(gt(V1,V2), true),
    ?assertEqual(lt(V2,V1), true),
    ?assertEqual(gt(V1,V3), false),
    ?assertEqual(strict_ge(V1,V3), true),
    ?assertEqual(strict_ge(V1,V1), false),
    ?assertEqual(ge(V1,V4), false),
    ?assertEqual(le(V1,V4), false),
    ?assertEqual(eq(V1,V4), false),
    ?assertEqual(ge(V1,V5), false).

-endif.
