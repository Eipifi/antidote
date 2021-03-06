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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


-define(SNAPSHOT_THRESHOLD, 10).
-define(SNAPSHOT_MIN, 2).
-define(OPS_THRESHOLD, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         read/4,
         update/2]).

%% Callbacks
-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-type cache_id() :: ets:tid().

-record(state, {
  partition :: partition_id(),
  ops_cache :: cache_id(),
  snapshot_cache :: cache_id()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time
-spec read(key(), type(), snapshot_time(), txid()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, TxId) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref,
                                        {read, Key, Type, SnapshotTime, TxId},
                                        materializer_vnode_master).


%%@doc write operation to cache for future read
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

handle_command({read, Key, Type, SnapshotTime, TxId}, Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    %% @todo Why is the return value ignored??
    _ = internal_read(Sender, Key, Type, SnapshotTime, TxId, OpsCache, SnapshotCache),
    {noreply, State};

handle_command({update, Key, DownstreamOp}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    true = op_insert_gc(Key,DownstreamOp, OpsCache, SnapshotCache),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0} ,
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
    F = fun({Key,Operation}, A) ->
                Fun(Key, Operation, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{ops_cache=OpsCache}) ->
    {Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, {Key, Operation}),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{ops_cache=OpsCache}) ->
    true = ets:delete(OpsCache),
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.



%%---------------- Internal Functions -------------------%%

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(pid() | ignore, key(), type(), snapshot_time(), txid() | ignore, cache_id(), cache_id()) -> {ok, snapshot()} | {error, no_snapshot}.
internal_read(Sender, Key, Type, SnapshotTime, TxId, OpsCache, SnapshotCache) ->
    {SnapDict, LatestSnapshot, SnapshotCommitTime} = case ets:lookup(SnapshotCache, Key) of
        [] ->
            {orddict:new(), clocksi_materializer:new(Type), ignore};
        [{_, SnapshotDict}] ->
            case get_latest_snapshot(SnapshotDict, SnapshotTime) of
                {ok, no_snapshot} ->
                    {SnapshotDict, clocksi_materializer:new(Type), ignore};
                {ok, {SCT, LS}} ->
                    {SnapshotDict, LS, SCT}
            end
    end,
    case ets:lookup(OpsCache, Key) of
        [] ->
            riak_core_vnode:reply(Sender, {ok, LatestSnapshot}),
            {ok, LatestSnapshot};
        [{_, OpsDict}] ->
            {ok, Ops} = filter_ops(OpsDict),
            case clocksi_materializer:materialize(Type, LatestSnapshot, SnapshotCommitTime, SnapshotTime, Ops, TxId) of
                {ok, Snapshot, CommitTime} ->
                    %% the following checks for the case there were no snapshots and there were operations, but none was applicable
                    %% for the given snapshot_time
                    case (CommitTime == ignore) of
                        true  ->
                            riak_core_vnode:reply(Sender, {error, no_snapshot}),
                            {error, no_snapshot};
                        false ->
                            riak_core_vnode:reply(Sender, {ok, Snapshot}),
                            SnapshotDict1 = orddict:store(CommitTime,Snapshot, SnapDict),
                            snapshot_insert_gc(Key,SnapshotDict1, OpsDict, SnapshotCache, OpsCache),
                            {ok, Snapshot}
                    end;
                {error, Reason} ->
                    riak_core_vnode:reply(Sender, {error, Reason}),
                    {error, Reason}
            end
    end.

%% @doc Obtains, from an orddict of Snapshots, the latest snapshot that can be included in
%% a snapshot identified by SnapshotTime
-spec get_latest_snapshot(orddict:orddict(), snapshot_time())
                         -> {ok, {commit_time(), snapshot()}} | {ok, no_snapshot}.
get_latest_snapshot(SnapshotDict, SnapshotTime) ->
    case SnapshotDict of
        [] ->
            {ok, no_snapshot};
        [H|T] ->
            case orddict:filter(fun(Key, _Value) ->
                                        belongs_to_snapshot(Key, SnapshotTime) end, [H|T]) of
                [] ->
                    {ok, no_snapshot};
                [H1|T1] ->
                    % @todo The SnapshotDict should be organized such that the latest snapshot is in front.
                    {CommitTime, Snapshot} = lists:last([H1|T1]),
                    {ok, {CommitTime, Snapshot}}
            end
    end.

%% @doc Get a list of operations from an orddict of operations
-spec filter_ops([{any(),any()}]) -> {ok, [any()]}.
filter_ops(Ops) ->
    MapFun = fun(X) ->
            case X of
                  {_Key, Value} ->
                    Value;
                _ ->
                  []
            end
    end,
    {ok, lists:flatmap(MapFun, Ops)}.

%% @doc Check whether a commit time is included in a snapshot time.
-spec belongs_to_snapshot(commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot({Dc, CommitTime}, SnapshotTime) ->
	{ok, Ts} = vectorclock:get_clock_of_dc(Dc, SnapshotTime),
	CommitTime =< Ts.

%% @doc Operation to insert a snapshot in the cache and start
%%      garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), orddict:orddict(), orddict:orddict(), cache_id(), cache_id()) -> true.
snapshot_insert_gc(Key, SnapshotDict, OpsDict, SnapshotCache, OpsCache)->
    case (orddict:size(SnapshotDict))==?SNAPSHOT_THRESHOLD of
        true ->
            PrunedSnapshots = orddict:from_list(lists:sublist(orddict:to_list(SnapshotDict), 1+?SNAPSHOT_THRESHOLD-?SNAPSHOT_MIN, ?SNAPSHOT_MIN)),
            FirstOp = lists:nth(1, PrunedSnapshots),
            {CommitTime, _S} = FirstOp,
            PrunedOps = prune_ops(OpsDict, CommitTime),
            ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
            ets:insert(OpsCache, {Key, PrunedOps});
        false ->
            ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops(orddict:orddict(), commit_time()) -> orddict:orddict().
prune_ops(OpsDict, Threshold) ->
    orddict:filter(fun(_Key, Value) ->
                           (belongs_to_snapshot(Threshold,(lists:last(Value))#clocksi_payload.snapshot_time)) end, OpsDict).


%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), clocksi_payload(), cache_id(), cache_id()) -> true.
op_insert_gc(Key, DownstreamOp, OpsCache, SnapshotCache)->
    OpsDict = case ets:lookup(OpsCache, Key) of
                  []->
                      orddict:new();
                  [{_, Dict}]->
                      Dict
              end,
    case (orddict:size(OpsDict))>=?OPS_THRESHOLD of
        true ->
            Type=DownstreamOp#clocksi_payload.type,
            SnapshotTime=DownstreamOp#clocksi_payload.snapshot_time,
            Type=DownstreamOp#clocksi_payload.type,
            SnapshotTime=DownstreamOp#clocksi_payload.snapshot_time,
            {_, _} = internal_read(ignore, Key, Type, SnapshotTime, ignore, OpsCache, SnapshotCache),
            OpsDict1=orddict:append(DownstreamOp#clocksi_payload.commit_time, DownstreamOp, OpsDict),
            ets:insert(OpsCache, {Key, OpsDict1});
        false ->
            OpsDict1=orddict:append(DownstreamOp#clocksi_payload.commit_time, DownstreamOp, OpsDict),
            ets:insert(OpsCache, {Key, OpsDict1})
    end.


-ifdef(TEST).

%% @doc Testing filter_ops.
filter_ops_test() ->
    Ops  = orddict:new(),
    Ops1 = orddict:append(key1, [a1, a2], Ops),
    Ops2 = orddict:append(key1, [a3, a4], Ops1),
    
    Ops3 = orddict:append(key2, [b1, b2], Ops2),
    Ops4 = orddict:append(key3, [c1, c2], Ops3),
    Result = filter_ops(Ops4),
    ?assertEqual({ok, [[a1,a2], [a3,a4], [b1,b2], [c1,c2]]}, Result).

%% @doc Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
    CommitTime1= 1,
    CommitTime2= 1,
    SnapshotClockDC1 = 5, 
    SnapshotClockDC2 = 5,
    CommitTime3= 10,
    CommitTime4= 10,

    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
    ?assertEqual(true, belongs_to_snapshot({1, CommitTime1}, SnapshotVC)),
    ?assertEqual(true, belongs_to_snapshot({2, CommitTime2}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot({1, CommitTime3}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot({2, CommitTime4}, SnapshotVC)).


seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    S1 = Type:new(),

    %% Insert one increment
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
    io:format("after making the first write: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res1} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    io:format("after making the first read: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    %% Insert second increment
    {ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC1,16}]),
                      commit_time = {DC1,20},
                      txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
    io:format("after making the second write: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res2} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),
    io:format("after making the second read: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),

    %% Read old version
    {ok, ReadOld} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).
    

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok,Op2} = Type:update(increment, b, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
                      commit_time = {DC2,20},
                      txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
    {ok, Res2} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}, {DC2,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,0}, {DC2,0}]),
                                     commit_time = {DC2, 1},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
    io:format("after inserting the first op: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res1} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC2,1}, {DC1,0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    io:format("after making the first read: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:update(increment, b, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
									  type = Type,
									  op_param = {merge, Op2},
									  snapshot_time=vectorclock:from_list([{DC1,0}, {DC2,0}]),
									  commit_time = {DC1, 1},
									  txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
    io:format("after inserting the second op: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
									  
	%% Read different snapshots
    {ok, ReadDC1} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,1}, {DC2, 0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadDC1)),
        io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,0},{DC2,1}]), ignore, OpsCache, SnapshotCache),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),
    
    %% Read snapshot including both increments
    {ok, Res2} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC2,1}, {DC1,1}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)).
    
%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
	OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Type = riak_dt_gcounter,
    {ok, ReadResult} = internal_read(ignore, key, Type, vectorclock:from_list([{dc1,1}, {dc2, 0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(ReadResult)).
    
    
-endif.
