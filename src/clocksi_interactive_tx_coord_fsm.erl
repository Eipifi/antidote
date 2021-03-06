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
%% @doc The coordinator for a given Clock SI interactive transaction.
%%      It handles the state of the tx and executes the operations
%%      sequentially by sending each operation to the responsible
%%      clockSI_vnode of the involved key. when a tx is finalized
%%      (committed or aborted, the fsm also finishes.

-module(clocksi_interactive_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/2, start_link/1]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([execute_op/3,
         finish_op/3,
         prepare/2,
         receive_prepared/2,
         committing/3,
         receive_committed/2,
         abort/2,
         reply_to_client/2]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process 
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction
%%----------------------------------------------------------------------
-record(state, {
        from :: {pid(), term()},
        transaction :: tx(),
        updated_partitions :: preflist(),
        num_to_ack :: non_neg_integer(),
        prepare_time :: non_neg_integer(),
        commit_time :: non_neg_integer(),
        state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock], []).

start_link(From) ->
    gen_fsm:start_link(?MODULE, [From, ignore], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock]) ->
    {ok, SnapshotTime} = case ClientClock of
        ignore ->
            get_snapshot_time();
        _ ->
            get_snapshot_time(ClientClock)
    end,
    DcId = dc_utilities:get_my_dc_id(),
    {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    Transaction = #transaction{snapshot_time=LocalClock,
                               vec_snapshot_time=SnapshotTime,
                               txn_id=TransactionId},
    SD = #state{transaction=Transaction, updated_partitions=[], prepare_time=0},
    From ! {ok, TransactionId},
    {ok, execute_op, SD}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op({OpType, Args}, Sender,
           SD0=#state{transaction=Transaction, updated_partitions=UpdatedPartitions}) ->
    case OpType of
        prepare ->
            {next_state, prepare, SD0#state{from=Sender}, 0};
        read ->
            {Key, Type} = Args,
            Preflist = log_utilities:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            case clocksi_vnode:read_data_item(IndexNode, Transaction,
                                              Key, Type) of
                {error, Reason} ->
                    {reply, {error, Reason}, abort, SD0, 0};
                {ok, Snapshot} ->
                    ReadResult = Type:value(Snapshot),
                    {reply, {ok, ReadResult}, execute_op, SD0}
            end;
        update ->
            {Key, Type, Param} = Args,
            Preflist = log_utilities:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            case clocksi_downstream:generate_downstream_op(Transaction, IndexNode, Key, Type, Param) of
                {ok, DownstreamRecord} ->
                    case clocksi_vnode:update_data_item(IndexNode, Transaction, Key, Type, DownstreamRecord) of
                        ok ->
                            case lists:member(IndexNode, UpdatedPartitions) of
                                false ->
                                    NewUpdatedPartitions = [IndexNode|UpdatedPartitions],
                                    {reply, ok, execute_op, SD0#state{updated_partitions=NewUpdatedPartitions}};
                                true->
                                    {reply, ok, execute_op, SD0}
                            end;
                        {error, Reason} ->
                            {reply, {error, Reason}, abort, SD0, 0}
                    end;
                {error, Reason} ->
                    {reply, {error, Reason}, abort, SD0, 0}
            end
    end.

%% @doc a message from a client wanting to start committing the tx.
%%      this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(timeout, SD0=#state{transaction=Transaction, updated_partitions=UpdatedPartitions, from=From}) ->
    case length(UpdatedPartitions) of
        0 ->
            SnapshotTime = Transaction#transaction.snapshot_time,
            gen_fsm:reply(From, {ok, SnapshotTime}),
            {next_state, committing, SD0#state{state=committing, commit_time=SnapshotTime}};
        _->
            clocksi_vnode:prepare(UpdatedPartitions, Transaction),
            NumToAck = length(UpdatedPartitions),
            {next_state, receive_prepared, SD0#state{num_to_ack=NumToAck, state=prepared}}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck, from=From, prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of
        1 ->
            gen_fsm:reply(From, {ok, MaxPrepareTime}),
            {next_state, committing, S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}};
        _ ->
            {next_state, receive_prepared, S0#state{num_to_ack=NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

%% @doc after receiving all prepare_times, send the commit message to all
%%       updated partitions, and go to the "receive_committed" state.
committing(commit, Sender, SD0=#state{transaction=Transaction,
                                      updated_partitions=UpdatedPartitions,
                                      commit_time=CommitTime}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            {next_state, reply_to_client, SD0#state{state=committed, from=Sender}, 0};
        _ ->
            clocksi_vnode:commit(UpdatedPartitions, Transaction, CommitTime),
            {next_state, receive_committed, SD0#state{num_to_ack=NumToAck, from=Sender, state=committing}}
    end.

%% @doc the fsm waits for acks indicating that each partition has successfully
%%	committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack=NumToAck}) ->
    case NumToAck of
        1 ->
            {next_state, reply_to_client, S0#state{state=committed}, 0};
        _ ->
            {next_state, receive_committed, S0#state{num_to_ack=NumToAck-1}}
    end.

%% @doc When an error occurs or an updated partition
%%      does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{transaction=Transaction,
                          updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction),
    {next_state, reply_to_client, SD0#state{state=aborted},0};

abort(abort, SD0=#state{transaction=Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    clocksi_vnode:abort(UpdatedPartitions, Transaction),
    {next_state, reply_to_client, SD0#state{state=aborted},0}.

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(timeout, SD=#state{from=From, transaction=Transaction, state=TxState, commit_time=CommitTime}) ->
    case undefined =/= From of
        true ->
            TxId = Transaction#transaction.txn_id,
            Reply = case TxState of
                committed ->
                    DcId = dc_utilities:get_my_dc_id(),
                    CausalClock = vectorclock:set_clock_of_dc(DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                    {ok, {TxId, CausalClock}};
                aborted ->
                    {aborted, TxId};
                Reason ->
                    {TxId, Reason}
            end,
            gen_fsm:reply(From, Reply);
        false ->
            ok
    end,
    {stop, normal, SD}.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time())
                       -> {ok, snapshot_time()} | {error, reason()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).

-spec get_snapshot_time() -> {ok, snapshot_time()} | {error, reason()}.
get_snapshot_time() ->
    Now = clocksi_vnode:now_microsec(erlang:now()),
    case vectorclock:get_stable_snapshot() of
        {ok, VecSnapshotTime} ->
            DcId = dc_utilities:get_my_dc_id(),
            SnapshotTime = dict:update(DcId,
                                       fun (_Old) -> Now end,
                                       Now, VecSnapshotTime),
            {ok, SnapshotTime};
        {error, Reason} ->
            {error, Reason}
    end.

-spec wait_for_clock(snapshot_time()) ->
                           {ok, snapshot_time()} | {error, reason()}.
wait_for_clock(Clock) ->
   case get_snapshot_time() of
       {ok, VecSnapshotTime} ->
           case vectorclock:ge(VecSnapshotTime, Clock) of
               true ->
                   %% No need to wait
                   {ok, VecSnapshotTime};
               false ->
                   %% wait for snapshot time to catch up with Client Clock
                   timer:sleep(10),
                   wait_for_clock(Clock)
           end;
       {error, Reason} ->
          {error, Reason}
  end.
