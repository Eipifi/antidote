-module(derflow_adcounter_test).
-export([consume_ad/5,
         test/1]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),
    
    rt:wait_for_service(Node, notification),

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, [Node]),
    lager:info("Done!"),

    ?assertEqual({ok, 5}, Result),
    pass.

-endif.
    

test(Node) ->
    lager:error("W1"),
    Key = key1, 
    spawn(?MODULE, consume_ad, [Node, Key, 0, 5, manu]),
    spawn(?MODULE, consume_ad, [Node, Key, 0, 5, li]),
    
    Threshold = 5,
    derflow_floppy:threshold_read(Key, riak_dt_gcounter, Threshold).

consume_ad(Node, Key, Count, Limit, Actor) ->
    Rand = random:uniform(10),
    timer:sleep(Rand),
    case Count of
        Limit ->
            ok;
        _ ->
            {ok, _} = derflow_floppy:update(Key, riak_dt_gcounter, {increment, Actor}),
            consume_ad(Node, Key, Count+1, Limit, Actor)
    end.
    
