-module(ad_counter).
-export([run/1, consume_ad/4]).


run(Counter) ->
    Key = stupid, 
    spawn(ad_counter, consume_ad, [Key, 0, 5, chinese]),
    spawn(ad_counter, consume_ad, [Key, 0, 5, belgian]),
    %spawn(ad_counter, consume_ad, [Key, 0, 15]),
    derflow_api:threshold_read(Key, riak_dt_gcounter, Counter).

consume_ad(Key, Count, Limit, Actor) ->
    Rand = random:uniform(10),
    timer:sleep(Rand),
    case Count of
        Limit ->
            ok;
        _ ->
            derflow_api:update(Key, riak_dt_gcounter, {increment, Actor}),
            consume_ad(Key, Count+1, Limit, Actor)
    end.
    
