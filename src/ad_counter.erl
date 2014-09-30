-module(ad_counter).
-export([run/1, consume_ad/3]).


run(Counter) ->
    Key = stupid, 
    spawn(ad_counter, consume_ad, [Key, 0, 20]),
    spawn(ad_counter, consume_ad, [Key, 0, 10]),
    spawn(ad_counter, consume_ad, [Key, 0, 15]),
    derflow_api:threshold_read(Key, riak_dt_gcounter, Counter).

consume_ad(_Key, Limit, Limit) ->
    ok;
consume_ad(Key, Count, Limit) ->
    Rand = random:uniform(10),
    timer:sleep(Rand),
    derflow_api:update(Key, riak_dt_gcounter, {increment, chinese}),
    consume_ad(Key, Count+1, Limit).
    
