-module(derflow_api).
-export([threshold_read/3,
        update/3]).

threshold_read(Key, Type, Threshold) ->
    {ok, Result} = floppy:read(Key, Type),
    case satisfy_threshold(Result, Type, Threshold) of
        true ->
            {ok, Threshold};
        false ->
            timer:sleep(100),
            threshold_read(Key, Type, Threshold)
    end.

update(Key, Type, Operation) ->
    floppy:append(Key, Type, Operation).

%%Internal functions
satisfy_threshold(Result, Type, Threshold) ->
    case Type of
        riak_dt_gcounter ->
            Result >= Threshold;
        riak_dt_gset ->
            ResultSet = ordsets:from_list(Result),
            ThresSet = ordsets:from_list(Threshold),
            ThresSet == ordsets:intersection(ResultSet, ThresSet);
        riak_dt_lwwreg ->
            true
    end.
