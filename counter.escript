#!/usr/bin/env escript

-define(ADDRESS, "localhost").
-define(PORT, 8087).
-define(CounterKey, <<"counter">>).
-define(Bucket, <<"bucket">>).

load(Dep) ->
    Path = filename:dirname(escript:script_name()) ++ "/_build/default/lib/" ++ Dep ++ "/ebin",
    case code:add_pathz(Path) of
        true ->
            true;
        Err ->
            erlang:error({could_not_load, Path, Err})
    end.

main(_) ->
    [load(Dep) || Dep <- ["riak_pb", "antidote_pb", "protobuffs", "antidote_crdt"]],
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    dispatcher:start_link(),
    Counter = {?CounterKey, antidote_crdt_counter_pn, ?Bucket},
    C = {<<"value">>, antidote_crdt_counter_pn, ?Bucket},
    {ok, Tx} = antidotec_pb:start_transaction(Pid, ignore, {}),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 11}], Tx),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 3}], Tx),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Counter], Tx),
    case antidotec_counter:value(Val) of
        X when X < 10 -> antidotec_pb:update_objects(Pid, [{C, increment, 4}], Tx);
        _ -> antidotec_pb:update_objects(Pid, [{C, increment, 7}], Tx)
    end,
    {ok, [Val2]} = antidotec_pb:read_objects(Pid, [C], Tx),
    io:format("Valeur finale: ~p\n", [Val2]),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx).
