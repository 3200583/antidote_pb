#!/usr/bin/env escript

-define(ADDRESS, "localhost").
-define(PORT, 8087).
-define(CounterKey, <<"counter">>).
-define(SetKey, <<"set">>).
-define(MapKey, <<"map">>).
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
    dispatcher:init(),
    Counter = {?CounterKey, antidote_crdt_counter_pn, ?Bucket},
    
    io:format("--------- Starting First transaction ---------\n"),
    {ok, Tx} = antidotec_pb:start_transaction(Pid, ignore, {}),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 11}], Tx),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 3}], Tx),
    antidotec_pb:read_objects(Pid, [Counter], Tx),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx),

    io:format("--------- Starting Second transaction ---------\n"),
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    antidotec_pb:read_objects(Pid, [Counter], Tx2),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 2}], Tx2),
    antidotec_pb:update_objects(Pid, [{Counter, increment, 7}], Tx2),
    {ok, [CounterPayload]} = antidotec_pb:read_objects(Pid, [Counter], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    io:format("Final value of counter = ~p\n", [antidotec_counter:value(CounterPayload)]),

    io:format("--------- Starting Third transaction ---------\n"),
    Set = {?SetKey, antidote_crdt_set_aw, ?Bucket},
    {ok, Tx3} = antidotec_pb:start_transaction(Pid, ignore, {}),
    antidotec_pb:read_objects(Pid, [Set], Tx3),
    antidotec_pb:update_objects(Pid, [{Set, add, <<"2">>}, {Set, add, <<"7">>}, {Set, add, <<"8">>}], Tx3),
    antidotec_pb:update_objects(Pid, [{Set, add, <<"7">>}, {Set, add, <<"11">>}], Tx3),
    {ok, [SetPayload]} = antidotec_pb:read_objects(Pid, [Set], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx3),
    io:format("Final value of set = ~p\n", [antidotec_set:value(SetPayload)]).