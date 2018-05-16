#!/usr/bin/env escript

-define(ADDRESS, "localhost").
-define(PORT, 8087).
-define(MapKey, <<"map">>).
-define(SetKey, <<"set">>).
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
    io:format("--------- Starting Map transaction ---------\n"),
    Map = {?MapKey, antidote_crdt_map_rr, ?Bucket},
    Key = {?SetKey, antidote_crdt_set_aw},
    Op = {add, <<"test">>},
    {ok, Tx} = antidotec_pb:start_transaction(Pid, ignore, {}),
    antidotec_pb:update_objects(Pid, [{Map, update, [{Key, Op}]}], Tx),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Map], Tx),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx),
    io:format("Map: ~p\n", [Val]).