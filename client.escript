#!/usr/bin/env escript

-define(ADDRESS, "localhost").
-define(PORT, 8087).
-define(Key, <<"counter">>).
-define(Bucket, <<"bucket">>).

load(Dep) ->
    %Path = filename:dirname(escript:script_name()) ++ "/_build/default/lib/" ++ Dep ++ "/ebin",
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
    Bound_object = {?Key, antidote_crdt_counter_pn, ?Bucket},
    io:format("Starting Test transaction~n"),
    case antidotec_pb:start_transaction(Pid, ignore, {}) of
        {error, Reason} ->
            io:format("Could not start transaction: ~p~n", [Reason]);
        {ok, Tx} ->
            antidotec_pb:update_objects(Pid, [{Bound_object, increment, 11}], Tx),
            case antidotec_pb:read_objects(Pid, [Bound_object], Tx) of
                {error, Reason} ->
                    io:format("Could not read Counter: ~p~n", [Reason]);
                {ok, [Val]} ->
                    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx),
                    Value = antidotec_counter:value(Val),
                    true = Value >= 0,
                    io:format("Value for key ~p: ~p\n", [?Key, Value]),
                    _Disconnected = antidotec_pb_socket:stop(Pid),
                    io:format("Release is working!~n"),
                    ok
            end
    end.
    