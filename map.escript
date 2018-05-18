#!/usr/bin/env escript

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

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
    dispatcher:init(),
    Key = <<"pb_client_SUITE_crdt_map_aw_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, antidote_crdt_map_go, <<"bucket">>},
    {ok, Tx1} = antidotec_pb:start_transaction(Pid1, ignore, {}),
    ok = antidotec_pb:update_objects(Pid1, [
      {Bound_object, update, {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}}}], Tx1),
    ok = antidotec_pb:update_objects(Pid1, [
      {Bound_object, update, [
        {{<<"b">>, antidote_crdt_register_lww}, {assign, <<"X">>}},
        {{<<"c">>, antidote_crdt_register_mv}, {assign, <<"Paul">>}},
        {{<<"d">>, antidote_crdt_set_aw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
        {{<<"e">>, antidote_crdt_set_rw}, {add_all, [<<"Apple">>, <<"Banana">>]}},
        {{<<"f">>, antidote_crdt_counter_pn}, {increment , 7}},
        {{<<"g">>, antidote_crdt_map_go}, {update, [
          {{<<"x">>, antidote_crdt_register_mv}, {assign, <<"17">>}}
        ]}},
        {{<<"h">>, antidote_crdt_map_rr}, {update, [
          {{<<"x">>, antidote_crdt_register_mv}, {assign, <<"15">>}}
        ]}}
      ]}], Tx1),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx1),
    %% Read committed updated
    {ok, Tx3} = antidotec_pb:start_transaction(Pid1, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_values(Pid1, [Bound_object], Tx3),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, Tx3),
    ExpectedRes = {map, [
      {{<<"a">>, antidote_crdt_register_mv}, [<<"42">>]},
      {{<<"b">>, antidote_crdt_register_lww}, <<"X">>},
      {{<<"c">>, antidote_crdt_register_mv}, [<<"Paul">>]},
      {{<<"d">>, antidote_crdt_set_aw}, [<<"Apple">>, <<"Banana">>]},
      {{<<"e">>, antidote_crdt_set_rw}, [<<"Apple">>, <<"Banana">>]},
      {{<<"f">>, antidote_crdt_counter_pn}, 7},
      {{<<"g">>, antidote_crdt_map_go}, [
        {{<<"x">>, antidote_crdt_register_mv}, [<<"17">>]}
      ]},
      {{<<"h">>, antidote_crdt_map_rr}, [
        {{<<"x">>, antidote_crdt_register_mv}, [<<"15">>]}
      ]}
    ]},
    ?assertEqual(ExpectedRes, Val),
    _Disconnected = antidotec_pb_socket:stop(Pid1).
