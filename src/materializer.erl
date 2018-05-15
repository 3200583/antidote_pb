-module(materializer).

-export([read/2,
        update/2]).


read(_Objects, _TxProperties) ->
    ok.

update(Updates, _TxProperties) ->
    [{{_, Type, _}, _Op, _Value} | _] = Updates,
    Obj = Type:new(),
    Result = apply_update(Updates, Obj),
    io:format("Materializer update objects result: ~p\n",[Result]),
    ok.

apply_update([], Acc) ->
    Acc;

apply_update([Update|Tail], Acc) ->
    {{_, Type, _}, Op, Value} = Update,
    {ok, Result} = Type:downstream({Op, Value}, Acc),
    apply_update(Tail, Result).