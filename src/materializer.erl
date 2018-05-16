-module(materializer).

-export([init/0,
        start_transaction/3,
        commit_transaction/2,
        abort_transaction/2,
        read/2,
        update/2]).


init() -> ets:new(cache, [set, named_table, {read_concurrency, true}]).

start_transaction(_Pid, _TimeStamp, _TxnProperties) ->
    ok.

commit_transaction(_Pid, _TxProp = {interactive, _TxId}) ->
    ok;

commit_transaction(_Pid, _TxProp = {static, _TxId}) ->
    ok.

abort_transaction(_Pid, _TxProp = {interactive, _TxId}) ->
    ok.

read(Objects, _TxProperties) ->
    ExtractKey = fun(O) -> {Key, _Type, Bucket} = O,
                {Key, Bucket}
             end,
    FormattedObjects = lists:map(ExtractKey, Objects),
    read_internal(FormattedObjects, []).

read_internal([], Acc) ->
    lists:reverse(Acc);

read_internal([Object|Tail], Accu) -> 
    case ets:lookup(cache, Object) of
        [] -> read_internal(Tail, Accu);
        [{{_Key,_Bucket}, Value}] -> read_internal(Tail, [Value|Accu])
    end.

update(Updates, _TxProperties) ->
    apply_update(Updates, []),
    ok.

apply_update([], Acc) ->
    lists:reverse(Acc);

apply_update([Update|Tail], Acc) ->
    {{Key, Type, Bucket}, Op, Param} = Update,
    Obj = case ets:lookup(cache, {Key, Bucket}) of
        [] -> Type:new();
        [{{_,_}, Value}] -> 
            %io:format("Use cache value: ~p\n", [Value]),
            Value
    end,
    {ok, Downstream} = Type:downstream({Op, Param}, Obj),
    {ok, Result} = Type:update(Downstream, Obj),
    %io:format("Materializer update objects result: ~p\n",[Result]),
    update_cache({Key, Bucket}, Result),
    apply_update(Tail, [Result|Acc]).

update_cache(Key = {_K, _Bucket}, Value) ->
    ets:insert(cache, {Key, Value}).