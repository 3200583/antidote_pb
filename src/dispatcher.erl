-module(dispatcher).

-include_lib("riak_pb/include/antidote_pb.hrl").

-export([
         start_transaction/3,
         abort_transaction/2,
         commit_transaction/2,
         update_objects/3,
         read_values/3
         ]).

-define(TIMEOUT, 10000).

-spec start_transaction(Pid::term(), TimeStamp::term(), TxnProperties::term())
        -> {ok, {interactive, term()} | {static, {term(), term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    EncMsg = antidote_pb_codec:encode(start_transaction,
                                              {TimeStamp, TxnProperties}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> 
            {error, timeout};
        _ ->
            antidote_pb_codec:decode_response(Result)
    end.


-spec abort_transaction(Pid::term(), TxId::term()) -> ok.
abort_transaction(Pid, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec:encode(abort_transaction, TxId),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> 
            {error, timeout};
        _ ->
            antidote_pb_codec:decode_response(Result)
    end.

-spec commit_transaction(Pid::term(), TxId::{interactive,term()} | {static,term()}) ->
                                {ok, term()} | {error, term()}.
commit_transaction(Pid, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec:encode(commit_transaction, TxId),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            antidote_pb_codec:decode_response(Result)
    end;

commit_transaction(Pid, {static, _TxId}) ->
    antidotec_pb_socket:get_last_commit_time(Pid).

-spec update_objects(Pid::term(), Updates::[{term(), term(), term()}], TxId::term()) -> ok | {error, term()}.
update_objects(Pid, Updates, {interactive, TxId}) ->
    materializer:update(Updates, {interactive, TxId}),
    EncMsg = antidote_pb_codec:encode(update_objects, {Updates, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            antidote_pb_codec: decode_response(Result)
    end;

update_objects(Pid, Updates, {static, TxId}) ->
    materializer:update(Updates, {interactive, TxId}),
    {Clock, Properties} = TxId,
    EncMsg = antidote_pb_codec:encode(static_update_objects,
                                      {Clock, Properties, Updates}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction, CommitTimeStamp} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    ok;
                {error, Reason} -> {error, Reason}
            end
    end.

-spec read_values(Pid::term(), Objects::[term()], TxId::term()) -> {ok, [term()]}  | {error, term()}.
read_values(Pid, Objects, {interactive, TxId}) ->
    materializer:read(Objects, {interactive, TxId}),
    EncMsg = antidote_pb_codec:encode(read_objects, {Objects, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            antidote_pb_codec:decode_response(Result)
    end;

read_values(Pid, Objects, {static, TxId}) ->
    {Clock, Properties} = TxId,
    materializer:read(Objects, {interactive, TxId}),
    EncMsg = antidote_pb_codec:encode(static_read_objects,
                                      {Clock, Properties, Objects}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {static_read_objects_resp, Values, CommitTimeStamp} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    {ok, Values};
                {error, Reason} -> {error, Reason}
            end
    end.