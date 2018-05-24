-module(dispatcher).

-include_lib("riak_pb/include/antidote_pb.hrl").
-behaviour(gen_server).

-export([start_link/0,
        terminate/2,
        code_change/3,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        init/1]).

-export([start_transaction/3,
         abort_transaction/2,
         commit_transaction/2,
         update_objects/3,
         read_values/3
         ]).

-define(TIMEOUT, 10000).
-record(state, {
    pid :: pid(),
    buffer
}).

%% -------- Generic server callbacks -------- %%

init(_Args) -> 
    materializer:init(),
    {ok, #state{buffer = []}}.

encode_send_decode(Pid, Request, Args, State) ->
    EncMsg = antidote_pb_codec:encode(Request,Args),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
            {error, timeout} -> {reply, {error, timeout}, State};
        _ ->
            {reply, antidote_pb_codec:decode_response(Result), State}
    end.

handle_call({start_transaction, Pid, TimeStamp, TxnProperties}, _From, State) ->
    encode_send_decode(Pid, start_transaction, {TimeStamp, TxnProperties}, State);

handle_call({abort_transaction, Pid, TxId}, _From, State) ->
    encode_send_decode(Pid, abort_transaction, TxId, State);

handle_call({commit_transaction, Pid, TxId}, _From, State) ->
    encode_send_decode(Pid, commit_transaction, TxId, State);

handle_call({update_objects, Pid, Updates, TxId}, _From, State) ->
    materializer:update(Updates, {interactive, TxId}),
    encode_send_decode(Pid, update_objects, {Updates, TxId}, State);

handle_call({read_objects, Pid, Objects, TxId}, _From, State) ->
    M = materializer:read(Objects, {interactive, TxId}),
    io:format("Materialize value: ~p\n", [M]),
    encode_send_decode(Pid, read_objects, {Objects, TxId}, State);

handle_call(_Request, _From, State) -> {noreply, State}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

code_change(_Oldversion, State, _Extra) -> {ok, State}.

terminate(Reason, _State) -> Reason.


%% -------- API -------- %%

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start_transaction(Pid::term(), TimeStamp::term(), TxnProperties::term())
        -> {ok, {interactive, term()} | {static, {term(), term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    gen_server:call(?MODULE, {start_transaction, Pid, TimeStamp, TxnProperties}).   

-spec abort_transaction(Pid::term(), TxId::term()) -> ok.
abort_transaction(Pid, {interactive, TxId}) ->
    gen_server:call(?MODULE, {abort_transaction, Pid, TxId}).

-spec commit_transaction(Pid::term(), TxId::{interactive,term()} | {static,term()}) ->
                                {ok, term()} | {error, term()}.
commit_transaction(Pid, {interactive, TxId}) ->
    gen_server:call(?MODULE, {commit_transaction, Pid, TxId});

commit_transaction(Pid, {static, _TxId}) ->
    antidotec_pb_socket:get_last_commit_time(Pid).

-spec update_objects(Pid::term(), Updates::[{term(), term(), term()}], TxId::term()) -> ok | {error, term()}.
update_objects(Pid, Updates, {interactive, TxId}) ->
    gen_server:call(?MODULE, {update_objects, Pid, Updates, TxId});

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
    gen_server:call(?MODULE, {read_objects, Pid, Objects, TxId});

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