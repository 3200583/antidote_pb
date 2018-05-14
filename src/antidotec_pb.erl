%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidotec_pb).

-include_lib("riak_pb/include/antidote_pb.hrl").


-export([
         start_transaction/3,
         abort_transaction/2,
         commit_transaction/2,
         update_objects/3,
         read_objects/3,
         read_values/3]).

-define(TIMEOUT, 10000).

-spec start_transaction(Pid::term(), TimeStamp::term(), TxnProperties::term())
        -> {ok, {interactive, term()} | {static, {term(), term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    case is_static(TxnProperties) of
        true -> 
            {ok, {static, {TimeStamp, TxnProperties}}};
        false ->
            Result = dispatcher:start_transaction(Pid, TimeStamp, TxnProperties),
            case Result of
                {start_transaction, TxId} ->
                    {ok, {interactive, TxId}};
                {error, Reason} ->
                    {error, Reason};
                Other ->
                    {error, Other}
            end
    end.

-spec abort_transaction(Pid::term(), TxId::term()) -> ok.
abort_transaction(Pid, {interactive, TxId}) ->
    Result = dispatcher:abort_transaction(Pid, {interactive, TxId}),
    case Result of
        {opresponse, ok} -> ok;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end.

-spec commit_transaction(Pid::term(), TxId::{interactive,term()} | {static,term()}) ->
                                {ok, term()} | {error, term()}.
commit_transaction(Pid, {interactive, TxId}) ->
    Result = dispatcher:commit_transaction(Pid, {interactive, TxId}),
    case Result of
        {commit_transaction, CommitTimeStamp} -> {ok, CommitTimeStamp};
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end;
commit_transaction(Pid, {static, _TxId}) ->
    case dispatcher:commit_transaction(Pid, {static, _TxId}) of
        {ok, CommitTime} ->
             {ok, CommitTime}
    end.

-spec update_objects(Pid::term(), Updates::[{term(), term(), term()}], TxId::term()) -> ok | {error, term()}.
update_objects(Pid, Updates, {interactive, TxId}) ->
    Result = dispatcher:update_objects(Pid, Updates, {interactive, TxId}),
    case Result of
        {opresponse, ok} -> ok;
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end;

update_objects(Pid, Updates, {static, TxId}) ->
    Result = dispatcher:update_objects(Pid, Updates, {static, TxId}),
    case Result of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.
            
-spec read_objects(Pid::term(), Objects::[term()], TxId::term()) -> {ok, [term()]}  | {error, term()}.
read_objects(Pid, Objects, Transaction) ->
    case read_values(Pid, Objects, Transaction) of
        {ok, Values} ->
            ResObjects = lists:map(
                fun({Type, Val}) ->
                    Mod = antidotec_datatype:module_for_type(Type),
                    Mod:new(Val)
                end, Values),
            {ok, ResObjects};
        Other ->
            Other
    end.

-spec read_values(Pid::term(), Objects::[term()], TxId::term()) -> {ok, [term()]}  | {error, term()}.
read_values(Pid, Objects, {interactive, TxId}) ->
    Result = dispatcher:read_values(Pid, Objects, {interactive, TxId}),
    case Result of
        {read_objects, Values} ->
            {ok, Values};
        {error, Reason} -> {error, Reason};
        Other -> {error, Other}
    end;
read_values(Pid, Objects, {static, TxId}) ->
    Result = dispatcher:read_values(Pid, Objects, {static, TxId}),
    case Result of
        {ok, Values} -> {ok, Values};
        {error, Reason} -> {error, Reason}
    end.

is_static(TxnProperties) ->
    case TxnProperties of
        [{static, true}] ->
            true;
        _ -> false
    end.
