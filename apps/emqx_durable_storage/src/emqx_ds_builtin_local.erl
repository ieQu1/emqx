%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local).

-behavior(emqx_ds).
-behavior(emqx_ds_buffer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    %% `emqx_ds':
    open_db/2,
    add_generation/1,
    update_db_config/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    update_iterator/3,
    next/3,
    delete_next/4,
    shard_of_message/3,

    %% `emqx_ds_buffer':
    init_buffer/3,
    flush_buffer/4,
    shard_of_message/4
]).

%% internal exports:
-export([current_timestamp/0]).

-export_type([iterator/0, delete_iterator/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

-define(IT, 61).
-define(DELETE_IT, 62).

-type shard() :: binary().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := shard(),
        ?enc := term()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := shard(),
        ?enc := term()
    }.

-type db_opts() ::
    #{
        backend := builtin_local,
        storage := emqx_ds_storage_layer:prototype(),
        n_shards := pos_integer()
    }.

-define(stream(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec open_db(emqx_ds:db(), db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts) ->
    case emqx_ds_builtin_local_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    Errors = lists:filtermap(
        fun(Shard) ->
            case emqx_ds_storage_layer:add_generation({DB, Shard}, current_timestamp()) of
                ok ->
                    false;
                Error ->
                    {true, {Shard, Error}}
            end
        end,
        Shards
    ),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

-spec update_db_config(emqx_ds:db(), db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    Opts = #{} = emqx_ds_builtin_local_meta:update_db_config(DB, CreateOpts),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:update_config({DB, Shard}, current_timestamp(), Opts)
        end,
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{emqx_ds:generation_rank() => emqx_ds:generation_info()}.
list_generations_with_lifetimes(DB) ->
    lists:foldl(
        fun(Shard, Acc) ->
            maps:fold(
                fun(GenId, Data, Acc1) ->
                    Acc1#{{Shard, GenId} => Data}
                end,
                Acc,
                emqx_ds_storage_layer:list_generations_with_lifetimes({DB, Shard})
            )
        end,
        #{},
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec drop_generation(emqx_ds:db(), emqx_ds:generation_rank()) -> ok | {error, _}.
drop_generation(DB, {Shard, GenId}) ->
    emqx_ds_storage_layer:drop_generation({DB, Shard}, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(_DB) ->
    ok.

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Messages, Opts) ->
    try
        emqx_ds_buffer:store_batch(DB, Messages, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

-record(bs, {options :: term(), latest :: integer()}).
-type buffer_state() :: #bs{}.

-spec init_buffer(emqx_ds:db(), shard(), _Options) -> {ok, buffer_state()}.
init_buffer(DB, Shard, Options) ->
    {ok, #bs{options = Options, latest = 0}}.

-spec flush_buffer(emqx_ds:db(), shard(), [emqx_types:message()], buffer_state()) ->
    {buffer_state(), emqx_ds:store_batch_result()}.
flush_buffer(DB, Shard, Messages, S0 = #bs{options = Options, latest = Latest0}) ->
    {Latest, Batch} = assign_timestamps(Latest0, Messages),
    Result = emqx_ds_storage_layer:store_batch({DB, Shard}, Batch, Options),
    {S0#bs{latest = Latest}, Result}.

assign_timestamps(Latest, Messages) ->
    assign_timestamps(Latest, Messages, []).

assign_timestamps(Latest, [MessageIn | Rest], Acc) ->
    case emqx_message:timestamp(MessageIn, microsecond) of
        TimestampUs when TimestampUs > Latest ->
            Message = assign_timestamp(TimestampUs, MessageIn),
            assign_timestamps(TimestampUs, Rest, [Message | Acc]);
        _Earlier ->
            Message = assign_timestamp(Latest + 1, MessageIn),
            assign_timestamps(Latest + 1, Rest, [Message | Acc])
    end;
assign_timestamps(Latest, [], Acc) ->
    {Latest, lists:reverse(Acc)}.

assign_timestamp(TimestampUs, Message) ->
    {TimestampUs, Message}.

-spec shard_of_message(emqx_ds:db(), emqx_types:message(), clientid | topic, _Options) -> shard().
shard_of_message(DB, #message{from = From, topic = Topic}, SerializeBy, _Options) ->
    N = emqx_ds_builtin_local_meta:n_shards(DB),
    Hash =
        case SerializeBy of
            clientid -> erlang:phash2(From, N);
            topic -> erlang:phash2(Topic, N)
        end,
    integer_to_binary(Hash).

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), emqx_ds:ds_specific_stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer:get_streams({DB, Shard}, TopicFilter, StartTime),
            lists:map(
                fun({RankY, InnerStream}) ->
                    Rank = {Shard, RankY},
                    {Rank, ?stream(Shard, InnerStream)}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds:ds_specific_iterator()).
make_iterator(DB, ?stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case emqx_ds_storage_layer:make_iterator(ShardId, InnerStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec update_iterator(emqx_ds:db(), emqx_ds:ds_specific_iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(DB, Iter0 = #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0}, Key) ->
    case emqx_ds_storage_layer:update_iterator({DB, Shard}, StorageIter0, Key) of
        {ok, StorageIter} ->
            {ok, Iter0#{?enc => StorageIter}};
        {error, Err} ->
            {error, unrecoverable, Err};
        Err = {error, _, _} ->
            Err
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter0 = #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0}, N) ->
    ShardId = {DB, Shard},
    T0 = erlang:monotonic_time(microsecond),
    Result = emqx_ds_storage_layer:next(ShardId, StorageIter0, N, current_timestamp()),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    case Result of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [emqx_ds:ds_specific_delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer:get_delete_streams({DB, Shard}, TopicFilter, StartTime),
            lists:map(
                fun(InnerStream) ->
                    ?delete_stream(Shard, InnerStream)
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_delete_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, ?delete_stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case emqx_ds_storage_layer:make_delete_iterator(ShardId, InnerStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(emqx_ds:delete_iterator()).
delete_next(DB, Iter = #{?tag := ?DELETE_IT, ?shard := Shard, ?enc := StorageIter0}, Selector, N) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:delete_next(ShardId, StorageIter0, Selector, N, current_timestamp())
    of
        {ok, StorageIter, Ndeleted} ->
            {ok, Iter#{?enc => StorageIter}, Ndeleted};
        {ok, end_of_stream} ->
            {ok, end_of_stream};
        Error ->
            Error
    end.

%%================================================================================
%% Internal exports
%%================================================================================

current_timestamp() ->
    emqx_ds_builtin_local_meta:current_timestamp().

-spec shard_of_message(emqx_ds:db(), emqx_types:message(), clientid | topic) ->
    emqx_ds_replication_layer:shard_id().
shard_of_message(DB, {_Timestamp, #message{from = From, topic = Topic}}, SerializeBy) ->
    N = emqx_ds_replication_shard_allocator:n_shards(DB),
    Hash =
        case SerializeBy of
            clientid -> erlang:phash2(From, N);
            topic -> erlang:phash2(Topic, N)
        end,
    integer_to_binary(Hash).

%%================================================================================
%% Internal functions
%%================================================================================
