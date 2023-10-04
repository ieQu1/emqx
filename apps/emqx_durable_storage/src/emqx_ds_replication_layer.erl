%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_replication_layer).

-export([
          list_shards/1,
          open_db/2,
          store_batch/3,
          get_streams/3,
          make_iterator/2,
          next/2
        ]).


%% internal exports:
-export([ do_open_shard_v1/2,
          do_get_streams_v1/3,
          do_make_iterator_v1/3,
          do_next_v1/3
        ]).

-export_type([shard_id/0, stream/0, iterator/0, message_id/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db() :: binary().

-type shard_id() :: binary().

%% This record enapsulates the stream entity from the replication
%% level.
%%
%% TODO: currently the stream is hardwired to only support the
%% internal rocksdb storage. In t he future we want to add another
%% implementations for emqx_ds, so this type has to take this into
%% account.
-record(stream,
        { shard :: emqx_ds_replication_layer:shard_id()
        , enc :: emqx_ds_replication_layer:stream()
        }).

-opaque stream() :: stream().

-record(iterator,
        { shard :: emqx_ds_replication_layer:shard_id()
        , enc :: enqx_ds_replication_layer:iterator()
        }).

-opaque iterator() :: #iterator{}.

-type message_id() :: emqx_ds_storage_layer:message_id().

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard_id()].
list_shards(DB) ->
    %% TODO: milestone 5
    lists:map(
      fun(Node) ->
              shard_id(DB, Node)
      end,
      list_nodes()).

-spec open_db(emqx_ds:db(), emqx_ds:create_db_opts()) -> ok.
open_db(DB, Opts) ->
    lists:foreach(
      fun(Node) ->
              Shard = shard_id(DB, Node),
              emqx_ds_proto_v1:open_shard(Node, Shard, Opts)
      end,
      list_nodes()).

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
store_batch(DB, Msg, Opts) ->
    %% TODO: Currently we store messages locally.
    Shard = shard_id(DB, node()),
    emqx_ds_storage_layer:store_batch(Shard, Msg, Opts).

-spec get_streams(db(), emqx_ds:topic_filter(), emqx_ds:time()) -> [{emqx_ds:stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_replication_layer:list_shards(DB),
    lists:flatmap(
      fun(Shard) ->
              Node = node_of_shard(Shard),
              Streams = emqx_ds_proto_v1:get_streams(Node, Shard, TopicFilter, StartTime),
              [{add_shard_to_rank(Shard, Rank),
                #stream{ shard = Shard
                       , enc = I
                       }} || {Rank, I} <- Streams]
      end,
      Shards).

-spec make_iterator(stream(), emqx_ds:time()) -> {ok, iterator()} | {error, _}.
make_iterator(Stream, StartTime) ->
    #stream{shard = Shard, enc = StorageStream} = Stream,
    Node = node_of_shard(Shard),
    case emqx_ds_proto_v1:make_iterator(Node, Shard, StorageStream, StartTime) of
        {ok, Iter} ->
            {ok, #iterator{shard = Shard, enc = Iter}};
        Err = {error, _} ->
            Err
    end.

-spec next(iterator(), pos_integer()) ->
          {ok, iterator(), [emqx_types:message()]} | end_of_stream.
next(Iter0, BatchSize) ->
    #iterator{shard = Shard, enc = StorageIter0} = Iter0,
    Node = node_of_shard(Shard),
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic lary.
    case emqx_ds_proto_v1:next(Node, Shard, StorageIter0, BatchSize) of
        {ok, StorageIter, Batch} ->
            Iter = #iterator{shard = Shard, enc = StorageIter},
            {ok, Iter, Batch};
        end_of_stream ->
            end_of_stream
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

-spec do_open_shard_v1(shard_id(), emqx_ds:create_db_opts()) -> ok.
do_open_shard_v1(Shard, Opts) ->
    emqx_ds_storage_layer:open_shard(Shard, Opts).

-spec do_get_streams_v1(shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) ->
          [{integer(), _Stream}].
do_get_streams_v1(Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_streams(Shard, TopicFilter, StartTime).

-spec do_make_iterator_v1(shard_id(), _Stream, emqx_ds:time()) -> iterator().
do_make_iterator_v1(Shard, Stream, StartTime) ->
    emqx_ds_storage_layer:make_iterator(Shard, Stream, StartTime).

-spec do_next_v1(shard_id(), Iter, pos_integer()) ->
          {ok, Iter, [emqx_types:message()]} | end_of_stream.
do_next_v1(Shard, Iter, BatchSize) ->
    emqx_ds_storage_layer:next(Shard, Iter, BatchSize).

%%================================================================================
%% Internal functions
%%================================================================================

add_shard_to_rank(Shard, RankY) ->
    RankX = erlang:phash2(Shard, 255),
    {RankX, RankY}.

shard_id(DB, Node) ->
    %% TODO: don't bake node name into the schema, don't repeat the
    %% Mnesia's 1M$ mistake.
    NodeBin = atom_to_binary(Node),
    <<DB/binary, ":",  NodeBin/binary>>.

-spec node_of_shard(shard_id()) -> node().
node_of_shard(ShardId) ->
    [_DB, NodeBin] = binary:split(ShardId, <<":">>),
    binary_to_atom(NodeBin).

list_nodes() ->
    mria:running_nodes().
