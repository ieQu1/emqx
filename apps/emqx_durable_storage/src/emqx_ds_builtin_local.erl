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

%% API:
-export([]).

%% behavior callbacks:
-export([
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
    current_timestamp/2
]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-type iterator() :: term().

-type delete_iterator() :: term().

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec open_db(emqx_ds:db(), emqx_ds:create_db_opts()) -> ok | {error, _}.
open_db(DB, Opts) ->
    case emqx_ds_builtin_local_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(_DB) ->
    ok.

-spec update_db_config(emqx_ds:db(), emqx_ds:create_db_opts()) -> ok | {error, _}.
update_db_config(_DB, _Opts) ->
    ok.

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{emqx_ds:generation_rank() => emqx_ds:generation_info()}.
list_generations_with_lifetimes(_DB) ->
    #{}.

-spec drop_generation(emqx_ds:db(), emqx_ds:generation_rank()) -> ok | {error, _}.
drop_generation(_DB, _Rank) ->
    ok.

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(_DB) ->
    ok.

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) -> emqx_ds:store_batch_result().
store_batch(_DB, _Messages, _Opts) ->
    ok.

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) -> [{emqx_ds:stream_rank(), emqx_ds:ds_specific_stream()}].
get_streams(_DB, _TF, _Time) ->
    [].

-spec make_iterator(emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(emqx_ds:ds_specific_iterator()).
make_iterator(_DB, _Stream, _TF, _Time) ->
    ok.

-spec update_iterator(emqx_ds:db(), emqx_ds:ds_specific_iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(emqx_ds:ds_specific_iterator()).
update_iterator(_DB, Iter, _Key) ->
    Iter.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(_DB, _Iter, _N) ->
    ok.

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), time()) -> [emqx_ds:ds_specific_delete_stream()].
get_delete_streams(_DB, _TF, _Time) ->
    [].

-spec make_delete_iterator(emqx_ds:db(), emqx_ds:ds_specific_delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    make_delete_iterator_result(ds_specific_delete_iterator()).
make_delete_iterator(_DB, _Stream, _TF, _Time) ->
    ok.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    delete_next_result(delete_iterator()).
delete_next(_DB, _Iter, _Selector, _N) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
