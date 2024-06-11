%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_local_meta).

-behavior(gen_server).

%% API:
-export([start_link/0, open_db/2, shards/1, db_config/1, update_db_config/2, current_timestamp/0]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(META_TAB, emqx_ds_builtin_local_metadata_tab).

-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_builtin_local:db_opts()
}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec open_db(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) ->
    emqx_ds_builtin_local:create_db_opts().
open_db(DB, CreateOpts = #{backend := builtin_local, storage := _, n_shards := _}) ->
    transaction(
        fun() ->
            case mnesia:wread({?META_TAB, DB}) of
                [] ->
                    mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
                    CreateOpts;
                [#?META_TAB{db_props = Opts}] ->
                    Opts
            end
        end
    ).

-spec update_db_config(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) ->
    emqx_ds_builtin_local:db_opts().
update_db_config(DB, Opts) ->
    transaction(
        fun() ->
            mnesia:write(#?META_TAB{db = DB, db_props = Opts}),
            Opts
        end
    ).

-spec shards(emqx_ds:db()) -> [emqx_ds_builtin_local:shard()].
shards(DB) ->
    #{n_shards := NShards} = db_config(DB),
    [integer_to_binary(Shard) || Shard <- lists:seq(1, NShards)].

-spec db_config(emqx_ds:db()) -> emqx_ds_builtin_local:db_opts().
db_config(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Props}] ->
            Props;
        [] ->
            error({no_such_db, DB})
    end.

-spec current_timestamp() -> emqx_ds:time().
current_timestamp() ->
    %% TODO: make it monotonic between restarts
    erlang:system_time(microsecond).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {}).

init([]) ->
    process_flag(trap_exit, true),
    ensure_tables(),
    S = #s{},
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

ensure_tables() ->
    ok = mria:create_table(?META_TAB, [
        {local_content, true},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]).

transaction(Fun) ->
    case mria:transaction(mria:local_content_shard(), Fun) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.
