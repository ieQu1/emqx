%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_machine).
-moduledoc """
Ra Machine implementation

This code decides how successfully replicated and committed log entries (e.g.
commands) are applied to the shard storage state. This state is actually comprised
logically of 2 parts:

1. RocksDB database managed through `emqx_ds_storage_layer`.

2. Machine state (`ra_state()`) that holds very minimal state needed to ensure
   higher-level semantics, most importantly strictly monotonic quasi-wallclock
   timestamp used to assign unique message timestamps to fulfill "append-only"
   guarantees.

There are few subtleties in how storage state is persisted and recovered.
When the shard recovers from a shutdown or crash, this is what usually happens:

1. Shard storage layer starts up the RocksDB database.
2. Ra recovers the Raft log.
3. Ra recovers the latest machine snapshot (`ra_state()`), taken at some point
   in time (`RaftIdx`).
4. Ra applies existing Raft log entries starting from `RaftIdx`.

While most of the time storage layer state, machine snapshot and log entries are
consistent with each other, there are situations when they are not. Namely:
 * RocksDB decides to flush memtables to disk by itself, which is unexpected but
   possible.
 * Lagging replica accepts a storage snapshot sourced from a RocksDB checkpoint,
   and RocksDB database is always implicitly flushed before checkpointing.
In both of those cases, the Raft log would contain entries that were already
applied from the point of view of the storage layer, and we must anticipate that.

The process running Ra machine also keeps auxiliary ephemeral state in the process
dictionary, see `?pd_ra_*` macrodefs for details.

## Upgrades

Current version of the state machine callback module is represented by
`?code_version` macro. This value must be increased when incompatible
changes are made to the state machine and, importantly, to the storage
layer. This includes adding new storage layouts or changing the
semantics of some operations.

Code version is compared against a `vsn` field stored in the machine
state ("state version"), and the following logic applies:

- If the state version is less than the code version, then the leader
  should make the decision to upgrade the state version, preferably
  taking code version of the replicas into consideration. State
  upgrade is done explicitly by issuing a raft command. Before it is
  done, the state machine can operate normally, but care must be taken
  to make sure all underlying code follows the old paths.

- If the code version is equal to the state version, then operate
  normally.

- If the state version is greater than the code version, then abort
  all operations and signal the need to upgrade EMQX version.

- If the state version is less than `?min_version`, then the situation
  should be recovered by downgrading EMQX on all nodes, running
  migration to the last supported code version, and then upgrading.
  Due to complexity of such procedure, `?min_version` should be bumped
  rarely (compatibility with the code versions of the previous major
  EMQX version should be kept, unless explicitly specified in the
  release notes).

""".

-behaviour(ra_machine).

%% API:
-export([
    add_generation/1,
    otx_new_leader/1,
    otx_commit/5,
    drop_generation/1,
    update_schema/3
]).

%% behavior callbacks:
-export([
    init/1,
    apply/3,
    tick/2,
    state_enter/2,
    snapshot_module/0
]).

%% internal exports:
-export([]).

-export_type([machine_version/0, ra_state/0, ra_command/0]).

-include("emqx_ds_builtin_raft.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-define(code_version, 1).
-define(min_version, 0).

%%================================================================================
%% Type declarations
%%================================================================================

-type machine_version() :: non_neg_integer().

%% keys:
-define(tag, 1).
%% OTX:
-define(commit_otx, 1).
-define(prev_serial, 2).
-define(serial, 3).
-define(batches, 4).
-define(otx_leader_pid, 5).
-define(otx_timestamp, 6).

-type ra_state() :: ra_state_v1() | ra_state_v0().

%% Core state of the replication, i.e. the state of ra machine.
-type ra_state_v1() :: #{
    %% State machine version.
    vsn := 1,
    %% Shard ID.
    dbshard := {emqx_ds:db(), emqx_ds:shard()},

    %% Map that stores last schema change id for each site, it is used
    %% to discard obsolete schema updates.
    last_schema_changes := #{emqx_dsch:site() => emqx_dsch:pending_id()},

    schema := emqx_ds_builtin_raft:db_schema(),
    current_gen := non_neg_integer(),

    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead of the real time clock.
    latest := emqx_ds:time(),

    %% Transaction serial.
    tx_serial => emqx_ds_optimistic_tx:serial(),

    %% Pid of the OTX leader process (used to verify that transaction
    %% was initiated during the term of the correct leader):
    otx_leader_pid => pid() | undefined
}.

-type ra_state_v0() :: #{
    %% Shard ID.
    %%
    %% Note: v0 version predates version change protocol. To prevent
    %% it from mistakingly applying changes from the newer versions
    %% and corrupting the state, we renamed `db_shard' field starting
    %% from v1 to `dbshard'. It may lead to ugly crashes, but prevent
    %% state corruption.
    db_shard := {emqx_ds:db(), emqx_ds:shard()},

    %% Map that stores last schema change id for each site, it is used
    %% to discard obsolete schema updates.
    last_schema_changes := #{emqx_dsch:site() => emqx_dsch:pending_id()},

    schema := emqx_ds_builtin_raft:db_schema(),

    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead of the real time clock.
    latest := emqx_ds:time(),

    %% Transaction serial.
    tx_serial => emqx_ds_optimistic_tx:serial(),

    %% Pid of the OTX leader process (used to verify that transaction
    %% was initiated during the term of the correct leader):
    otx_leader_pid => pid() | undefined
}.

%% Commands. Each command is an entry in the replication log.
-type cmd_otx_new_leader() :: #{
    ?tag := new_otx_leader,
    pid := pid()
}.

-type cmd_commit_tx() :: #{
    ?tag := ?commit_otx,
    ?prev_serial := emqx_ds_optimistic_tx:serial(),
    ?serial := emqx_ds_optimistic_tx:serial(),
    ?otx_timestamp := emqx_ds:time(),
    ?batches := emqx_ds_optimistic_tx:batch(),
    ?otx_leader_pid := pid()
}.

-type cmd_update_schema() :: #{
    ?tag := update_schema,
    pending_id := emqx_dsch:pending_id(),
    originator := emqx_dsch:site(),
    schema := map()
}.

-type cmd_add_generation() :: #{
    ?tag := add_generation,
    since := emqx_ds:time()
}.

-type cmd_drop_generation() :: #{
    ?tag := drop_generation,
    generation := emqx_ds:generation()
}.

-type ra_command() ::
    cmd_otx_new_leader()
    | cmd_commit_tx()
    | cmd_update_schema()
    | cmd_add_generation()
    | cmd_drop_generation().

%% Index of the last yet unreleased Ra log entry.
-define(pd_ra_idx_need_release, '$emqx_ds_raft_idx_need_release').

%% Approximate number of bytes occupied by yet unreleased Ra log entries.
-define(pd_ra_bytes_need_release, '$emqx_ds_raft_bytes_need_release').

%% How often to release Raft logs?
%% Each time we written approximately this number of bytes.
%% Close to the RocksDB's default of 64 MiB.
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000_000).
%% ...Or at least each N log entries.
-define(RA_RELEASE_LOG_MIN_FREQ, 64_000).

-ifdef(TEST).
-undef(RA_RELEASE_LOG_APPROX_SIZE).
-undef(RA_RELEASE_LOG_MIN_FREQ).
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000).
-define(RA_RELEASE_LOG_MIN_FREQ, 1_000).
-endif.

%%================================================================================
%% API functions
%%================================================================================

-spec add_generation(emqx_ds:time()) -> cmd_add_generation().
add_generation(Since) when is_integer(Since) ->
    #{?tag => add_generation, since => Since}.

-spec update_schema(emqx_dsch:pending_id(), emqx_dsch:site(), emqx_ds_builtin_raft:db_schema()) ->
    cmd_update_schema().
update_schema(PendingId, Originator, NewSchema) when
    is_integer(PendingId), is_binary(Originator), is_map(NewSchema)
->
    #{
        ?tag => update_schema,
        pending_id => PendingId,
        originator => Originator,
        schema => NewSchema
    }.

-spec drop_generation(emqx_ds:generation()) -> cmd_drop_generation().
drop_generation(Gen) when is_integer(Gen) ->
    #{?tag => drop_generation, generation => Gen}.

-spec otx_new_leader(pid()) -> cmd_otx_new_leader().
otx_new_leader(Pid) when is_pid(Pid) ->
    #{?tag => new_otx_leader, pid => Pid}.

-spec otx_commit(
    emqx_ds_optimistic_tx:serial(),
    emqx_ds_optimistic_tx:serial(),
    emqx_ds:time(),
    emqx_ds_optimistic_tx:batch(),
    pid()
) -> cmd_commit_tx().
otx_commit(PrevSerial, Serial, Time, Batch, Leader) when
    is_integer(PrevSerial), is_integer(Serial), is_integer(Time), is_list(Batch), is_pid(Leader)
->
    #{
        ?tag => ?commit_otx,
        ?prev_serial => PrevSerial,
        ?serial => Serial,
        ?otx_timestamp => Time,
        ?batches => Batch,
        ?otx_leader_pid => Leader
    }.

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init(#{
    name := _,
    db := emqx_ds:db(),
    shard := emqx_ds:shard(),
    schema := emqx_ds_builtin_raft:db_schema()
}) -> ra_state_v1().
init(#{db := DB, shard := Shard, schema := Schema}) ->
    #{
        vsn => 1,
        dbshard => {DB, Shard},
        last_schema_changes => #{},
        schema => Schema,
        latest => 0,
        tx_serial => 0,
        otx_leader_pid => undefined,
        current_gen => 0
    }.

snapshot_module() ->
    emqx_ds_builtin_raft_server_snapshot.

-spec tick(integer(), ra_state()) -> ra_machine:effects().
tick(_TimeMs, #{db_shard := _DBShard}) ->
    [].

-spec state_enter(ra_server:ra_state() | eol, ra_state()) -> ra_machine:effects().
state_enter(MemberState, State = #{db_shard := {DB, Shard}}) ->
    ?tp(
        debug,
        ds_ra_state_enter,
        State#{state => MemberState}
    ),
    emqx_ds_builtin_raft_metrics:rasrv_state_changed(DB, Shard, MemberState),
    set_cache(MemberState, State),
    _ =
        case MemberState of
            leader ->
                emqx_ds_builtin_raft_db_lifecycle:async_start_leader_sup(DB, Shard);
            _ ->
                emqx_ds_builtin_raft_db_lifecycle:async_stop_leader_sup(DB, Shard)
        end,
    [].

-spec apply(ra_machine:command_meta_data(), ra_command(), ra_state()) ->
    {ra_state(), _Reply, _Effects}.
apply(RaftMeta, Command, State) ->
    case State of
        #{vsn := MachineVersion, dbshard := DBShard} ->
            ok;
        #{db_shard := DBShard} when not is_map_key(vsn, State) ->
            MachineVersion = 0
    end,
    case MachineVersion >= ?min_version andalso MachineVersion =< ?code_version of
        true ->
            apply(RaftMeta, MachineVersion, DBShard, Command, State);
        false ->
            %% FIXME: make sure the raft server doesn't restart to
            %% avoid spamming the logs.
            ?tp(
                alert,
                ds_builtin_raft_incompatible_version,
                #{
                    reason =>
                        "Current version of EMQX is incompatible with the durable storage state",
                    machine_version => MachineVersion,
                    minimum => ?min_version,
                    maximim => ?code_version
                }
            ),
            exit(incompatible_emqx_version)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec apply(
    ra_machine:command_meta_data(),
    machine_version(),
    {emqx_ds:db(), emqx_ds:shard()},
    ra_command(),
    ra_state()
) ->
    {ra_state(), _Reply, _Effects}.
apply(
    RaftMeta,
    _MachineVersion,
    DBShard,
    #{
        ?tag := ?commit_otx,
        ?prev_serial := SerCtl,
        ?serial := Serial,
        ?otx_timestamp := Timestamp,
        ?batches := Batches,
        ?otx_leader_pid := From
    },
    State0 = #{tx_serial := ExpectedSerial, otx_leader_pid := Leader}
) ->
    case From of
        Leader when SerCtl =:= ExpectedSerial ->
            case emqx_ds_storage_layer_ttv:commit_batch(DBShard, Batches, #{durable => false}) of
                ok ->
                    emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial),
                    State = State0#{tx_serial := Serial, latest := Timestamp},
                    Result = ok,
                    set_ts(DBShard, Timestamp + 1),
                    DispatchF = fun(Stream) ->
                        emqx_ds_beamformer:shard_event(DBShard, [Stream])
                    end,
                    emqx_ds_storage_layer_ttv:dispatch_events(DBShard, Batches, DispatchF),
                    Effects = try_release_log({Serial, length(Batches)}, RaftMeta, State);
                Err = ?err_unrec(_) ->
                    State = State0,
                    Result = Err,
                    Effects = []
            end;
        Leader ->
            %% Leader pid matches, but not the serial:
            State = State0,
            Result = ?err_unrec({serial_mismatch, SerCtl, ExpectedSerial}),
            Effects = [];
        _ ->
            %% Leader mismatch:
            State = State0,
            Result = ?err_unrec({not_the_leader, #{got => From, expect => Leader}}),
            Effects = []
    end,
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    MachineVersion,
    DBShard,
    #{?tag := add_generation, since := Since},
    State0
) ->
    ?tp(
        debug,
        ds_ra_add_generation,
        #{
            shard => DBShard,
            since => Since
        }
    ),
    case MachineVersion of
        0 ->
            Result = emqx_ds_storage_layer:add_generation(DBShard, Since),
            add_generation_effects(RaftMeta, State0, Result);
        _ ->
            #{schema := #{storage := Prototype}, current_gen := LastGen} = State0,
            CurrentGen = LastGen + 1,
            State = State0#{current_gen := CurrentGen},
            ok = emqx_ds_storage_layer:add_generation(DBShard, CurrentGen, Since, Prototype),
            add_generation_effects(RaftMeta, State, ok)
    end;
apply(
    RaftMeta,
    MachineVersion,
    DBShard,
    #{?tag := update_schema, pending_id := PendingId, originator := Site, schema := Schema},
    #{last_schema_changes := LSC, latest := Latest} = State0
) ->
    ?tp(
        warning,
        ds_ra_update_config,
        #{
            shard => DBShard,
            schema => Schema,
            originator => Site,
            pending_id => PendingId,
            latest => Latest,
            lsc => LSC
        }
    ),
    State =
        case LSC of
            #{Site := NewerId} when NewerId >= PendingId ->
                %% This update has been already applied. Ignore
                %% it:
                State0;
            #{} ->
                ok = emqx_ds_storage_layer:update_config(DBShard, Latest, Schema),
                State0#{schema := Schema, last_schema_changes := LSC#{Site => PendingId}}
        end,
    Effect = release_log(RaftMeta, State),
    Effect =/= {release_cursor, 0, State} andalso
        ?tp(ds_ra_effects, #{effects => [Effect], meta => RaftMeta}),
    {State, ok, [Effect]};
apply(
    _RaftMeta,
    _MachineVersion,
    DBShard,
    #{?tag := drop_generation, generation := GenId},
    State
) ->
    ?tp(
        info,
        ds_ra_drop_generation,
        #{
            shard => DBShard,
            generation => GenId
        }
    ),
    Result = emqx_ds_storage_layer:drop_slab(DBShard, GenId),
    {State, Result};
apply(
    _RaftMeta,
    _MachineVersion,
    DBShard,
    #{
        ?tag := new_otx_leader,
        pid := Pid
    },
    State = #{tx_serial := Serial, latest := Timestamp}
) ->
    set_otx_leader(DBShard, Pid),
    Reply = {Serial, Timestamp},
    {State#{otx_leader_pid => Pid}, Reply}.

set_cache(MemberState, State = #{db_shard := DBShard, latest := Latest}) when
    MemberState =:= leader; MemberState =:= follower
->
    set_ts(DBShard, Latest),
    case State of
        #{tx_serial := Serial} ->
            emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial);
        #{} ->
            ok
    end,
    case State of
        #{otx_leader_pid := Pid} ->
            set_otx_leader(DBShard, Pid);
        #{} ->
            ok
    end;
set_cache(_, _) ->
    ok.

-doc """
Set PID of the optimistic transaction leader at the time of the last
Raft log entry applied locally. Since log replication may be delayed,
this pid may belong to a process long gone, and the pid can be even
reclaimed by other process if the node had restarted. Because of that,
DON'T SEND MESSAGES to this pid.

This pid is used ONLY to verify that the transaction context has been
created during the term of the current leader.
""".
set_otx_leader({DB, Shard}, Pid) ->
    ?tp(info, dsrepl_set_otx_leader, #{db => DB, shard => Shard, pid => Pid}),
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_otx_leader_pid, Pid).

set_ts({DB, Shard}, TS) ->
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_timestamp, TS).

try_release_log({_N, BatchSize}, RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Because cursor release means storage flush (see
    %% `emqx_ds_builtin_raft_server_snapshot:write/3`), we should do that not too often
    %% (so the storage is happy with L0 SST sizes) and not too rarely (so we don't
    %% accumulate huge Raft logs).
    case inc_bytes_need_release(BatchSize) of
        AccSize when AccSize > ?RA_RELEASE_LOG_APPROX_SIZE ->
            release_log(RaftMeta, State);
        _NotYet ->
            case get_log_need_release(RaftMeta) of
                undefined ->
                    [];
                PrevIdx when CurrentIdx - PrevIdx > ?RA_RELEASE_LOG_MIN_FREQ ->
                    %% Release everything up to the last log entry, but only if there were
                    %% more than %% `?RA_RELEASE_LOG_MIN_FREQ` new entries since the last
                    %% release.
                    release_log(RaftMeta, State);
                _ ->
                    []
            end
    end.

release_log(RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Release everything up to the last log entry. This is important: any log entries
    %% following `CurrentIdx` should not contribute to `State` (that will be recovered
    %% from a snapshot).
    update_log_need_release(RaftMeta),
    reset_bytes_need_release(),
    {release_cursor, CurrentIdx, State}.

get_log_need_release(RaftMeta) ->
    case erlang:get(?pd_ra_idx_need_release) of
        undefined ->
            update_log_need_release(RaftMeta),
            undefined;
        LastIdx ->
            LastIdx
    end.

update_log_need_release(#{index := CurrentIdx}) ->
    erlang:put(?pd_ra_idx_need_release, CurrentIdx).

get_bytes_need_release() ->
    emqx_maybe:define(erlang:get(?pd_ra_bytes_need_release), 0).

inc_bytes_need_release(Size) ->
    Acc = get_bytes_need_release() + Size,
    erlang:put(?pd_ra_bytes_need_release, Acc),
    Acc.

reset_bytes_need_release() ->
    erlang:put(?pd_ra_bytes_need_release, 0).

add_generation_effects(RaftMeta, State = #{db_shard := DBShard}, Result) ->
    emqx_ds_beamformer:generation_event(DBShard),
    Effect = release_log(RaftMeta, State),
    Effect =/= {release_cursor, 0, State} andalso
        ?tp(ds_ra_effects, #{effects => [Effect], meta => RaftMeta}),
    {State, Result, [Effect]}.
