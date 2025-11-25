%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest_machine_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-import(rabbit_ct_helpers, [get_config/2]).

-define(MAC, rabbitmq_stream_s3_log_manifest_machine).
-define(META(), #{time => ?LINE}).

all() ->
    [
        spawn_writer,
        simultaneous_manifest_requests,
        out_of_order_fragment_uploads
    ].

%%----------------------------------------------------------------------------

spawn_writer(Config) ->
    Dir = get_config(Config, priv_dir),
    Mac0 = ?MAC:new(),
    Pid = self(),
    Tag = erlang:make_ref(),
    From = {Pid, Tag},
    Event1 = #writer_spawned{
        pid = Pid,
        reply_tag = Tag,
        writer_ref = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
        dir = Dir
    },
    {Mac1, Effects1} = ?MAC:apply(?META(), Event1, Mac0),
    ?assertEqual(Effects1, [
        #register_offset_listener{writer_pid = Pid, offset = -1},
        #download_manifest{dir = Dir}
    ]),
    Event2 = #manifest_downloaded{dir = Dir, manifest = undefined},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event2, Mac1),
    ?assertEqual(Effects2, [#reply{to = From, response = undefined}]),
    ok.

simultaneous_manifest_requests(Config) ->
    %% While a writer is starting up an offset reader could start up too, for
    %% example when using `stream-perf-test` on single-node RabbitMQ. The
    %% download of the manifest should be shared between any processes which
    %% request it without duplicate downloads effects.

    Dir = get_config(Config, priv_dir),
    WriterPid = self(),
    Tag = erlang:make_ref(),
    WriterFrom = {WriterPid, Tag},
    Event1 = #writer_spawned{
        pid = WriterPid,
        reply_tag = Tag,
        writer_ref = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
        dir = Dir
    },

    %% Simulate two offset readers starting while the manifest is being
    %% downloaded.
    Pid1 = spawn(fun() -> ok end),
    From1 = {Pid1, erlang:make_ref()},
    Event2 = #manifest_requested{requester = From1, dir = Dir},
    Pid2 = spawn(fun() -> ok end),
    From2 = {Pid2, erlang:make_ref()},
    Event3 = #manifest_requested{requester = From2, dir = Dir},

    {Mac1, Effects1} = handle_events(?META(), [Event1, Event2, Event3], ?MAC:new()),
    ?assertEqual(Effects1, [
        #register_offset_listener{writer_pid = WriterPid, offset = -1},
        #download_manifest{dir = Dir}
    ]),

    Event4 = #manifest_downloaded{dir = Dir, manifest = undefined},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event4, Mac1),
    ?assertEqual(Effects2, [
        #reply{to = WriterFrom, response = undefined},
        #reply{to = From1, response = undefined},
        #reply{to = From2, response = undefined}
    ]),
    ok.

out_of_order_fragment_uploads(Config) ->
    {Mac0, Ref} = setup_writer(Config),
    Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{writer_ref = Ref, fragment = F} || F <- Fragments],
    {Mac1, Effects1} = handle_events(?META(), FragmentsAvailable, Mac0),
    ?assertEqual(Effects1, []),
    Event1 = #commit_offset_increased{writer_ref = Ref, offset = 60},
    {Mac2, Effects2} = ?MAC:apply(?META(), Event1, Mac1),
    ?assertMatch(
        [
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 0}},
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 20}},
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 40}},
            #register_offset_listener{}
        ],
        Effects2
    ),
    [Up1, Up2, Up3] = [
        #fragment_uploaded{writer_ref = Ref, info = fragment_to_info(F)}
     || F <- Fragments
    ],
    {Mac3, Effects3} = handle_events(?META(), [Up2, Up3], Mac2),
    %% The manifest won't be updated until a run of fragments have been uploaded:
    %% no holes are allowed in the manifest.
    ?assertEqual(Effects3, []),
    %% Once the first fragment has finished uploading then the manifest is
    %% updated for all fragments.
    {_Mac4, Effects4} = ?MAC:apply(?META(), Up1, Mac3),
    ?assertMatch([#upload_manifest{manifest = #manifest{first_offset = 0}}], Effects4),
    ok.

%%----------------------------------------------------------------------------

setup_writer(Config) ->
    Dir = get_config(Config, priv_dir),
    Pid = self(),
    Tag = erlang:make_ref(),
    WriterRef = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
    Event1 = #writer_spawned{
        pid = Pid,
        reply_tag = Tag,
        writer_ref = WriterRef,
        dir = Dir
    },
    Event2 = #manifest_downloaded{dir = Dir, manifest = undefined},
    {Mac, _} = handle_events(?META(), [Event1, Event2], ?MAC:new()),
    {Mac, WriterRef}.

handle_events(Meta, Events, Mac) ->
    handle_events(Meta, Events, Mac, []).

handle_events(_Meta, [], Mac, Acc) ->
    Effects = lists:flatten(lists:reverse(Acc)),
    {Mac, Effects};
handle_events(Meta, [Event | Rest], Mac0, Acc) ->
    {Mac, Effects} = ?MAC:apply(Meta, Event, Mac0),
    handle_events(Meta, Rest, Mac, [Effects | Acc]).

fragment(Offset, LastOffset) ->
    #fragment{
        segment_offset = Offset,
        first_offset = Offset,
        first_timestamp = erlang:system_time(millisecond),
        last_offset = LastOffset,
        next_offset = LastOffset + 1
    }.

fragment_to_info(#fragment{
    first_offset = O,
    first_timestamp = T,
    next_offset = N,
    seq_no = Seq,
    size = Size
}) ->
    #fragment_info{
        offset = O,
        timestamp = T,
        next_offset = N,
        seq_no = Seq,
        size = Size
    }.
