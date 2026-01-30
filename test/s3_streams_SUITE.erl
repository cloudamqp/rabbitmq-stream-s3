%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(s3_streams_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include("rabbitmq_stream_s3.hrl").

all() ->
    [
        publish_consume
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    % Configure Osiris to use S3 components and set the S3 API backend
    Config2 = rabbit_ct_helpers:merge_app_env(
        Config1,
        [
            {osiris, [
                {log_reader, rabbitmq_stream_s3_log_reader},
                {log_manifest, rabbitmq_stream_s3_log_manifest}
            ]},
            {rabbitmq_stream_s3, [
                {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs},
                {manifest_debounce_modifications, 1}
            ]},
            {rabbit, [
                {max_message_size, 134217728}
            ]}
        ]
    ),
    Config3 = rabbit_ct_helpers:run_setup_steps(
        Config2,
        rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()
    ),
    Config3.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
            rabbit_ct_broker_helpers:teardown_steps()
    ).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------
publish_consume(Config) ->
    % Generate payloads
    Payload1K = <<0:(1024 * 8)>>,
    Payload64M = <<1:(64 * 1024 * 1024 * 8)>>,

    % Open channel
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 1}),

    QName = <<"stream_1">>,

    % Should not be data for the stream we haven't declared yet
    ?assertMatch(
        {error, not_found},
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        )
    ),

    % Create a stream.
    ?assertEqual(ok, stream_declare(Ch, QName)),
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload1K}
        )
    ),

    % Publishing a >= 64MB message triggers upload, since there is already a
    % chunk (the 1KB one) in the stream.
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload64M}
        )
    ),

    % One fragment should exist
    ?awaitMatch(
        {ok, Manifest, [_Fragment]} when Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        500
    ),

    % If we delete the stream, the data in the tiered storage should eventually
    % be cleared out.
    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),

    ?assertMatch(
        {error, not_found},
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        )
    ),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

%% -------------------------------------------------------------------
%% Private functions
%% -------------------------------------------------------------------
stream_declare(Ch, StreamName) ->
    Args = [{<<"x-queue-type">>, longstr, <<"stream">>}],

    case
        amqp_channel:call(
            Ch,
            #'queue.declare'{
                queue = StreamName,
                durable = true,
                arguments = Args
            }
        )
    of
        #'queue.declare_ok'{} -> ok;
        Error -> {error, Error}
    end.
