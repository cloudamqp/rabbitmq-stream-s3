%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3).

-doc """
A unique, randomly generated ID.

This binary contains random bytes. Use `format_uid/1` to format into human
readable text.
""".
-type uid() :: <<_:64>>.

-export_type([uid/0]).

-export([uid/0, null_uid/0, format_uid/1]).

-doc "Creates a new random UID.".
-spec uid() -> uid().
uid() ->
    crypto:strong_rand_bytes(8).

-doc """
Creates a zeroed UID binary.

This is meant for covering scenarios where the UID is not used.
""".
-spec null_uid() -> uid().
null_uid() ->
    <<0:64>>.

-doc "Formats a UID as human-readable text".
-spec format_uid(uid()) -> <<_:128>>.
format_uid(<<_:64>> = Uid) ->
    binary:encode_hex(Uid, lowercase).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

format_uid_test() ->
    ?assertEqual(<<"0000000000000000">>, format_uid(null_uid())),
    ok.

-endif.
