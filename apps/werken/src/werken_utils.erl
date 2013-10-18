-module(werken_utils).
-compile([{parse_transform, lager_transform}]).
-export([now_to_epoch/0, date_to_milliseconds/5, epoch_to_milliseconds/1, size_or_length/1, generate_job_id/0, generate_worker_id/0, generate_client_id/0, args_to_list/1, list_to_null_list/1, merge_records/3, generate_unique_id/2]).

% refactor all this shit
now_to_epoch() ->
  {Mega, Sec, _} = erlang:now(),
  (Mega * 1000000) + Sec.

epoch_to_milliseconds(Epoch) when is_list(Epoch) ->
  E = erlang:list_to_integer(Epoch),
  epoch_to_milliseconds(E);

epoch_to_milliseconds(Epoch) ->
  UnixEpoch={{1970, 1, 1}, {0, 0, 0}},
  UnixSeconds = calendar:datetime_to_gregorian_seconds(UnixEpoch),
  Now = calendar:now_to_universal_time(erlang:now()),
  NowSeconds = calendar:datetime_to_gregorian_seconds(Now),
  NowEpoch = NowSeconds - UnixSeconds,
  case Epoch > NowEpoch of
    true -> (Epoch - NowEpoch) * 1000;
    _ -> 0
  end.

date_to_milliseconds(Minute, Hour, DayOfMonth, Month, []) ->
  {{Y, _, _}, {_, _, _}} = calendar:now_to_universal_time(erlang:now()),
  DateTime = {{Y, Month, DayOfMonth}, {Hour, Minute, 0}},
  Seconds = calendar:datetime_to_gregorian_seconds(DateTime),
  Now = calendar:now_to_universal_time(erlang:now()),
  NowSeconds = calendar:datetime_to_gregorian_seconds(Now),
  case Seconds > NowSeconds of
    true -> (Seconds - NowSeconds) * 1000;
    _ -> 0
  end;

date_to_milliseconds(Minute, Hour, [], Month, DayOfWeek) ->
  {{Y, M, D}, {_, _, _}} = calendar:now_to_universal_time(erlang:now()),
  CurrentDayOfWeek = calendar:day_of_the_week(Y, M, D), % this is 1 based, gearman is 0 based
  NewCurrentDayOfWeek = CurrentDayOfWeek - 1,
  Offset = case DayOfWeek >= NewCurrentDayOfWeek of
    true ->
      DayOfWeek - NewCurrentDayOfWeek;
    _ ->
      (7 - NewCurrentDayOfWeek) + DayOfWeek
  end,
  NewDay = Offset + D,
  date_to_milliseconds(Minute, Hour, NewDay, Month, []);

date_to_milliseconds(Minute, Hour, DayOfMonth, Month, _DayOfWeek) ->
  date_to_milliseconds(Minute, Hour, DayOfMonth, Month, []).

size_or_length(Term) when is_binary(Term) ->
  size(Term);

size_or_length(Term) when is_list(Term) ->
  length(Term).

generate_unique_id(FunctionName, Data) ->
  B = list_to_binary(FunctionName),
  C = <<B/binary, Data/binary>>,
  hmac:hexlify(erlsha2:sha256(C)).

generate_worker_id() ->
  "W:" ++ integer_to_list(random_int()).

generate_client_id() ->
  "C:" ++ integer_to_list(random_int()).

generate_job_id() ->
  "J:" ++ string:to_lower(hmac:hexlify(erlsha2:sha224(integer_to_list(random_int())))).

args_to_list(Args) ->
  Parts = binary:split(Args, [<<0>>], [global]),
  Result = lists:map(fun(X) -> binary_to_list(X) end, Parts),
  case Result of
    [[]] ->
      [];
    Other ->
      Other
  end.

list_to_null_list(List) ->
  L = lists:foldr(
    fun(X, Result) ->
        case Result of
          [] ->
            [X];
          _ ->
            [X|[0|Result]]
        end
    end, [], List),
  list_to_binary(L).

%%% This is a slightly modified version of some code that Adam Lindberg posted
%%% to StackOverflow, here:
%%% http://stackoverflow.com/questions/62245/merging-records-for-mnesia
%%% Thanks Adam.
%%% @end
merge_records(RecordName, RecordA, RecordB) ->
  list_to_tuple(
    lists:append([RecordName],
      merge(tl(tuple_to_list(RecordA)),
            tl(tuple_to_list(RecordB)),
            []))).

%% private functions
random_int() ->
  A = erlang:phash2(erlang:now()),
  B = erlang:phash2(crypto:strong_rand_bytes(64)),
  A * B.

%%% @spec merge(A, B, []) -> [term()]
%%%     A = [term()]
%%%     B = [term()]
%%%
%%% @doc Merges the lists `A' and `B' into to a new list
%%%
%%% Each element in `A' and `B' are compared.
%%% If they match, the matching element is added to the result.
%%% If one is undefined and the other is not, the one that is not undefined
%%% is added to the result. If each has a value and they differ, `A' takes
%%% precedence.
merge([C|ATail], [C|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([undefined|ATail], [C|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([C|ATail], [undefined|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([C|ATail], [_|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([], [], Result) ->
  lists:reverse(Result).
