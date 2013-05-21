-module(werken_utils).
-export([size_or_length/1, generate_job_id/0, generate_worker_id/0, generate_client_id/0, args_to_list/1, list_to_null_list/1, merge_records/3]).

size_or_length(Term) when is_binary(Term) ->
  size(Term);

size_or_length(Term) when is_list(Term) ->
  length(Term).

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

%% internal functions
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
